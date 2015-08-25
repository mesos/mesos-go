package messenger

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/golang/glog"
)

const (
	DefaultReadTimeout  = 5 * time.Second
	DefaultWriteTimeout = 5 * time.Second
)

type decoderID int32

func (did decoderID) String() string {
	return "[" + strconv.Itoa(int(did)) + "]"
}

func (did *decoderID) Next() decoderID {
	return decoderID(atomic.AddInt32((*int32)(did), 1))
}

var (
	errHijackFailed = errors.New("failed to hijack http connection")
	did             decoderID // decoder ID counter
)

type Decoder interface {
	Requests() <-chan *Request
	Err() <-chan error
	Cancel(bool)
}

type Request struct {
	*http.Request
	response chan<- Response // callers that are finished with a Request should ensure that response is *always* closed, regardless of whether a Response has been written.
}

type Response struct {
	code   int
	reason string
}

type httpDecoder struct {
	req          *http.Request       // original request
	res          http.ResponseWriter // original response; TODO(jdef) kill this
	kalive       bool                // keepalive
	chunked      bool                // chunked
	msg          chan *Request
	resCh        chan chan Response // queue of responses that we're waiting for
	rch          chan Response      // current response that we're attempting to process
	con          net.Conn
	rw           *bufio.ReadWriter
	errCh        chan error
	state        httpState
	buf          *bytes.Buffer
	lrc          *limitReadCloser
	shouldQuit   chan struct{} // signal chan, closes upon calls to Cancel(...)
	forceQuit    chan struct{} // signal chan, indicates that quit is NOT graceful; closes upon Cancel(false)
	quitOnce     sync.Mutex
	readTimeout  time.Duration
	writeTimeout time.Duration
	idtag        string
	out          *bytes.Buffer // buffer used to cache a pending response
}

// DecodeHTTP hijacks an HTTP server connection and generates mesos libprocess HTTP
// requests via the returned chan. Upon generation of an error in the error chan the
// decoder's internal goroutine will terminate. This func returns immediately.
// The caller should immediately *stop* using the ResponseWriter and Request that were
// passed as parameters; the decoder assumes full control of the HTTP transport.
func DecodeHTTP(w http.ResponseWriter, r *http.Request) Decoder {
	id := (&did).Next()
	d := &httpDecoder{
		state:        bootstrapState,
		msg:          make(chan *Request),
		errCh:        make(chan error, 1),
		req:          r,
		res:          w,
		shouldQuit:   make(chan struct{}),
		forceQuit:    make(chan struct{}),
		readTimeout:  DefaultReadTimeout,
		writeTimeout: DefaultWriteTimeout,
		idtag:        id.Next().String(),
		resCh:        make(chan chan Response, 10),
	}
	go d.run()
	return d
}

func (d *httpDecoder) Requests() <-chan *Request {
	return d.msg
}

func (d *httpDecoder) Err() <-chan error {
	return d.errCh
}

// Cancel the decoding process; if graceful then process pending responses before terminating
func (d *httpDecoder) Cancel(graceful bool) {
	log.V(2).Infof("%scancel:%b", d.idtag, graceful)
	d.quitOnce.Lock()
	defer d.quitOnce.Unlock()
	select {
	case <-d.shouldQuit:
		// already quitting, but perhaps gracefully?
	default:
		close(d.shouldQuit)
	}
	// check this here allows caller to "upgrade" from a graceful cancel to a forced one
	if !graceful {
		select {
		case <-d.forceQuit:
			// already forcefully quitting
		default:
			close(d.forceQuit) // push it!
		}
	}
}

func (d *httpDecoder) run() {
	defer log.V(2).Infoln(d.idtag + "run: terminating")
	for d.state != nil {
		next := d.state(d)
		d.state = next
		d.rch = d.checkResponses(d.rch)
	}
}

// tryFlushResponse flushes the response buffer (if not empty); returns true if
func (d *httpDecoder) tryFlushResponse(rch chan Response) (rchOut chan Response, success bool) {
	rchOut = rch
	if d.out == nil || d.out.Len() == 0 {
		// we always succeed when there's no work to do
		success = true
		return
	}

	log.V(2).Infof(d.idtag+"try-flush-responses: %d bytes to flush", d.out.Len())
	// set a write deadline here so that we don't block for very long.
	err := d.setWriteTimeout()
	if err != nil {
		// this is a problem because if we can't set the timeout then we can't guarantee
		// how long a write op might block for. Log the error and skip this response.
		log.Errorln("failed to set write deadline, aborting response:", err.Error())
	} else {
		_, err = d.out.WriteTo(d.rw.Writer)
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() && d.out.Len() > 0 {
				// we couldn't fully write before timing out, return rch and hope that
				// we have better luck next time.
				return
			}
			// we don't really know how to deal with other kinds of errors, so
			// log it and skip the rest of the response.
			log.Errorln("failed to write response buffer:", err.Error())
		}
		err = d.rw.Flush()
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() && d.out.Len() > 0 {
				return
			}
			log.Errorln("failed to flush response buffer:", err.Error())
		}
	}

	// When we're finished dumping (for whatever reason), set rchOut = nil to indicate the the in-flight response has been flushed
	rchOut = nil
	success = true
	d.out.Reset()
	return
}

// check the given response chan (if any); if rch is nil then attempt to dequeue the next response chan
// from resCh. return the currently active response chan (if any) that we should check upon the next invocation.
func (d *httpDecoder) checkResponses(rch chan Response) chan Response {
	log.V(2).Info(d.idtag + "check-responses")
	defer log.V(2).Info(d.idtag + "check-responses: finished")
responseLoop:
	for {
		var ok bool
		rch, ok = d.tryFlushResponse(rch)
		if !ok {
			return rch
		}

		if rch == nil {
			// pop one off resCh
			select {
			case <-d.forceQuit:
				return nil

			case rch = <-d.resCh:
				// check for tie
				select {
				case <-d.forceQuit:
					return nil
				default:
				}

			default:
				// no one told us to force-stop, and there's no outstanding responses
				// that we're waiting for; move on!
				return nil
			}
		}

		log.V(2).Infoln(d.idtag + "check-responses: handling response")

		// rch is not nil and will deliver us a response code to send back across the wire.
		// check for the response code on rch, otherwise defer.
		select {
		case <-d.forceQuit:
			return nil

		case resp, ok := <-rch:
			// check for tie
			select {
			case <-d.forceQuit:
				return nil
			default:
			}

			if !ok {
				// no response code to send, move on
				rch = nil
				continue responseLoop
			}

			// response required, so build it
			if d.out == nil {
				d.out = &bytes.Buffer{}
			}
			d.buildResponseEntity(&resp)

		default:
			// response code isn't ready, and no one has asked us to quit; check back later
			return rch
		}
	}
}

// TODO(jdef) make this a func on Response, to write its contents to a *bytes.Buffer
func (d *httpDecoder) buildResponseEntity(resp *Response) {
	log.V(2).Infoln(d.idtag + "build-response-entity")

	// generate new response buffer content and continue; buffer should have
	// at least a response status-line w/ Content-Length: 0
	d.out.WriteString("HTTP/1.1 ")
	d.out.WriteString(strconv.Itoa(resp.code))
	d.out.WriteString(" ")
	d.out.WriteString(resp.reason)
	d.out.WriteString(crlf + "Content-Length: 0" + crlf)

	select {
	case <-d.shouldQuit:
		if len(d.resCh) == 0 {
			// this is the last request in the pipeline and we've been told to quit, so
			// indicate that the server will close the connection.
			// see flushResponsesState()
			d.out.WriteString("Connection: Close" + crlf)
		}
	default:
	}
	d.out.WriteString(crlf) // this ends the HTTP response entity
}

// updateForRequest updates the chunked and kalive fields of the decoder to align
// with the header values of the request
func (d *httpDecoder) updateForRequest() {
	// check "Transfer-Encoding" for "chunked"
	d.chunked = false
	for _, v := range d.req.Header["Transfer-Encoding"] {
		if v == "chunked" {
			d.chunked = true
			break
		}
	}
	if !d.chunked && d.req.ContentLength < 0 {
		// strongly suspect that Go's internal net/http lib is stripping
		// the Transfer-Encoding header from the initial request, so this
		// workaround makes a very mesos-specific assumption: an unknown
		// Content-Length indicates a chunked stream.
		d.chunked = true
	}

	// check "Connection" for "Keep-Alive"
	d.kalive = d.req.Header.Get("Connection") == "Keep-Alive"

	log.V(2).Infof(d.idtag+"update-for-request: chunked %v keep-alive %v", d.chunked, d.kalive)
}

func (d *httpDecoder) readBodyContent() httpState {
	log.V(2).Info(d.idtag + "read-body-content")
	if d.chunked {
		d.buf = &bytes.Buffer{}
		return readChunkHeaderState
	} else {
		d.lrc = limit(d.rw.Reader, d.req.ContentLength)
		d.buf = &bytes.Buffer{}
		return readBodyState
	}
}

const http202response = "HTTP/1.1 202 OK\r\nContent-Length: 0\r\n\r\n"

func (d *httpDecoder) generateRequest() httpState {
	log.V(2).Infof(d.idtag + "generate-request")
	// send a Request to msg
	b := d.buf.Bytes()
	rch := make(chan Response, 1)
	r := &Request{
		Request: &http.Request{
			Method:        d.req.Method,
			URL:           d.req.URL,
			Proto:         d.req.Proto,
			ProtoMajor:    d.req.ProtoMajor,
			ProtoMinor:    d.req.ProtoMinor,
			Header:        d.req.Header,
			Close:         !d.kalive,
			Host:          d.req.Host,
			RequestURI:    d.req.RequestURI,
			Body:          &body{bytes.NewBuffer(b)},
			ContentLength: int64(len(b)),
		},
		response: rch,
	}

	select {
	case d.resCh <- rch:
	case <-d.forceQuit:
		return terminateState
	}

	select {
	case d.msg <- r:
	case <-d.forceQuit:
		return terminateState
	}

	if d.kalive {
		d.req = &http.Request{
			ContentLength: -1,
			Header:        make(http.Header),
		}
		return awaitRequestState
	} else {
		return gracefulTerminateState
	}
}

type httpState func(d *httpDecoder) httpState

// terminateState forcefully shuts down the state machine
func terminateState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "terminate-state")
	// closing these chans tells Decoder users that it's wrapping up
	close(d.msg)
	close(d.errCh)

	// attempt to forcefully close the connection and signal response handlers that
	// no further responses should be written
	d.Cancel(false)

	if d.con != nil {
		d.con.Close()
	}

	// there is no spoon
	return nil
}

func gracefulTerminateState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "gracefully-terminate-state")
	// closing these chans tells Decoder users that it's wrapping up
	close(d.msg)
	close(d.errCh)

	// gracefully terminate the connection; signal that we should flush pending
	// responses before closing the connection.
	d.Cancel(true)
	return flushResponsesState
}

func flushResponsesState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "flush-responses-state")
	select {
	case <-d.forceQuit:
		return terminateState
	default:
	}

	if len(d.resCh) > 0 {
		// at least 1 outstanding response to be serviced, not counting the one
		// maintained by the run() machinery.
		return flushResponsesState
	}

	// handle rch that may be in-flight in run(); if we return nil here
	// then an in-flight rch will never be written out.
	if d.rch != nil {
		return flushResponsesState
	}

	// finally done!
	log.V(2).Infoln(d.idtag + "flush-responses-state:done")
	return nil
}

type limitReadCloser struct {
	*io.LimitedReader
	r *bufio.Reader
}

func limit(r *bufio.Reader, limit int64) *limitReadCloser {
	return &limitReadCloser{
		LimitedReader: &io.LimitedReader{
			R: r,
			N: limit,
		},
		r: r,
	}
}

func (l *limitReadCloser) Close() error {
	// discard remaining
	_, err := l.r.Discard(int(l.LimitedReader.N)) // TODO(jdef) possible overflow due to type cast
	return err
}

// bootstrapState expects to be called when the standard net/http lib has already
// read the initial request query line + headers from a connection. the request
// is ready to be hijacked at this point.
func bootstrapState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "bootstrap-state")
	d.updateForRequest()

	// hijack
	hj, ok := d.res.(http.Hijacker)
	if !ok {
		http.Error(d.res, "server does not support hijack", http.StatusInternalServerError)
		d.errCh <- errHijackFailed
		return terminateState
	}
	c, rw, err := hj.Hijack()
	if err != nil {
		http.Error(d.res, "failed to hijack the connection", http.StatusInternalServerError)
		d.errCh <- errHijackFailed
		return terminateState
	}
	d.rw = rw
	d.con = c
	return d.readBodyContent()
}

type body struct {
	*bytes.Buffer
}

func (b *body) Close() error { return nil }

// checkTimeoutOrFail tests whether the given error is related to a timeout condition.
// returns true if the caller should advance to the returned state.
func (d *httpDecoder) checkTimeoutOrFail(err error, stateContinue httpState) (httpState, bool) {
	if err != nil {
		if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
			select {
			case <-d.forceQuit:
				return terminateState, true
			case <-d.shouldQuit:
				return gracefulTerminateState, true
			default:
				return stateContinue, true
			}
		}
		d.errCh <- err
		return terminateState, true
	}
	return nil, false
}

func (d *httpDecoder) setReadTimeoutOrFail() bool {
	if d.readTimeout > 0 {
		err := d.con.SetReadDeadline(time.Now().Add(d.readTimeout))
		if err != nil {
			d.errCh <- err
			return false
		}
	}
	return true
}

func (d *httpDecoder) setWriteTimeout() error {
	if d.writeTimeout > 0 {
		return d.con.SetWriteDeadline(time.Now().Add(d.writeTimeout))
	}
	return nil
}

func readChunkHeaderState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "read-chunk-header-state")
	tr := textproto.NewReader(d.rw.Reader)
	if !d.setReadTimeoutOrFail() {
		return terminateState
	}
	hexlen, err := tr.ReadLine()
	if next, ok := d.checkTimeoutOrFail(err, readChunkHeaderState); ok {
		return next
	}

	clen, err := strconv.ParseInt(hexlen, 16, 64)
	if err != nil {
		d.errCh <- err
		return terminateState
	}

	if clen == 0 {
		return readEndOfChunkStreamState
	}

	d.lrc = limit(d.rw.Reader, clen)
	return readChunkState
}

func readChunkState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag+"read-chunk-state, bytes remaining:", d.lrc.N)
	if !d.setReadTimeoutOrFail() {
		return terminateState
	}
	_, err := d.buf.ReadFrom(d.lrc)
	if next, ok := d.checkTimeoutOrFail(err, readChunkState); ok {
		return next
	}
	return readEndOfChunkState
}

const crlf = "\r\n"

func readEndOfChunkState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "read-end-of-chunk-state")
	if !d.setReadTimeoutOrFail() {
		return terminateState
	}
	b, err := d.rw.Reader.Peek(2)
	if len(b) == 2 {
		if string(b) == crlf {
			d.rw.Discard(2)
			return readChunkHeaderState
		}
		d.errCh <- errors.New(d.idtag + "unexpected data at end-of-chunk marker")
		return terminateState
	}
	// less than two bytes avail
	if next, ok := d.checkTimeoutOrFail(err, readEndOfChunkState); ok {
		return next
	}
	panic("couldn't peek 2 bytes, but didn't get an error?!")
}

func readEndOfChunkStreamState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "read-end-of-chunk-stream-state")
	if !d.setReadTimeoutOrFail() {
		return terminateState
	}
	b, err := d.rw.Reader.Peek(2)
	if len(b) == 2 {
		if string(b) == crlf {
			d.rw.Discard(2)
			return d.generateRequest()
		}
		d.errCh <- errors.New(d.idtag + "unexpected data at end-of-chunk marker")
		return terminateState
	}
	// less than 2 bytes avail
	if next, ok := d.checkTimeoutOrFail(err, readEndOfChunkStreamState); ok {
		return next
	}
	panic("couldn't peek 2 bytes, but didn't get an error?!")
}

func readBodyState(d *httpDecoder) httpState {
	log.V(2).Infof(d.idtag+"read-body-state: %d bytes remaining", d.lrc.N)
	// read remaining bytes into the buffer
	var err error
	if d.lrc.N > 0 {
		if !d.setReadTimeoutOrFail() {
			return terminateState
		}
		_, err = d.buf.ReadFrom(d.lrc)
	}
	if d.lrc.N <= 0 {
		return d.generateRequest()
	}
	if next, ok := d.checkTimeoutOrFail(err, readBodyState); ok {
		return next
	}
	return readBodyState
}

func awaitRequestState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "await-request-state")
	tr := textproto.NewReader(d.rw.Reader)
	if !d.setReadTimeoutOrFail() {
		return terminateState
	}
	requestLine, err := tr.ReadLine()
	if next, ok := d.checkTimeoutOrFail(err, awaitRequestState); ok {
		return next
	}
	ss := strings.SplitN(requestLine, " ", 3)
	if len(ss) < 3 {
		if err == io.EOF {
			return gracefulTerminateState
		}
		d.errCh <- errors.New(d.idtag + "illegal request line")
		return terminateState
	}
	r := d.req
	r.Method = ss[0]
	r.RequestURI = ss[1]
	r.URL, err = url.ParseRequestURI(ss[1])
	if err != nil {
		d.errCh <- err
		return terminateState
	}
	major, minor, ok := http.ParseHTTPVersion(ss[2])
	if !ok {
		d.errCh <- errors.New(d.idtag + "malformed HTTP version")
		return terminateState
	}
	r.ProtoMajor = major
	r.ProtoMinor = minor
	r.Proto = ss[2]
	return readHeaderState
}

func readHeaderState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "read-header-state")
	if !d.setReadTimeoutOrFail() {
		return terminateState
	}
	r := d.req
	tr := textproto.NewReader(d.rw.Reader)
	h, err := tr.ReadMIMEHeader()
	for k, v := range h {
		if rh, exists := r.Header[k]; exists {
			r.Header[k] = append(rh, v...)
		} else {
			r.Header[k] = v
		}
		log.V(2).Infoln(d.idtag+"request header", k, v)
	}
	if next, ok := d.checkTimeoutOrFail(err, readHeaderState); ok {
		return next
	}

	// special headers: Host, Content-Length, Transfer-Encoding
	r.Host = r.Header.Get("Host")
	r.TransferEncoding = r.Header["Transfer-Encoding"]
	if cl := r.Header.Get("Content-Length"); cl != "" {
		l, err := strconv.ParseInt(cl, 10, 64)
		if err != nil {
			d.errCh <- err
			return terminateState
		}
		if l > -1 {
			r.ContentLength = l
			log.V(2).Infoln(d.idtag+"set content length", r.ContentLength)
		}
	}
	d.updateForRequest()
	return d.readBodyContent()
}
