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
	DefaultReadTimeout = 5 * time.Second
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
	Requests() <-chan *http.Request
	Err() <-chan error
	Cancel()
}

type httpDecoder struct {
	req         *http.Request       // original request
	res         http.ResponseWriter // original response
	kalive      bool                // keepalive
	chunked     bool                // chunked
	msg         chan *http.Request
	con         net.Conn
	rw          *bufio.ReadWriter
	errCh       chan error
	state       httpState
	buf         *bytes.Buffer
	lrc         *limitReadCloser
	shouldQuit  chan struct{} // signal chan
	quitOnce    sync.Once
	readTimeout time.Duration
	idtag       string
}

// DecodeHTTP hijacks an HTTP server connection and generates mesos libprocess HTTP
// requests via the returned chan. Upon generation of an error in the error chan the
// decoder's internal goroutine will terminate. This func returns immediately.
// The caller should immediately *stop* using the ResponseWriter and Request that were
// passed as parameters; the decoder assumes full control of the HTTP transport.
func DecodeHTTP(w http.ResponseWriter, r *http.Request) Decoder {
	id := (&did).Next()
	d := &httpDecoder{
		state:       bootstrapState,
		msg:         make(chan *http.Request),
		errCh:       make(chan error, 1),
		req:         r,
		res:         w,
		shouldQuit:  make(chan struct{}),
		readTimeout: DefaultReadTimeout,
		idtag:       id.Next().String(),
	}
	go d.run()
	return d
}

func (d *httpDecoder) Requests() <-chan *http.Request {
	return d.msg
}

func (d *httpDecoder) Err() <-chan error {
	return d.errCh
}

func (d *httpDecoder) Cancel() {
	d.quitOnce.Do(func() { close(d.shouldQuit) })
}

func (d *httpDecoder) run() {
	for d.state != nil {
		next := d.state(d)
		d.state = next
	}
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
	r := &http.Request{
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
	}

	select {
	case d.msg <- r:
	case <-d.shouldQuit:
		// best effort, we may have just tied
		select {
		case d.msg <- r:
		default:
		}
		return terminateState
	}

	// send a 202 response to mesos so that it knows to continue
	// TODO(jdef) set a write deadline here
	_, err := d.rw.WriteString(http202response)
	if err != nil {
		if log.V(2) {
			log.Errorln(d.idtag+"failed to write response code?!", err.Error())
		}
	} else {
		err = d.rw.Flush()
		if err != nil {
			if log.V(2) {
				log.Errorln(d.idtag+"failed to flush writer?!", err.Error())
			}
		}
	}

	// normally we'd terminate on the above error, but I'm testing something with libprocess: perhaps
	// it leaves the connection half-open so that we can continue to receive events?

	if d.kalive {
		d.req = &http.Request{
			ContentLength: -1,
			Header:        make(http.Header),
		}
		return awaitRequestState
	} else {
		// TODO(jdef) we probably want to send a HTTP 202 or something?!
		return terminateState
	}
}

type httpState func(d *httpDecoder) httpState

// terminateState shuts down the state machine
func terminateState(d *httpDecoder) httpState {
	close(d.msg)
	close(d.errCh)
	if d.con != nil {
		d.con.Close()
	}
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
	_, err := l.r.Discard(int(l.LimitedReader.N)) // TODO(jdef) possible overflow
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

// checkError tests whether the given error is related to a timeout condition.
// returns true if the caller should advance to the returned state.
func (d *httpDecoder) checkError(err error, stateContinue httpState) (httpState, bool) {
	if err != nil {
		if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
			select {
			case <-d.shouldQuit:
				return terminateState, true
			default:
				return stateContinue, true
			}
		}
		d.errCh <- err
		return terminateState, true
	}
	return nil, false
}

func (d *httpDecoder) setReadTimeout() bool {
	if d.readTimeout > 0 {
		err := d.con.SetReadDeadline(time.Now().Add(d.readTimeout))
		if err != nil {
			d.errCh <- err
			return false
		}
	}
	return true
}

func readChunkHeaderState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "read-chunk-header-state")
	tr := textproto.NewReader(d.rw.Reader)
	if !d.setReadTimeout() {
		return terminateState
	}
	hexlen, err := tr.ReadLine()
	if next, ok := d.checkError(err, readChunkHeaderState); ok {
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
	if !d.setReadTimeout() {
		return terminateState
	}
	_, err := d.buf.ReadFrom(d.lrc)
	if next, ok := d.checkError(err, readChunkState); ok {
		return next
	}
	return readEndOfChunkState
}

const crlf = "\r\n"

func readEndOfChunkState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "read-end-of-chunk-state")
	if !d.setReadTimeout() {
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
	if next, ok := d.checkError(err, readEndOfChunkState); ok {
		return next
	}
	panic("couldn't peek 2 bytes, but didn't get an error?!")
}

func readEndOfChunkStreamState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "read-end-of-chunk-stream-state")
	if !d.setReadTimeout() {
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
	if next, ok := d.checkError(err, readEndOfChunkStreamState); ok {
		return next
	}
	panic("couldn't peek 2 bytes, but didn't get an error?!")
}

func readBodyState(d *httpDecoder) httpState {
	log.V(2).Infof(d.idtag+"read-body-state: %d bytes remaining", d.lrc.N)
	// read remaining bytes into the buffer
	var err error
	if d.lrc.N > 0 {
		if !d.setReadTimeout() {
			return terminateState
		}
		_, err = d.buf.ReadFrom(d.lrc)
	}
	if d.lrc.N <= 0 {
		return d.generateRequest()
	}
	if next, ok := d.checkError(err, readBodyState); ok {
		return next
	}
	return readBodyState
}

func awaitRequestState(d *httpDecoder) httpState {
	log.V(2).Infoln(d.idtag + "await-request-state")
	tr := textproto.NewReader(d.rw.Reader)
	if !d.setReadTimeout() {
		return terminateState
	}
	requestLine, err := tr.ReadLine()
	if next, ok := d.checkError(err, awaitRequestState); ok {
		return next
	}
	ss := strings.SplitN(requestLine, " ", 3)
	if len(ss) < 3 {
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
	if !d.setReadTimeout() {
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
	if next, ok := d.checkError(err, readHeaderState); ok {
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
