package detector

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/upid"
	"golang.org/x/net/context"
)

const (
	mesosHttpClientTimeout  = 10 * time.Second //TODO(jdef) configurable via fiag?
	mesosLeaderSyncInterval = 30 * time.Second //TODO(jdef) configurable via fiag?
	defaultMesosMasterPort  = 5050
)

type Standalone struct {
	ch         chan *mesos.MasterInfo
	client     *http.Client
	tr         *http.Transport
	pollOnce   sync.Once
	initial    *mesos.MasterInfo
	done       chan struct{}
	cancelOnce sync.Once
}

// Create a new stand alone master detector.
func NewStandalone(mi *mesos.MasterInfo) *Standalone {
	log.V(2).Infof("creating new standalone detector for %+v", mi)
	ch := make(chan *mesos.MasterInfo)
	tr := &http.Transport{}
	return &Standalone{
		ch: ch,
		client: &http.Client{
			Transport: tr,
			Timeout:   mesosHttpClientTimeout,
		},
		tr:      tr,
		initial: mi,
		done:    make(chan struct{}),
	}
}

func (s *Standalone) String() string {
	return fmt.Sprintf("{initial: %+v}", s.initial)
}

// Detecting the new master.
func (s *Standalone) Detect(o MasterChanged) error {
	log.V(2).Info("Detect()")
	s.pollOnce.Do(func() {
		log.V(1).Info("spinning up asyc master detector poller")
		go s.poller()
	})
	if o != nil {
		log.V(1).Info("spawning asyc master detector listener")
		go func() {
			log.V(2).Infof("waiting for polled to send updates")
		pollWaiter:
			for {
				select {
				case mi, ok := <-s.ch:
					if !ok {
						break pollWaiter
					}
					log.V(1).Infof("detected master change: %+v", mi)
					o.Notify(mi)
				case <-s.done:
					return
				}
			}
			o.Notify(nil)
		}()
	} else {
		log.Warningf("detect called with a nil master change listener")
	}
	return nil
}

func (s *Standalone) Done() <-chan struct{} {
	return s.done
}

func (s *Standalone) Cancel() {
	s.cancelOnce.Do(func() { close(s.done) })
}

// poll for changes to master leadership via current leader's /state.json endpoint.
// we start with the `initial` leader, aborting if none was specified. thereafter,
// the `leader` property of the state.json is used to identify the next leader that
// should be polled.
//
// TODO(jdef) somehow determine all masters in cluster from the state.json?
//
func (s *Standalone) poller() {
	if s.initial == nil {
		log.Warningf("aborting master poller since initial master info is nil")
		return
	}
	//TODO(jdef) we could attempt to unpack IP addres if Host=""
	if s.initial.Hostname != nil && len(*s.initial.Hostname) == 0 {
		log.Warningf("aborted mater poller since initial master info has no host")
		return
	}
	addr := *s.initial.Hostname
	port := uint32(defaultMesosMasterPort)
	if s.initial.Port != nil && *s.initial.Port != 0 {
		port = *s.initial.Port
	}
	addr = net.JoinHostPort(addr, strconv.Itoa(int(port)))
	log.V(1).Infof("polling for master leadership at '%v'", addr)
	var lastpid *upid.UPID
	for {
		startedAt := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), mesosLeaderSyncInterval)
		if pid, err := s.fetchPid(ctx, addr); err == nil {
			if !pid.Equal(lastpid) {
				log.V(2).Infof("detected leadership change from '%v' to '%v'", lastpid, pid)
				lastpid = pid
				elapsed := time.Now().Sub(startedAt)
				mi := CreateMasterInfo(pid)
				select {
				case s.ch <- mi:
					// noop
				case <-time.After(mesosLeaderSyncInterval - elapsed):
					// no one heard the master change, oh well - poll again
					cancel()
					continue
				case <-s.done:
					cancel()
					return
				}
			} else {
				log.V(2).Infof("no change to master leadership: '%v'", lastpid)
			}
		} else {
			log.Warning(err)
		}
		remaining := mesosLeaderSyncInterval - time.Now().Sub(startedAt)
		log.V(3).Infof("master leader poller sleeping for %v", remaining)
		time.Sleep(remaining)
		cancel()
	}
}

func (s *Standalone) fetchPid(ctx context.Context, address string) (*upid.UPID, error) {
	//TODO(jdef) need better address parsing, for now assume host, or host:port format
	//TODO(jdef) need SSL support
	uri := fmt.Sprintf("http://%s/state.json", address)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}
	var pid *upid.UPID
	err = s.httpDo(ctx, req, func(res *http.Response, err error) error {
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			return fmt.Errorf("HTTP request failed with code %d: %v", res.StatusCode, res.Status)
		}
		blob, err1 := ioutil.ReadAll(res.Body)
		if err1 != nil {
			return err1
		}
		log.V(3).Infof("Got mesos state, content length %v", len(blob))
		type State struct {
			Leader string `json:"leader"` // ex: master(1)@10.22.211.18:5050
		}
		state := &State{}
		err = json.Unmarshal(blob, state)
		if err != nil {
			return err
		}
		pid, err = upid.Parse(state.Leader)
		return err
	})
	return pid, err
}

type responseHandler func(*http.Response, error) error

// hacked from https://blog.golang.org/context
func (s *Standalone) httpDo(ctx context.Context, req *http.Request, f responseHandler) error {
	// Run the HTTP request in a goroutine and pass the response to f.
	ch := make(chan error, 1)
	go func() { ch <- f(s.client.Do(req)) }()
	select {
	case <-ctx.Done():
		s.tr.CancelRequest(req)
		<-ch // Wait for f to return.
		return ctx.Err()
	case err := <-ch:
		return err
	}
}
