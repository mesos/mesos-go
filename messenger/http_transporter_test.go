package messenger

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/mesos/mesos-go/messenger/testmessage"
	"github.com/mesos/mesos-go/upid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestTransporterNew(t *testing.T) {
	id, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(t, err)
	trans := NewHTTPTransporter(id)
	assert.NotNil(t, trans)
	assert.NotNil(t, trans.upid)
	assert.NotNil(t, trans.messageQueue)
	assert.NotNil(t, trans.client)
}

func TestTransporterSend(t *testing.T) {
	idreg := regexp.MustCompile(`[A-Za-z0-9_\-]+@[A-Za-z0-9_\-]+:[0-9]+`)
	serverId := "testserver"

	// setup mesos client-side
	fromUpid, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(t, err)

	protoMsg := testmessage.GenerateSmallMessage()
	msgName := getMessageName(protoMsg)
	msg := &Message{
		Name:         msgName,
		ProtoMessage: protoMsg,
	}
	requestURI := fmt.Sprintf("/%s/%s", serverId, msgName)

	// setup server-side
	srv := makeMockServer(requestURI, func(rsp http.ResponseWriter, req *http.Request) {
		from := req.Header.Get("Libprocess-From")
		assert.NotEmpty(t, from)
		assert.True(t, idreg.MatchString(from))
	})
	defer srv.Close()
	toUpid, err := upid.Parse(fmt.Sprintf("%s@%s", serverId, srv.Listener.Addr().String()))
	assert.NoError(t, err)

	// make transport call.
	transport := NewHTTPTransporter(fromUpid)
	msg.UPID = toUpid
	err = transport.Send(context.TODO(), msg)
	assert.NoError(t, err)
}

func TestTransporterStartAndSend(t *testing.T) {
	serverId := "testserver"
	serverPort := getNewPort()
	serverAddr := "127.0.0.1:" + strconv.Itoa(serverPort)
	protoMsg := testmessage.GenerateSmallMessage()
	msgName := getMessageName(protoMsg)

	// setup receiver (server) process
	rcvPid, err := upid.Parse(fmt.Sprintf("%s@%s", serverId, serverAddr))
	assert.NoError(t, err)
	receiver := NewHTTPTransporter(rcvPid)

	ctrl := make(chan bool)
	reqPath := "/testserver/" + msgName
	receiver.mux.HandleFunc(reqPath, func(rsp http.ResponseWriter, req *http.Request) {
		go func() {
			ctrl <- true
		}()
	})
	err = receiver.Listen()
	assert.NoError(t, err)

	go func() {
		err = receiver.Start()
		assert.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 7) // time to catchup

	// setup sender (client) process
	sndUpid, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(t, err)

	sender := NewHTTPTransporter(sndUpid)
	msg := &Message{
		UPID:         rcvPid,
		Name:         msgName,
		ProtoMessage: protoMsg,
	}
	sender.Send(context.TODO(), msg)

	select {
	case <-time.After(time.Second * 5):
		t.Fatalf("Timeout")
	case <-ctrl:
	}
}

func TestTransporterStartAndRcvd(t *testing.T) {
	serverId := "testserver"
	serverPort := getNewPort()
	serverAddr := "127.0.0.1:" + strconv.Itoa(serverPort)
	protoMsg := testmessage.GenerateSmallMessage()
	msgName := getMessageName(protoMsg)
	ctrl := make(chan bool)

	// setup receiver (server) process
	rcvPid, err := upid.Parse(fmt.Sprintf("%s@%s", serverId, serverAddr))
	assert.NoError(t, err)
	receiver := NewHTTPTransporter(rcvPid)
	receiver.Install(msgName)
	err = receiver.Listen()
	assert.NoError(t, err)

	go func() {
		msg := receiver.Recv()
		assert.Equal(t, msgName, msg.Name)
		ctrl <- true
	}()

	go func() {
		err = receiver.Start()
		assert.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 7) // time to catchup

	// setup sender (client) process
	sndUpid, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(t, err)

	sender := NewHTTPTransporter(sndUpid)
	msg := &Message{
		UPID:         rcvPid,
		Name:         msgName,
		ProtoMessage: protoMsg,
	}
	sender.Send(context.TODO(), msg)

	select {
	case <-time.After(time.Second * 5):
		t.Fatalf("Timeout")
	case <-ctrl:
	}
}

func TestTransporterStartAndInject(t *testing.T) {
	serverId := "testserver"
	serverPort := getNewPort()
	serverAddr := "127.0.0.1:" + strconv.Itoa(serverPort)
	protoMsg := testmessage.GenerateSmallMessage()
	msgName := getMessageName(protoMsg)
	ctrl := make(chan bool)

	// setup receiver (server) process
	rcvPid, err := upid.Parse(fmt.Sprintf("%s@%s", serverId, serverAddr))
	assert.NoError(t, err)
	receiver := NewHTTPTransporter(rcvPid)
	receiver.Install(msgName)

	go func() {
		msg := receiver.Recv()
		assert.Equal(t, msgName, msg.Name)
		ctrl <- true
	}()
	time.Sleep(time.Millisecond * 7) // time to catchup

	msg := &Message{
		UPID:         rcvPid,
		Name:         msgName,
		ProtoMessage: protoMsg,
	}
	receiver.Inject(context.TODO(), msg)

	select {
	case <-time.After(time.Millisecond * 5):
		t.Fatalf("Timeout")
	case <-ctrl:
	}
}

func TestTransporterStartAndStop(t *testing.T) {
	serverId := "testserver"
	serverPort := getNewPort()
	serverAddr := "127.0.0.1:" + strconv.Itoa(serverPort)

	// setup receiver (server) process
	rcvPid, err := upid.Parse(fmt.Sprintf("%s@%s", serverId, serverAddr))
	assert.NoError(t, err)
	receiver := NewHTTPTransporter(rcvPid)
	err = receiver.Listen()
	assert.NoError(t, err)

	go func() {
		err = receiver.Start()
		assert.Error(t, err) // should fail to accept due to closed conn.
	}()
	time.Sleep(time.Millisecond * 7) // time to catchup
	receiver.Stop()
}

func makeMockServer(path string, handler func(rsp http.ResponseWriter, req *http.Request)) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc(path, handler)
	return httptest.NewServer(mux)
}
