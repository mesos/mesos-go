package messenger

import (
	"fmt"
	"math/rand"
	"os/exec"
	"path"
	"runtime"
	"testing"
	"time"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/mesos/mesos-go/messenger/testmessage"
	"github.com/mesos/mesos-go/upid"
	"github.com/mesosphere/testify/assert"
)

var (
	usedport         = 5050
	receivedMessages = 0
	done             = make(chan bool, 1)
	benchMessageCnt  = 0
)

func noopHandler(*upid.UPID, proto.Message) {
	receivedMessages++
	if receivedMessages >= benchMessageCnt {
		done <- true
	}
}

func getNewPort() int {
	usedport++
	return usedport
}

func shuffleMessages(queue *[]proto.Message) {
	for i := range *queue {
		index := rand.Intn(i + 1)
		(*queue)[i], (*queue)[index] = (*queue)[index], (*queue)[i]
	}
}

func generateSmallMessages(n int) []proto.Message {
	queue := make([]proto.Message, n)
	for i := range queue {
		queue[i] = testmessage.GenerateSmallMessage()
	}
	return queue
}

func generateMediumMessages(n int) []proto.Message {
	queue := make([]proto.Message, n)
	for i := range queue {
		queue[i] = testmessage.GenerateMediumMessage()
	}
	return queue
}

func generateBigMessages(n int) []proto.Message {
	queue := make([]proto.Message, n)
	for i := range queue {
		queue[i] = testmessage.GenerateBigMessage()
	}
	return queue
}

func generateLargeMessages(n int) []proto.Message {
	queue := make([]proto.Message, n)
	for i := range queue {
		queue[i] = testmessage.GenerateLargeMessage()
	}
	return queue
}

func generateMixedMessages(n int) []proto.Message {
	queue := make([]proto.Message, n*4)
	for i := 0; i < n*4; i = i + 4 {
		queue[i] = testmessage.GenerateSmallMessage()
		queue[i+1] = testmessage.GenerateMediumMessage()
		queue[i+2] = testmessage.GenerateBigMessage()
		queue[i+3] = testmessage.GenerateLargeMessage()
	}
	shuffleMessages(&queue)
	return queue
}

func installMessages(t *testing.T, m Messenger, queue *[]proto.Message, counts *[]int, done chan struct{}) {
	testCounts := func(counts []int, done chan struct{}) {
		for i := range counts {
			if counts[i] != cap(*queue)/4 {
				return
			}
		}
		close(done)
	}
	hander1 := func(from *upid.UPID, pbMsg proto.Message) {
		(*queue) = append(*queue, pbMsg)
		(*counts)[0]++
		testCounts(*counts, done)
	}
	hander2 := func(from *upid.UPID, pbMsg proto.Message) {
		(*queue) = append(*queue, pbMsg)
		(*counts)[1]++
		testCounts(*counts, done)
	}
	hander3 := func(from *upid.UPID, pbMsg proto.Message) {
		(*queue) = append(*queue, pbMsg)
		(*counts)[2]++
		testCounts(*counts, done)
	}
	hander4 := func(from *upid.UPID, pbMsg proto.Message) {
		(*queue) = append(*queue, pbMsg)
		(*counts)[3]++
		testCounts(*counts, done)
	}
	assert.NoError(t, m.Install(hander1, &testmessage.SmallMessage{}))
	assert.NoError(t, m.Install(hander2, &testmessage.MediumMessage{}))
	assert.NoError(t, m.Install(hander3, &testmessage.BigMessage{}))
	assert.NoError(t, m.Install(hander4, &testmessage.LargeMessage{}))
}

func runTestServer(b *testing.B) *exec.Cmd {
	_, file, _, _ := runtime.Caller(1)
	testServerPath := path.Join(path.Dir(file), "/testserver/main.go")
	cmd := exec.Command("go", "run", testServerPath)
	if err := cmd.Start(); err != nil {
		b.Fatal("Cannot run test server:", err)
	}
	return cmd
}

func TestMessengerFailToInstall(t *testing.T) {
	m := NewMesosMessenger(&upid.UPID{ID: "mesos"})
	handler := func(from *upid.UPID, pbMsg proto.Message) {}
	assert.NotNil(t, m)
	assert.NoError(t, m.Install(handler, &testmessage.SmallMessage{}))
	assert.Error(t, m.Install(handler, &testmessage.SmallMessage{}))
}

func TestMessengerFailToStart(t *testing.T) {
	m1 := NewMesosMessenger(&upid.UPID{ID: "mesos", Host: "localhost", Port: "5050"})
	m2 := NewMesosMessenger(&upid.UPID{ID: "mesos", Host: "localhost", Port: "5050"})
	assert.NoError(t, m1.Start())
	assert.Error(t, m2.Start())
}

func TestMessengerFailToSend(t *testing.T) {
	upid, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(t, err)
	m := NewMesosMessenger(upid)
	assert.NoError(t, m.Start())
	assert.Error(t, m.Send(upid, &testmessage.SmallMessage{}))
}

func TestMessenger(t *testing.T) {
	messages := generateMixedMessages(1000)

	upid1, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(t, err)
	upid2, err := upid.Parse(fmt.Sprintf("mesos2@localhost:%d", getNewPort()))
	assert.NoError(t, err)

	m1 := NewMesosMessenger(upid1)
	m2 := NewMesosMessenger(upid2)

	counts := make([]int, 4)
	msgQueue := make([]proto.Message, 0, len(messages))
	done := make(chan struct{})
	installMessages(t, m2, &msgQueue, &counts, done)

	assert.NoError(t, m1.Start())
	assert.NoError(t, m2.Start())

	go func() {
		for _, msg := range messages {
			assert.NoError(t, m1.Send(upid2, msg))
		}
	}()

	select {
	case <-time.After(time.Second * 10):
		t.Fatalf("Timeout")
	case <-done:
	}

	for i := range counts {
		assert.Equal(t, 1000, counts[i])
	}
	assert.Equal(t, messages, msgQueue)
}

func BenchmarkMessengerSendSmallMessage(b *testing.B) {
	messages := generateSmallMessages(1000)
	cmd := runTestServer(b)
	defer cmd.Process.Kill()

	upid1, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(b, err)
	upid2, err := upid.Parse("testserver@localhost:8080")
	assert.NoError(b, err)

	m1 := NewMesosMessenger(upid1)
	assert.NoError(b, m1.Start())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m1.Send(upid2, messages[i%1000])
	}
}

func BenchmarkMessengerSendMediumMessage(b *testing.B) {
	messages := generateMediumMessages(1000)
	cmd := runTestServer(b)
	defer cmd.Process.Kill()

	upid1, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(b, err)
	upid2, err := upid.Parse("testserver@localhost:8080")
	assert.NoError(b, err)

	m1 := NewMesosMessenger(upid1)
	assert.NoError(b, m1.Start())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m1.Send(upid2, messages[i%1000])
	}
}

func BenchmarkMessengerSendBigMessage(b *testing.B) {
	messages := generateBigMessages(1000)
	cmd := runTestServer(b)
	defer cmd.Process.Kill()

	upid1, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(b, err)
	upid2, err := upid.Parse("testserver@localhost:8080")
	assert.NoError(b, err)

	m1 := NewMesosMessenger(upid1)
	assert.NoError(b, m1.Start())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m1.Send(upid2, messages[i%1000])
	}
}

func BenchmarkMessengerSendLargeMessage(b *testing.B) {
	messages := generateLargeMessages(1000)
	cmd := runTestServer(b)
	defer cmd.Process.Kill()

	upid1, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(b, err)
	upid2, err := upid.Parse("testserver@localhost:8080")
	assert.NoError(b, err)

	m1 := NewMesosMessenger(upid1)
	assert.NoError(b, m1.Start())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m1.Send(upid2, messages[i%1000])
	}
}

func BenchmarkMessengerSendMixedMessage(b *testing.B) {
	messages := generateMixedMessages(1000)
	cmd := runTestServer(b)
	defer cmd.Process.Kill()

	upid1, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(b, err)
	upid2, err := upid.Parse("testserver@localhost:8080")
	assert.NoError(b, err)

	m1 := NewMesosMessenger(upid1)
	assert.NoError(b, m1.Start())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m1.Send(upid2, messages[i%1000])
	}
}

func BenchmarkMessengerSendRecvSmallMessage(b *testing.B) {
	messages := generateSmallMessages(1000)

	upid1, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(b, err)
	upid2, err := upid.Parse(fmt.Sprintf("mesos2@localhost:%d", getNewPort()))
	assert.NoError(b, err)

	m1 := NewMesosMessenger(upid1)
	m2 := NewMesosMessenger(upid2)
	assert.NoError(b, m1.Start())
	assert.NoError(b, m2.Start())
	assert.NoError(b, m2.Install(noopHandler, &testmessage.SmallMessage{}))

	time.Sleep(time.Second) // Avoid race on upid.
	receivedMessages = 0
	benchMessageCnt = b.N
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m1.Send(upid2, messages[i%1000])
	}
	<-done
}

func BenchmarkMessengerSendRecvMediumMessage(b *testing.B) {
	messages := generateMediumMessages(1000)

	upid1, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(b, err)
	upid2, err := upid.Parse(fmt.Sprintf("mesos2@localhost:%d", getNewPort()))
	assert.NoError(b, err)

	m1 := NewMesosMessenger(upid1)
	m2 := NewMesosMessenger(upid2)
	assert.NoError(b, m1.Start())
	assert.NoError(b, m2.Start())
	assert.NoError(b, m2.Install(noopHandler, &testmessage.MediumMessage{}))

	time.Sleep(time.Second) // Avoid race on upid.
	receivedMessages = 0
	benchMessageCnt = b.N
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m1.Send(upid2, messages[i%1000])
	}
	<-done
}

func BenchmarkMessengerSendRecvBigMessage(b *testing.B) {
	messages := generateBigMessages(1000)

	upid1, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(b, err)
	upid2, err := upid.Parse(fmt.Sprintf("mesos2@localhost:%d", getNewPort()))
	assert.NoError(b, err)

	m1 := NewMesosMessenger(upid1)
	m2 := NewMesosMessenger(upid2)
	assert.NoError(b, m1.Start())
	assert.NoError(b, m2.Start())
	assert.NoError(b, m2.Install(noopHandler, &testmessage.BigMessage{}))

	time.Sleep(time.Second) // Avoid race on upid.
	receivedMessages = 0
	benchMessageCnt = b.N
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m1.Send(upid2, messages[i%1000])
	}
	<-done
}

func BenchmarkMessengerSendRecvLargeMessage(b *testing.B) {
	messages := generateLargeMessages(1000)

	upid1, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(b, err)
	upid2, err := upid.Parse(fmt.Sprintf("mesos2@localhost:%d", getNewPort()))
	assert.NoError(b, err)

	m1 := NewMesosMessenger(upid1)
	m2 := NewMesosMessenger(upid2)
	assert.NoError(b, m1.Start())
	assert.NoError(b, m2.Start())
	assert.NoError(b, m2.Install(noopHandler, &testmessage.LargeMessage{}))

	time.Sleep(time.Second) // Avoid race on upid.
	receivedMessages = 0
	benchMessageCnt = b.N
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m1.Send(upid2, messages[i%1000])
	}
	<-done
}

func BenchmarkMessengerSendRecvMixedMessage(b *testing.B) {
	messages := generateMixedMessages(1000)

	upid1, err := upid.Parse(fmt.Sprintf("mesos1@localhost:%d", getNewPort()))
	assert.NoError(b, err)
	upid2, err := upid.Parse(fmt.Sprintf("mesos2@localhost:%d", getNewPort()))
	assert.NoError(b, err)

	m1 := NewMesosMessenger(upid1)
	m2 := NewMesosMessenger(upid2)
	assert.NoError(b, m1.Start())
	assert.NoError(b, m2.Start())
	assert.NoError(b, m2.Install(noopHandler, &testmessage.SmallMessage{}))
	assert.NoError(b, m2.Install(noopHandler, &testmessage.MediumMessage{}))
	assert.NoError(b, m2.Install(noopHandler, &testmessage.BigMessage{}))
	assert.NoError(b, m2.Install(noopHandler, &testmessage.LargeMessage{}))

	time.Sleep(time.Second) // Avoid race on upid.
	receivedMessages = 0
	benchMessageCnt = b.N
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m1.Send(upid2, messages[i%1000])
	}
	<-done
}
