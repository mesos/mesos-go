####Benchmark of the messenger.

```shell
$ go test -v -run=Bench* -bench=. -cpu=4 -send-routines=4
PASS
BenchmarkMessengerSendSmallMessage-4 50000 58833 ns/op
BenchmarkMessengerSendMediumMessage-4 50000 58367 ns/op
BenchmarkMessengerSendBigMessage-4 50000 56553 ns/op
BenchmarkMessengerSendLargeMessage-4 50000 56711 ns/op
BenchmarkMessengerSendMixedMessage-4 50000 61250 ns/op
BenchmarkMessengerSendRecvSmallMessage-4 50000 46408 ns/op
BenchmarkMessengerSendRecvMediumMessage-4 50000 44517 ns/op
BenchmarkMessengerSendRecvBigMessage-4 50000 45937 ns/op
BenchmarkMessengerSendRecvLargeMessage-4 50000 47275 ns/op
BenchmarkMessengerSendRecvMixedMessage-4 50000 43715 ns/op
ok github.com/mesos/mesos-go/messenger 112.142s
```
 
####environment:

```
OS: Linux yifan-laptop 3.13.0-32-generic #57-Ubuntu SMP Tue Jul 15 03:51:08 UTC 2014 x86_64 x86_64 x86_64 GNU/Linux
CPU: Intel(R) Core(TM) i5-3210M CPU @ 2.50GHz
MEM: 4G DDR3 1600MHz
```
