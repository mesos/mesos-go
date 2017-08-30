package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/mesos/mesos-go/api/v1/lib"
)

// #include <stdio.h>
// #include <stdlib.h>
// #include <termios.h>
// #include <unistd.h>
// #include <fcntl.h>
// #include <sys/ioctl.h>
//
// /* because golang doesn't like the ... param of ioctl */
// int ioctl_winsize(int d, unsigned long request, void *buf) {
//   return ioctl(d, request, buf);
// }
//
import "C"
import "unsafe"

type cleanups struct {
	ops  []func()
	once sync.Once
}

func (c *cleanups) unwind() {
	c.once.Do(func() {
		for _, f := range c.ops {
			defer f()
		}
	})
}

func (c *cleanups) push(f func()) {
	if f != nil {
		c.ops = append(c.ops, f)
	}
}

func initTTY() (_ func(), _ <-chan mesos.TTYInfo_WindowSize, err error) {
	cleanups := new(cleanups)
	defer func() {
		if err != nil {
			cleanups.unwind()
		}
	}()

	ttyname := C.ctermid((*C.char)(unsafe.Pointer(nil)))
	if p := (*C.char)(unsafe.Pointer(ttyname)); p == nil {
		err = fmt.Errorf("failed to get tty name")
		return
	}

	ttyfd, err := syscall.Open(C.GoString(ttyname), syscall.O_RDWR, 0)
	if ttyfd < 0 {
		err = fmt.Errorf("failed to open tty device: %d", ttyfd)
		return
	}
	cleanups.push(func() { syscall.Close(ttyfd) })

	var original_termios C.struct_termios
	result := C.tcgetattr(C.int(ttyfd), &original_termios)
	if result < 0 {
		err = fmt.Errorf("failed getting termios: %d", result)
		return
	}

	new_termios := original_termios
	C.cfmakeraw(&new_termios)
	result = C.tcsetattr(C.int(ttyfd), C.TCSANOW, &new_termios)
	if result < 0 {
		err = fmt.Errorf("failed setting termios: %d", result)
		return
	}
	cleanups.push(func() {
		r := C.tcsetattr(C.int(ttyfd), C.TCSANOW, &original_termios)
		if r < 0 {
			fmt.Sprintf("failed to set original termios: %d", r)
		}
	})

	var original_winsize C.struct_winsize
	result = C.ioctl_winsize(0, C.TIOCGWINSZ, unsafe.Pointer(&original_winsize))
	if result < 0 {
		err = fmt.Errorf("failed to get winsize: %d", result)
		return
	}
	cleanups.push(func() {
		r := C.ioctl_winsize(0, C.TIOCSWINSZ, unsafe.Pointer(&original_winsize))
		if r < 0 {
			fmt.Sprintf("failed to set winsize: %d", r)
		}
	})

	cleanups.push(swapfd(uintptr(ttyfd), "tty", &os.Stdout))
	cleanups.push(swapfd(uintptr(ttyfd), "tty", &os.Stderr))
	cleanups.push(swapfd(uintptr(ttyfd), "tty", &os.Stdin))

	// translate window-size signals into chan events
	var (
		c     = make(chan os.Signal, 1)
		winch = make(chan mesos.TTYInfo_WindowSize, 1)
	)
	go func() {
		for range c {
			signal.Ignore(os.Signal(syscall.SIGWINCH))
			var temp_winsize C.struct_winsize
			r := C.ioctl_winsize(0, C.TIOCGWINSZ, unsafe.Pointer(&temp_winsize))
			if r < 0 {
				panic(fmt.Sprintf("failed to get winsize: %d", r))
			}
			winch <- mesos.TTYInfo_WindowSize{
				Rows:    uint32(temp_winsize.ws_row),
				Columns: uint32(temp_winsize.ws_col),
			}
			signal.Notify(c, os.Signal(syscall.SIGWINCH))
		}
	}()
	signal.Notify(c, os.Signal(syscall.SIGWINCH))

	// cleanup properly upon SIGTERM
	term := make(chan os.Signal, 1)
	go func() {
		<-term
		cleanups.unwind()
		println("terminating upon SIGTERM")
		os.Exit(0)
	}()
	signal.Notify(term, os.Signal(syscall.SIGTERM))

	return cleanups.unwind, nil, nil
}

func swapfd(newfd uintptr, name string, target **os.File) func() {
	f := os.NewFile(newfd, name)
	if f == nil {
		panic(fmt.Sprintf("failed to swap fd for %q", name))
	}
	old := *target
	*target = f
	return func() {
		*target = old
	}
}
