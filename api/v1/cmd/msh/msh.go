package main

// Usage: msh {...command line args...}
//
// For example:
//    msh -master 10.2.0.5:5050 -- ls -laF /tmp
//
// TODO: -gpu=1 to enable GPU_RESOURCES caps and request 1 gpu
//

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/mesos/mesos-go/api/v1/cmd/msh/app"
)

func main() {
	conf := app.DefaultConfig()
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	conf.RegisterFlags(fs)
	fs.Parse(os.Args[1:])

	conf.Command = fs.Args()
	if len(conf.Command) < 1 { // msh by itself prints usage
		fs.Usage()
		os.Exit(1)
	}

	msh := app.New(conf)
	if err := msh.Run(context.Background()); err != nil {
		if exitErr, ok := err.(app.ExitError); ok {
			if code := int(exitErr); code != 0 {
				log.Println(exitErr)
				os.Exit(code)
			}
			// else, code=0 indicates success, exit normally
		} else {
			log.Fatalf("%#v", err)
		}
	}
}
