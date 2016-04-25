package app

import (
	"log"
	"time"
)

func forever(name string, jobRestartDelay time.Duration, counter metricCounter, f func() error) {
	for {
		counter(name)
		err := f()
		if err != nil {
			log.Printf("job %q exited with error %+v", name, err)
		} else {
			log.Printf("job %q exited", name)
		}
		time.Sleep(jobRestartDelay)
	}
}
