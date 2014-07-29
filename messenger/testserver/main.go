package main

import (
	"net/http"
)

func main() {
	http.HandleFunc("/testserver/mesos.internal.SmallMessage", func(http.ResponseWriter, *http.Request) {})
	http.HandleFunc("/testserver/mesos.internal.MediumMessage", func(http.ResponseWriter, *http.Request) {})
	http.HandleFunc("/testserver/mesos.internal.BigMessage", func(http.ResponseWriter, *http.Request) {})
	http.HandleFunc("/testserver/mesos.internal.LargeMessage", func(http.ResponseWriter, *http.Request) {})
	http.ListenAndServe("localhost:8080", nil)
}
