package server

import (
	"net/http"
	"strings"

	log "github.com/golang/glog"
)

type HttpPathMapping struct {
	HttpPath string
	FilePath string
}

func registerHandler(fileToServe HttpPathMapping) {
	log.Infof("httpPath: %v\n", fileToServe.HttpPath)
	log.Infof("filePath: %v\n", fileToServe.FilePath)

	http.HandleFunc(fileToServe.HttpPath, func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, fileToServe.FilePath)
	})
}

func registerHandlers(filesToServe []HttpPathMapping) {
	for _, m := range filesToServe {
		registerHandler(m)
	}
}

func GetHttpPath(path string) string {
	// Create base path (http://foobar:5000/<base>)
	pathSplit := strings.Split(path, "/")
	var base string
	if len(pathSplit) > 0 {
		base = pathSplit[len(pathSplit)-1]
	} else {
		base = path
	}

	return "/" + base
}

func GetDefaultMappings(filePaths []string) []HttpPathMapping {
	mappings := []HttpPathMapping{}

	for _, f := range filePaths {
		m := HttpPathMapping{
			HttpPath: GetHttpPath(f),
			FilePath: f,
		}

		mappings = append(mappings, m)
	}

	return mappings
}

func StartHttpServer(address string, filesToServe []HttpPathMapping) {
	registerHandlers(filesToServe)
	go http.ListenAndServe(address, nil)
}
