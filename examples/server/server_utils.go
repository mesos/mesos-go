package server

import (
	"fmt"

	log "github.com/golang/glog"
)

func ServeExecutorArtifact(address string, port int, filePath string) string {
	filesToServe := GetDefaultMappings([]string{filePath})

	httpPath := filesToServe[0].HttpPath
	serverURI := fmt.Sprintf("%s:%d", address, port)
	hostURI := fmt.Sprintf("http://%s%s", serverURI, httpPath)

	log.Infof("Hosting artifact '%s' at '%s'", httpPath, hostURI)

	StartHttpServer(serverURI, filesToServe)
	return hostURI
}
