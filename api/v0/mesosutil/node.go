package mesosutil

import (
	"net"
	"os"
	"os/exec"
	"strings"

	log "github.com/golang/glog"
)

//TODO(jdef) copied from kubernetes/pkg/util/node.go
func GetHostname(hostnameOverride string) string {
	hostname := hostnameOverride
	if hostname == "" {
		// Note: We use exec here instead of os.Hostname() because we
		// want the FQDN, and this is the easiest way to get it.
		fqdn, err := exec.Command("hostname", "-f").Output()
		if err != nil || len(fqdn) == 0 {
			log.Errorf("Couldn't determine hostname fqdn, failing back to hostname: %v", err)
			hostname, err = os.Hostname()
			if err != nil {
				log.Fatalf("Error getting hostname: %v", err)
			}
		} else {
			hostname = string(fqdn)
		}
	}
	return strings.TrimSpace(hostname)
}

// LookupIP returns the first ipv4 address it can find for the host.
// If one isn't found, it returns the first ipv6 found. Returns nil if
// the host cannot be resolved to an ipv4 or ipv6 address.
func LookupIP(host string) net.IP {
	var ipv4 net.IP
	var ipv6 net.IP

	// This is needed for the people cross-compiling from macos to linux.
	// The cross-compiled version of net.LookupIP() fails to handle plain IPs.
	// See https://github.com/mesos/mesos-go/pull/117
	if ip := net.ParseIP(host); ip != nil {
		if ipv4 = ip.To4(); ipv4 != nil {
			return ipv4
		}
		return ip.To16()
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		log.Errorf("failed to lookup IPs for host '%v': %v", host, err)
		return nil
	}

	// Find the first ipv4 and break. If none are found, keep the
	// first ipv6 found.
	for _, ip := range ips {
		if ipv4 = ip.To4(); ipv4 != nil {
			break
		}

		if ipv6 == nil {
			ipv6 = ip.To16()
		}
	}

	if ipv4 != nil {
		return ipv4
	} else if ipv6 != nil {
		log.Warningf("host does not resolve to an IPv4 address: %v", host)
		return ipv6
	} else {
		log.Errorf("host does not resolve to an IPv4 or IPv6 address: %v", host)
	}

	return nil
}
