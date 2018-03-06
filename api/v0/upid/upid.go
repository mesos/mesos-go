/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package upid

import (
	"fmt"
	log "github.com/golang/glog"
	"net"
	"strings"
)

// UPID is a equivalent of the UPID in libprocess.
type UPID struct {
	ID   string
	Host string
	Port string
}

// Parse parses the UPID from the input string.
func Parse(input string) (*UPID, error) {
	upid := new(UPID)

	splits := strings.Split(input, "@")
	if len(splits) != 2 {
		return nil, fmt.Errorf("Expect one `@' in the input")
	}
	upid.ID = splits[0]

	// Using network "tcp" allows us to resolve ipv4 and ipv6
	if _, err := net.ResolveTCPAddr("tcp", splits[1]); err != nil {
		return nil, err
	}
	upid.Host, upid.Port, _ = net.SplitHostPort(splits[1])
	return upid, nil
}

// String returns the string representation.
func (u UPID) String() string {
	return fmt.Sprintf("%s@%s", u.ID, net.JoinHostPort(u.Host, u.Port))
}

// Equal returns true if two upid is equal
func (u *UPID) Equal(upid *UPID) bool {
	if u == nil {
		return upid == nil
	} else {
		return upid != nil && u.ID == upid.ID && u.Host == upid.Host && u.Port == upid.Port
	}
}

// LookupIP attempts to parse hostname into an ip. If that doesn't work it will perform a
// lookup and try to find an ipv4 or ipv6 ip in the results.
func LookupIP(hostname string) (net.IP, error) {
	var ip net.IP
	parsedIP := net.ParseIP(hostname)

	if ip = parsedIP.To4(); ip != nil {
		// This is needed for the people cross-compiling from macos to linux.
		// The cross-compiled version of net.LookupIP() fails to handle plain IPs.
		// See https://github.com/mesos/mesos-go/pull/117
	} else if ip = parsedIP.To16(); ip != nil {

	} else if ips, err := net.LookupIP(hostname); err == nil {
		// Find the first ipv4 and break.
		for _, lookupIP := range ips {
			if ip = lookupIP.To4(); ip != nil {
				break
			}
		}

		if ip == nil {
			// no ipv4, best guess, just take the first addr
			if len(ips) > 0 {
				if ip = ips[0].To16(); ip != nil {
					// Attempt conversion to ipv6, else return first entry in ips
				} else {
					ip = ips[0]
				}
				log.Warningf("failed to find an IPv4 address for '%v', best guess is '%v'", hostname, ip)
			} else {
				return nil, fmt.Errorf("host does not resolve to an IPv4 or IPv6 address: %v", hostname)
			}
		}
	} else {
		return nil, fmt.Errorf("failed to lookup IPs for host '%v': %v", hostname, err)
	}

	return ip, nil
}
