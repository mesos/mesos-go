package main

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/mesos/mesos-go/encoding"
)

type URL struct{ url.URL }

func (u *URL) Set(value string) error {
	parsed, err := url.Parse(value)
	if err != nil {
		return err
	}
	u.URL = *parsed
	return nil
}

type codec struct{ *encoding.Codec }

func (c *codec) Set(value string) (err error) {
	switch strings.ToLower(value) {
	case "protobuf":
		c.Codec = &encoding.ProtobufCodec
	case "json":
		c.Codec = &encoding.JSONCodec
	default:
		err = fmt.Errorf("bad codec %q", value)
	}
	return
}
