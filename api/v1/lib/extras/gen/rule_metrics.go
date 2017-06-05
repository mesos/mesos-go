// +build ignore

package main

import (
	"os"
	"text/template"
)

func main() {
	Run(handlersTemplate, nil, os.Args...)
}

var handlersTemplate = template.Must(template.New("").Parse(`package {{.Package}}

// go generate {{.Args}}
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"context"
	"strings"

	"github.com/mesos/mesos-go/api/v1/lib/extras/metrics"
{{range .Imports}}
	{{ printf "%q" . -}}
{{end}}
)

{{.RequireType "E" -}}{{/* TODO(jdef): should support an optional return arg for use w/ calls */ -}}
func Metrics(harness metrics.Harness) Rule {
	return func(ctx context.Context, e {{.Type "E"}}, err error, ch Chain) (context.Context, {{.Type "E"}}, error) {
		typename := strings.ToLower(e.GetType().String())
		harness(func() error {
			ctx, e, err = ch(ctx, e, err)
			return err
		}, typename)
		return ctx, e, err
	}
}
`))
