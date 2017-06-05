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

{{.RequireType "E" -}}
func Metrics(harness metrics.Harness) Rule {
	return func(ctx context.Context, e {{.Type "E"}}, {{.Arg "Z" "z," -}} err error, ch Chain) (context.Context, {{.Type "E"}}, {{.Arg "Z" "," -}} error) {
		typename := strings.ToLower(e.GetType().String())
		harness(func() error {
			ctx, e, {{.Ref "Z" "z," -}} err = ch(ctx, e, {{.Ref "Z" "z," -}} err)
			return err
		}, typename)
		return ctx, e, {{.Ref "Z" "z," -}} err
	}
}
`))
