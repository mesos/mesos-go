// +build ignore

package main

import (
	"os"
	"text/template"
)

func main() {
	Run(metricsTemplate, metricsTestTemplate, os.Args...)
}

var metricsTemplate = template.Must(template.New("").Parse(`package {{.Package}}

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

{{.RequireType "E" -}}{{/* type E should have a GetType() func that returns an object w/ a String() func */ -}}
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

var metricsTestTemplate = template.Must(template.New("").Parse(`package {{.Package}}

// go generate {{.Args}}
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"context"
	"errors"
	"reflect"
	"testing"
{{range .Imports}}
	{{ printf "%q" . -}}
{{end}}
)

{{.RequireType "E" -}}
{{.RequirePrototype "E" -}}
{{.RequirePrototype "Z" -}}
func TestMetrics(t *testing.T) {
	var (
		i   int
		ctx = context.Background()
		p   = {{.Prototype "E"}}
		a   = errors.New("a")
		h   = func(f func() error, _ ...string) error {
			i++
			return f()
		}
		r = Metrics(h)
	)
	{{if .Type "Z" -}}
	var zp = {{.Prototype "Z"}}
	{{end -}}
	for ti, tc := range []struct {
		ctx context.Context
		e   {{.Type "E"}}
		{{if .Type "Z"}}
		{{- .Arg "Z" "z  "}}
		{{end -}}
		err error
	}{
		{ctx, p, {{.Ref "Z" "zp," -}} a},
		{ctx, p, {{.Ref "Z" "zp," -}} nil},
		{{if .Type "Z"}}
		{ctx, p, {{.Ref "Z" "nil," -}} a},
		{{end -}}
		{ctx, nil, {{.Ref "Z" "zp," -}} a},
	} {
		c, e, {{.Ref "Z" "z," -}} err := r.Eval(tc.ctx, tc.e, {{.Ref "Z" "tc.z," -}} tc.err, ChainIdentity)
		if !reflect.DeepEqual(c, tc.ctx) {
			t.Errorf("test case %d: expected context %q instead of %q", ti, tc.ctx, c)
		}
		if !reflect.DeepEqual(e, tc.e) {
			t.Errorf("test case %d: expected event %q instead of %q", ti, tc.e, e)
		}
		{{if .Type "Z" -}}
		if !reflect.DeepEqual(z, tc.z) {
			t.Errorf("expected return object %q instead of %q", z, tc.z)
		}
		{{end -}}
		if !reflect.DeepEqual(err, tc.err) {
			t.Errorf("test case %d: expected error %q instead of %q", ti, tc.err, err)
		}
		if y := ti + 1; y != i {
			t.Errorf("test case %d: expected count %q instead of %q", ti, y, i)
		}
	}
}
`))
