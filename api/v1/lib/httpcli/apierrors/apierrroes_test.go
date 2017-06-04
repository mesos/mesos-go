package apierrors

import (
	"testing"
	"net/http"
	"io/ioutil"
	"bytes"
	"reflect"
)

func TestFromResponse(t *testing.T) {
	for _, tt := range([]struct {
		r *http.Response
		e error
	} {
		{ nil, nil },
		{
			&http.Response{ StatusCode: 200 },
			nil,
		},
		{
			&http.Response{ StatusCode: 400, Body: ioutil.NopCloser(bytes.NewBufferString("missing framework id")) },
			&Error{ 400, ErrorTable[CodeMalformedRequest], "missing framework id"},
		},
	}) {
		rr := FromResponse(tt.r)
		if !reflect.DeepEqual(tt.e, rr) {
			t.Errorf("Expected: %v, got: %v", tt.e, rr)
		}
	}
}

func TestErrorMessage(t *testing.T) {
	for _, tt := range([]struct {
		e Error
		m string
	} {
		{ Error{400, "malformed request", "missing framework id"}, "malformed request: missing framework id" },
		{ Error{400, "malformed request", ""}, "malformed request" },
	}) {
		if tt.e.Error() != tt.m {
			t.Errorf("Expected: %s, got: %s", tt.m, tt.e.Error())
		}
	}
}