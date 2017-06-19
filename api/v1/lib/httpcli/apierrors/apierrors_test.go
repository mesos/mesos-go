package apierrors

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
)

func TestFromResponse(t *testing.T) {
	for _, tt := range []struct {
		r *http.Response
		e error
	}{
		{nil, nil},
		{
			&http.Response{StatusCode: 200},
			nil,
		},
		{
			&http.Response{StatusCode: 400, Body: ioutil.NopCloser(bytes.NewBufferString("missing framework id"))},
			&Error{400, ErrorTable[CodeMalformedRequest] + ": missing framework id"},
		},
	} {
		rr := FromResponse(tt.r)
		if !reflect.DeepEqual(tt.e, rr) {
			t.Errorf("Expected: %v, got: %v", tt.e, rr)
		}
	}
}

func TestError(t *testing.T) {
	for _, tt := range []struct {
		code         Code
		isErr        bool
		details      string
		wantsMessage string
	}{
		{200, false, "", ""},
		{400, true, "", "malformed request"},
		{400, true, "foo", "malformed request: foo"},
	} {
		err := tt.code.Error(tt.details)
		if tt.isErr != (err != nil) {
			t.Errorf("expected isErr %v but error was %q", tt.isErr, err)
		}
		if err != nil && err.Error() != tt.wantsMessage {
			t.Errorf("Expected: %s, got: %s", tt.wantsMessage, err.Error())
		}
	}
}
