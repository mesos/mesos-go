package encoding

import (
	"encoding/json"
	"io"

	"github.com/mesos/mesos-go/encoding/proto"
)

const (
	// ProtobufMediaType is the Protobuf serialization format media type.
	ProtobufMediaType = "application/x-protobuf"
	// JSONMediaType is the JSON serialiation format media type.
	JSONMediaType = "application/json; charset=utf-8"
)

var (
	// ProtobufCodec is the Mesos scheduler API Protobufs codec.
	ProtobufCodec = Codec{
		MediaTypes: [2]string{ProtobufMediaType, ProtobufMediaType},
		NewEncoder: NewProtobufEncoder,
		NewDecoder: NewProtobufDecoder,
	}
	// JSONCodec is the Mesos scheduler API JSON codec.
	JSONCodec = Codec{
		MediaTypes: [2]string{JSONMediaType, JSONMediaType},
		NewEncoder: NewJSONEncoder,
		NewDecoder: NewJSONDecoder,
	}
)

type (
	// A Codec composes encoding and decoding of a serialization format.
	Codec struct {
		// MediaTypes holds the media types of the codec encoding and decoding
		// formats, respectively.
		MediaTypes [2]string
		// NewEncoder returns a new encoder for the defined media type.
		NewEncoder func(io.Writer) Encoder
		// NewDecoder returns a new decoder for the defined media type.
		NewDecoder func(io.Reader) Decoder
	}
	// An Encoder encodes a given object or returns an error in case of failure.
	Encoder interface {
		Encode(interface{}) error
	}
	// A Decoder decodes a given object or returns an error in case of failure.
	Decoder interface {
		Decode(interface{}) error
	}
)

// NewProtobufEncoder returns a new Encoder of Calls to Protobuf messages written to
// the given io.Writer.
func NewProtobufEncoder(w io.Writer) Encoder { return proto.NewEncoder(w) }

// NewJSONEncoder returns a new Encoder of Calls to JSON messages written to
// the given io.Writer.
func NewJSONEncoder(w io.Writer) Encoder { return json.NewEncoder(w) }

// NewProtobufDecoder returns a new Decoder of Protobuf messages read from the
// given io.Reader to Events.
func NewProtobufDecoder(r io.Reader) Decoder { return proto.NewDecoder(r) }

// NewJSONDecoder returns a new Decoder of JSON messages read from the
// given io.Reader to Events.
func NewJSONDecoder(r io.Reader) Decoder { return json.NewDecoder(r) }
