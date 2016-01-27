package encoding

import (
	"encoding/json"
	"io"

	"github.com/mesos/mesos-go/encoding/framing"
	"github.com/mesos/mesos-go/encoding/proto"

	pb "github.com/gogo/protobuf/proto"
)

const (
	// ProtobufMediaType is the Protobuf serialization format media type.
	ProtobufMediaType = "application/x-protobuf"
	// JSONMediaType is the JSON serialiation format media type.
	JSONMediaType = "application/json"
)

var (
	// ProtobufCodec is the Mesos scheduler API Protobufs codec.
	ProtobufCodec = Codec{
		Name:       "protobuf",
		MediaTypes: [2]string{ProtobufMediaType, ProtobufMediaType},
		NewEncoder: NewProtobufEncoder,
		NewDecoder: NewProtobufDecoder,
	}
	// JSONCodec is the Mesos scheduler API JSON codec.
	JSONCodec = Codec{
		Name:       "json",
		MediaTypes: [2]string{JSONMediaType, JSONMediaType},
		NewEncoder: NewJSONEncoder,
		NewDecoder: NewJSONDecoder,
	}
)

// A Codec composes encoding and decoding of a serialization format.
type Codec struct {
	// Name holds the codec name.
	Name string
	// MediaTypes holds the media types of the codec encoding and decoding
	// formats, respectively.
	MediaTypes [2]string
	// NewEncoder returns a new encoder for the defined media type.
	NewEncoder func(io.Writer) Encoder
	// NewDecoder returns a new decoder for the defined media type.
	NewDecoder func(framing.Reader) Decoder
}

// String implements the fmt.Stringer interface.
func (c *Codec) String() string { return c.Name }

type (
	// Marshaler composes the supported marshaling formats.
	Marshaler interface {
		pb.Marshaler
		json.Marshaler
	}
	// Unmarshaler composes the supporter unmarshaling formats.
	Unmarshaler interface {
		pb.Unmarshaler
		json.Unmarshaler
	}
	// An Encoder encodes a given Marshaler or returns an error in case of failure.
	Encoder func(Marshaler) error
	// A Decoder decodes a given Unmarshaler or returns an error in case of failure.
	Decoder func(Unmarshaler) error
)

// Encode is an utility method that calls the Encoder itself.
func (e Encoder) Invoke(m Marshaler) error { return e(m) }

// Decode is an utility method that calls the Decoder itself.
func (d Decoder) Invoke(u Unmarshaler) error { return d(u) }

// NewProtobufEncoder returns a new Encoder of Calls to Protobuf messages written to
// the given io.Writer.
func NewProtobufEncoder(w io.Writer) Encoder {
	enc := proto.NewEncoder(w)
	return func(m Marshaler) error { return enc.Encode(m) }
}

// NewJSONEncoder returns a new Encoder of Calls to JSON messages written to
// the given io.Writer.
func NewJSONEncoder(w io.Writer) Encoder {
	enc := json.NewEncoder(w)
	return func(m Marshaler) error { return enc.Encode(m) }
}

// NewProtobufDecoder returns a new Decoder of Protobuf messages read from the
// given io.Reader to Events.
func NewProtobufDecoder(r framing.Reader) Decoder {
	uf := func(b []byte, m interface{}) error {
		return pb.Unmarshal(b, m.(pb.Message))
	}
	dec := framing.NewDecoder(r, uf)
	return func(u Unmarshaler) error { return dec.Decode(u) }
}

// NewJSONDecoder returns a new Decoder of JSON messages read from the
// given io.Reader to Events.
func NewJSONDecoder(r framing.Reader) Decoder {
	dec := framing.NewDecoder(r, json.Unmarshal)
	return func(u Unmarshaler) error { return dec.Decode(u) }
}
