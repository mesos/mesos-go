package encoding

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/mesos/mesos-go/api/v1/lib/encoding/framing"
	"github.com/mesos/mesos-go/api/v1/lib/encoding/proto"

	pb "github.com/gogo/protobuf/proto"
)

type MediaType string

const (
	// MediaTypeProtobuf is the Protobuf serialization format media type.
	MediaTypeProtobuf = MediaType("application/x-protobuf")
	// MediaTypeJSON is the JSON serialiation format media type.
	MediaTypeJSON = MediaType("application/json")
)

// DefaultCodecs are pre-configured default Codecs, ready to use OOTB
var DefaultCodecs = map[MediaType]Codec{
	MediaTypeProtobuf: Codec(&codec{
		name:       "protobuf",
		mediaTypes: [2]MediaType{MediaTypeProtobuf, MediaTypeProtobuf},
		newEncoder: NewProtobufEncoder,
		newDecoder: NewProtobufDecoder,
	}),
	MediaTypeJSON: Codec(&codec{
		name:       "json",
		mediaTypes: [2]MediaType{MediaTypeJSON, MediaTypeJSON},
		newEncoder: NewJSONEncoder,
		newDecoder: NewJSONDecoder,
	}),
}

// Codec returns the configured Codec for the media type, or nil if no such Codec has been configured.
func (m MediaType) Codec() Codec { return DefaultCodecs[m] }

// ContentType returns the HTTP Content-Type associated with the MediaType
func (m MediaType) ContentType() string { return string(m) }

type (
	// A Codec composes encoding and decoding of a serialization format.
	Codec interface {
		fmt.Stringer
		Name() string
		RequestType() MediaType
		ResponseType() MediaType
		// NewEncoder returns a new encoder for the defined media type.
		NewEncoder(io.Writer) Encoder
		// NewDecoder returns a new decoder for the defined media type.
		NewDecoder(framing.Reader) Decoder
	}

	codec struct {
		// Name holds the codec name.
		name string
		// MediaTypes holds the media types of the codec encoding and decoding
		// formats, respectively.
		mediaTypes [2]MediaType
		// NewEncoder returns a new encoder for the defined media type.
		newEncoder func(io.Writer) EncoderFunc
		// NewDecoder returns a new decoder for the defined media type.
		newDecoder func(framing.Reader) DecoderFunc
	}
)

// String implements the fmt.Stringer interface.
func (c *codec) String() string {
	if c == nil {
		return ""
	}
	return c.name
}

func (c *codec) Name() string                        { return c.name }
func (c *codec) RequestType() MediaType              { return c.mediaTypes[0] }
func (c *codec) ResponseType() MediaType             { return c.mediaTypes[1] }
func (c *codec) NewEncoder(w io.Writer) Encoder      { return c.newEncoder(w) }
func (c *codec) NewDecoder(r framing.Reader) Decoder { return c.newDecoder(r) }

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
	Encoder interface {
		Encode(Marshaler) error
	}

	// EncoderFunc is the functional adapter for Encoder
	EncoderFunc func(Marshaler) error

	// A Decoder decodes a given Unmarshaler or returns an error in case of failure.
	Decoder interface {
		Decode(Unmarshaler) error
	}

	// DecoderFunc is the functional adapter for Decoder
	DecoderFunc func(Unmarshaler) error
)

// Decode implements the Decoder interface
func (f DecoderFunc) Decode(u Unmarshaler) error { return f(u) }

// Encode implements the Encoder interface
func (f EncoderFunc) Encode(m Marshaler) error { return f(m) }

// NewProtobufEncoder returns a new Encoder of Calls to Protobuf messages written to
// the given io.Writer.
func NewProtobufEncoder(w io.Writer) EncoderFunc {
	enc := proto.NewEncoder(w)
	return func(m Marshaler) error { return enc.Encode(m) }
}

// NewJSONEncoder returns a new Encoder of Calls to JSON messages written to
// the given io.Writer.
func NewJSONEncoder(w io.Writer) EncoderFunc {
	enc := json.NewEncoder(w)
	return func(m Marshaler) error { return enc.Encode(m) }
}

// NewProtobufDecoder returns a new Decoder of Protobuf messages read from the
// given framing.Reader to Events.
func NewProtobufDecoder(r framing.Reader) DecoderFunc {
	var (
		uf  = func(b []byte, m interface{}) error { return pb.Unmarshal(b, m.(pb.Message)) }
		dec = framing.NewDecoder(r, uf)
	)
	return func(u Unmarshaler) error { return dec.Decode(u) }
}

// NewJSONDecoder returns a new Decoder of JSON messages read from the
// given framing.Reader to Events.
func NewJSONDecoder(r framing.Reader) DecoderFunc {
	dec := framing.NewDecoder(r, json.Unmarshal)
	return func(u Unmarshaler) error { return dec.Decode(u) }
}
