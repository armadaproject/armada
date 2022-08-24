package protoutil

import (
	"encoding/json"
	"io"

	"github.com/coreos/pkg/httputil"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
)

// Minimal implementation of marshaller using default json package, this assumes all proto messages can be nicely marshalled with it
// For kubernetes objects this works much better then default JSONpb
// To handle one of feature of protocol bufers custom json marshaller is required

type JSONMarshaller struct{}

func (*JSONMarshaller) ContentType() string {
	return httputil.JSONContentType
}

func (j *JSONMarshaller) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (j *JSONMarshaller) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (j *JSONMarshaller) NewDecoder(r io.Reader) gwruntime.Decoder {
	return json.NewDecoder(r)
}

func (j *JSONMarshaller) NewEncoder(w io.Writer) gwruntime.Encoder {
	return json.NewEncoder(w)
}

func (*JSONMarshaller) Delimiter() []byte {
	return []byte("\n")
}
