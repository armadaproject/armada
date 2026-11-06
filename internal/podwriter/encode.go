package podwriter

import (
	"bytes"
	"fmt"
	"path"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/client-go/kubernetes/scheme"
)

var protoEncodingPrefix = []byte{0x6b, 0x38, 0x73, 0x00}

// Encoder encodes pods with the same protobuf format the API server uses for
// etcd storage.
type Encoder struct {
	serializer *protobuf.Serializer
}

func NewEncoder() *Encoder {
	return &Encoder{
		serializer: protobuf.NewSerializer(scheme.Scheme, scheme.Scheme),
	}
}

// EncodeKey returns the etcd key for a pod.
func EncodeKey(prefix, namespace, name string) string {
	if prefix == "" {
		prefix = "/registry"
	}
	return path.Join(prefix, "pods", namespace, name)
}

// EncodePod returns the etcd value for a pod.
func (e *Encoder) EncodePod(pod *v1.Pod) ([]byte, error) {
	pod.TypeMeta = metav1.TypeMeta{
		APIVersion: "v1",
		Kind:       "Pod",
	}
	var buffer bytes.Buffer
	if err := e.serializer.Encode(pod, &buffer); err != nil {
		return nil, fmt.Errorf("encode pod: %w", err)
	}
	if !bytes.HasPrefix(buffer.Bytes(), protoEncodingPrefix) {
		return nil, fmt.Errorf("encode pod: unexpected protobuf prefix")
	}
	return buffer.Bytes(), nil
}

// DecodePod decodes an etcd value into a pod. It is used only on the
// already-exists path to check whether the existing pod belongs to the same
// run.
func (e *Encoder) DecodePod(data []byte) (*v1.Pod, error) {
	pod := &v1.Pod{}
	if _, _, err := e.serializer.Decode(data, nil, pod); err != nil {
		return nil, fmt.Errorf("decode pod: %w", err)
	}
	return pod, nil
}
