package processor

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/util"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestTemplateProcessor(t *testing.T) {

	jobId := util.NewULID()
	jobIdProto := armadaevents.MustProtoUuidFromUlidString(jobId)

	tests := map[string]struct {
		input    *armadaevents.SubmitJob
		expected *armadaevents.SubmitJob
	}{
		"Test Template Annotations": {
			input: &armadaevents.SubmitJob{
				JobId: jobIdProto,
				MainObject: &armadaevents.KubernetesMainObject{
					ObjectMeta: &armadaevents.ObjectMeta{
						Annotations: map[string]string{
							"foo": "http://foo.com/{{JobId}}",
							"bar": "http://foo.com/{JobId}",
							"baz": "http://foo.com",
						},
					},
				},
			},
			expected: &armadaevents.SubmitJob{
				JobId: jobIdProto,
				MainObject: &armadaevents.KubernetesMainObject{
					ObjectMeta: &armadaevents.ObjectMeta{
						Annotations: map[string]string{
							"foo": "http://foo.com/JobId",
							"bar": fmt.Sprintf("http://foo.com/%s", jobId),
							"baz": "http://foo.com",
						},
					},
				},
			},
		},
		"Test Template Labels": {
			input: &armadaevents.SubmitJob{
				JobId: jobIdProto,
				MainObject: &armadaevents.KubernetesMainObject{
					ObjectMeta: &armadaevents.ObjectMeta{
						Labels: map[string]string{
							"foo": "http://foo.com/{{JobId}}",
							"bar": "http://foo.com/{JobId}",
							"baz": "http://foo.com",
						},
					},
				},
			},
			expected: &armadaevents.SubmitJob{
				JobId: jobIdProto,
				MainObject: &armadaevents.KubernetesMainObject{
					ObjectMeta: &armadaevents.ObjectMeta{
						Labels: map[string]string{
							"foo": "http://foo.com/JobId",
							"bar": fmt.Sprintf("http://foo.com/%s", jobId),
							"baz": "http://foo.com",
						},
					},
				},
			},
		},
		"Test Template Nothing": {
			input: &armadaevents.SubmitJob{
				JobId: jobIdProto,
				MainObject: &armadaevents.KubernetesMainObject{
					ObjectMeta: &armadaevents.ObjectMeta{
						Annotations: map[string]string{
							"baz": "http://foo.com",
						},
						Labels: map[string]string{
							"baz": "http://bar.com",
						},
					},
				},
			},
			expected: &armadaevents.SubmitJob{
				JobId: jobIdProto,
				MainObject: &armadaevents.KubernetesMainObject{
					ObjectMeta: &armadaevents.ObjectMeta{
						Annotations: map[string]string{
							"baz": "http://foo.com",
						},
						Labels: map[string]string{
							"baz": "http://bar.com",
						},
					},
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			p := jobIdTemplateProcessor{}
			p.Apply(tc.input)
			assert.Equal(t, tc.expected, tc.input)
		})
	}
}
