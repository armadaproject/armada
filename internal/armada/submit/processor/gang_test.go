package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestGangAnnotationProcessor(t *testing.T) {
	tests := map[string]struct {
		config      configuration.SubmissionConfig
		annotations map[string]string
		expected    map[string]string
	}{
		"no change": {
			annotations: make(map[string]string),
			expected:    make(map[string]string),
		},
		"No change for non-gang jobs": {
			config: configuration.SubmissionConfig{
				DefaultGangNodeUniformityLabel: "foo",
			},
			annotations: make(map[string]string),
			expected:    make(map[string]string),
		},
		"Empty default": {
			annotations: map[string]string{
				configuration.GangIdAnnotation: "bar",
			},
			expected: map[string]string{
				configuration.GangIdAnnotation:                  "bar",
				configuration.GangNodeUniformityLabelAnnotation: "",
			},
		},
		"Add when missing": {
			config: configuration.SubmissionConfig{
				DefaultGangNodeUniformityLabel: "foo",
			},
			annotations: map[string]string{
				configuration.GangIdAnnotation: "bar",
			},
			expected: map[string]string{
				configuration.GangIdAnnotation:                  "bar",
				configuration.GangNodeUniformityLabelAnnotation: "foo",
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			p := gangAnnotationProcessor{
				defaultGangNodeUniformityLabel: tc.config.DefaultGangNodeUniformityLabel,
			}
			submitMsg := submitMsgFromAnnotations(tc.annotations)
			p.Apply(submitMsg)
			assert.Equal(t, submitMsgFromAnnotations(tc.expected), submitMsg)
		})
	}
}

func submitMsgFromAnnotations(annotations map[string]string) *armadaevents.SubmitJob {
	return &armadaevents.SubmitJob{
		MainObject: &armadaevents.KubernetesMainObject{
			ObjectMeta: &armadaevents.ObjectMeta{
				Annotations: annotations,
			},
		},
	}
}
