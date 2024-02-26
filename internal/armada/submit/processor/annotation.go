package processor

import (
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"strings"
)

type annotationProcessor struct {
	defaultGangNodeUniformityLabel string
}

func (p annotationProcessor) Apply(msg *armadaevents.SubmitJob) {

	annotations := msg.MainObject.GetObjectMeta().GetAnnotations()

	if annotations == nil {
		return
	}

	if _, ok := annotations[configuration.GangIdAnnotation]; ok {
		if _, ok := annotations[configuration.GangNodeUniformityLabelAnnotation]; !ok {
			annotations[configuration.GangNodeUniformityLabelAnnotation] = p.defaultGangNodeUniformityLabel
		}
	}

}

func temp(labels map[string]string, jobId string) {
	for key, value := range labels {
		value := strings.ReplaceAll(value, "{{JobId}}", ` \z`) // \z cannot be entered manually, hence its use
		value = strings.ReplaceAll(value, "{JobId}", jobId)
		labels[key] = strings.ReplaceAll(value, ` \z`, "JobId")
	}
}
