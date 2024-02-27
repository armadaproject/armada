package processor

import (
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type gangAnnotationProcessor struct {
	defaultGangNodeUniformityLabel string
}

func (p gangAnnotationProcessor) Apply(msg *armadaevents.SubmitJob) {

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
