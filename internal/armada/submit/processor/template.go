package processor

import (
	"strings"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

type jobIdTemplateProcessor struct{}

func (p jobIdTemplateProcessor) Apply(msg *armadaevents.SubmitJob) {
	jobId := armadaevents.MustUlidStringFromProtoUuid(msg.JobId)
	template(msg.MainObject.GetObjectMeta().GetAnnotations(), jobId)
	template(msg.MainObject.GetObjectMeta().GetAnnotations(), jobId)
}

func template(labels map[string]string, jobId string) {
	for key, value := range labels {
		value := strings.ReplaceAll(value, "{{JobId}}", ` \z`) // \z cannot be entered manually, hence its use
		value = strings.ReplaceAll(value, "{JobId}", jobId)
		labels[key] = strings.ReplaceAll(value, ` \z`, "JobId")
	}
}
