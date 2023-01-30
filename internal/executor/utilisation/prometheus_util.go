package utilisation

import (
	"github.com/prometheus/common/model"
)

func groupSamplesBy(samples model.Vector, labelName model.LabelName) map[model.LabelValue]model.Vector {
	result := map[model.LabelValue]model.Vector{}
	for _, sample := range samples {
		labelValue := sample.Metric[labelName]
		result[labelValue] = append(result[labelValue], sample)
	}
	return result
}
