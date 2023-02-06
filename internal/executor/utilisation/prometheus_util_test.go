package utilisation

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

func makeSample(val float64, labelName string, labelValue string) *model.Sample {
	metric := map[model.LabelName]model.LabelValue{
		model.LabelName(labelName): model.LabelValue(labelValue),
	}
	return &model.Sample{Value: model.SampleValue(val), Metric: metric}
}

func TestGroupSamplesBy(t *testing.T) {
	s1 := makeSample(1.0, "pod", "a")
	s2 := makeSample(2.0, "pod", "a")
	s3 := makeSample(3.0, "pod", "b")
	samples := []*model.Sample{s1, s2, s3}

	result := groupSamplesBy(samples, "pod")

	assert.Equal(t, 2, len(result))
	assert.Equal(t, 2, len(result["a"]))
	assert.Equal(t, 1, len(result["b"]))

	assert.Equal(t, s1, result["a"][0])
	assert.Equal(t, s2, result["a"][1])
	assert.Equal(t, s3, result["b"][0])
}
