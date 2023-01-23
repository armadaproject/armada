package utilisation

import (
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	discovery "k8s.io/api/discovery/v1"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

func TestGetUrlsToScrape_WHenEndPointIsGood_ReturnsUrl(t *testing.T) {
	endpointSlices := []*discovery.EndpointSlice{makeGoodEndpointSlice("10.0.0.1", "node1")}
	result := getUrlsToScrape(endpointSlices, []string{"node1", "node2"})

	assert.Equal(t, 1, len(result))
	assert.Equal(t, "http://10.0.0.1:9400/metrics", result[0])
}

func TestGetUrlsToScrape_WhenConditionIsBad_ReturnsNothing(t *testing.T) {
	False := false
	True := true

	endpointSlices := []*discovery.EndpointSlice{makeGoodEndpointSlice("10.0.0.1", "node1")}
	endpointSlices[0].Endpoints[0].Conditions.Ready = &False
	result := getUrlsToScrape(endpointSlices, []string{"node1"})
	assert.Equal(t, 0, len(result))

	endpointSlices = []*discovery.EndpointSlice{makeGoodEndpointSlice("10.0.0.1", "node1")}
	endpointSlices[0].Endpoints[0].Conditions.Serving = &False
	result = getUrlsToScrape(endpointSlices, []string{"node1"})
	assert.Equal(t, 0, len(result))

	endpointSlices = []*discovery.EndpointSlice{makeGoodEndpointSlice("10.0.0.1", "node1")}
	endpointSlices[0].Endpoints[0].Conditions.Terminating = &True
	result = getUrlsToScrape(endpointSlices, []string{"node1"})
	assert.Equal(t, 0, len(result))
}

func TestGetUrlsToScrape_WhenNodeNotFound_ReturnsNothing(t *testing.T) {
	endpointSlices := []*discovery.EndpointSlice{makeGoodEndpointSlice("10.0.0.1", "node1")}
	result := getUrlsToScrape(endpointSlices, []string{"anothernode"})
	assert.Equal(t, 0, len(result))
}

func TestGetUrlsToScrape_WhenNoAddresses_ReturnsNothing(t *testing.T) {
	endpointSlices := []*discovery.EndpointSlice{makeGoodEndpointSlice("10.0.0.1", "node1")}
	endpointSlices[0].Endpoints[0].Addresses = []string{}
	result := getUrlsToScrape(endpointSlices, []string{"anothernode"})
	assert.Equal(t, 0, len(result))
}

func TestParseResponse(t *testing.T) {

	content :=
		`# HELP DCGM_FI_DEV_MEM_COPY_UTIL Memory utilization (in %).
# TYPE DCGM_FI_DEV_MEM_COPY_UTIL gauge
DCGM_FI_DEV_MEM_COPY_UTIL{gpu="0",UUID="GPU-0fad1988-2940-49d6-e05a-713ae4a9ea37",device="nvidia0",modelName="Tesla V100-SXM2-32GB",Hostname="nvidia-dcgm-exporter-sv6g2",container="gputest",namespace="gpu-operator",pod="test1"} 21
DCGM_FI_DEV_MEM_COPY_UTIL{gpu="1",UUID="GPU-4020fc4b-b520-24af-5a2d-b77b33a194a5",device="nvidia1",modelName="Tesla V100-SXM2-32GB",Hostname="nvidia-dcgm-exporter-sv6g2",container="gputest",namespace="gpu-operator",pod="test2"} 2
# HELP DCGM_FI_DEV_ENC_UTIL Encoder utilization (in %).
# TYPE DCGM_FI_DEV_ENC_UTIL gauge
DCGM_FI_DEV_ENC_UTIL{gpu="0",UUID="GPU-0fad1988-2940-49d6-e05a-713ae4a9ea37",device="nvidia0",modelName="Tesla V100-SXM2-32GB",Hostname="nvidia-dcgm-exporter-sv6g2",container="gputest",namespace="gpu-operator",pod="test1"} 0
DCGM_FI_DEV_ENC_UTIL{gpu="1",UUID="GPU-4020fc4b-b520-24af-5a2d-b77b33a194a5",device="nvidia1",modelName="Tesla V100-SXM2-32GB",Hostname="nvidia-dcgm-exporter-sv6g2",container="gputest",namespace="gpu-operator",pod="test2"} 0
`

	response := http.Response{
		Header: http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}},
		Body:   ioutil.NopCloser(strings.NewReader((content))),
	}

	result, err := parseResponse(&response, []string{"DCGM_FI_DEV_MEM_COPY_UTIL"})

	assert.NoError(t, err)

	assert.Equal(t, 2, len(result))
	assert.Equal(t, model.LabelValue("0"), result[0].Metric["gpu"])
	assert.Equal(t, model.LabelValue("test1"), result[0].Metric["pod"])
	assert.Equal(t, model.LabelValue("GPU-0fad1988-2940-49d6-e05a-713ae4a9ea37"), result[0].Metric["UUID"])
	assert.Equal(t, 21.0, float64(result[0].Value))

	assert.Equal(t, model.LabelValue("test2"), result[1].Metric["pod"])
	assert.Equal(t, 2.0, float64(result[1].Value))

}

func makeGoodEndpointSlice(ipAddress string, nodeName string) *discovery.EndpointSlice {
	var portNum int32 = 9400
	var ready = true
	var serving = true
	var terminating = false
	return &discovery.EndpointSlice{
		Endpoints: []discovery.Endpoint{
			{
				Addresses: []string{ipAddress},
				NodeName:  &nodeName,
				Conditions: discovery.EndpointConditions{
					Ready:       &ready,
					Serving:     &serving,
					Terminating: &terminating,
				},
			},
		},
		Ports: []discovery.EndpointPort{
			{Port: &portNum},
		},
	}
}
