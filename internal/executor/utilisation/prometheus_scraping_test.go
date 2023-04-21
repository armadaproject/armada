package utilisation

import (
	"errors"
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

func TestScrapeUrls_OneSuccessResponse(t *testing.T) {
	url := "http://working"

	client := makeMockHttpGetter()
	client.addSuccessResponse(url, testResponseHeader(), testResponseText())

	result := scrapeUrls([]string{url}, []string{"DCGM_FI_DEV_MEM_COPY_UTIL"}, client)

	validateParsed(t, result)
}

func TestScrapeUrls_OneSuccessOneFailResponse(t *testing.T) {
	workingUrl := "http://working"
	brokenUrl := "http://broken"

	client := makeMockHttpGetter()
	client.addSuccessResponse(workingUrl, testResponseHeader(), testResponseText())
	client.addFailureResponse(brokenUrl, errors.New("this url is broken"))

	result1 := scrapeUrls([]string{brokenUrl, workingUrl}, []string{"DCGM_FI_DEV_MEM_COPY_UTIL"}, client)
	validateParsed(t, result1)

	result2 := scrapeUrls([]string{workingUrl, brokenUrl}, []string{"DCGM_FI_DEV_MEM_COPY_UTIL"}, client)
	validateParsed(t, result2)
}

func TestParseResponse_Success(t *testing.T) {
	result, err := parseResponse(makeTestResponse(), []string{"DCGM_FI_DEV_MEM_COPY_UTIL"})

	assert.NoError(t, err)

	validateParsed(t, result)
}

func TestParseResponse_Invalid(t *testing.T) {
	response := makeResponse(testResponseHeader(), "an invalid response")

	result, err := parseResponse(response, []string{"DCGM_FI_DEV_MEM_COPY_UTIL"})

	assert.Nil(t, result)
	assert.Error(t, err)
}

func validateParsed(t *testing.T, result model.Vector) {
	assert.Equal(t, 2, len(result))

	assert.Equal(t, model.LabelValue("DCGM_FI_DEV_MEM_COPY_UTIL"), result[0].Metric[model.MetricNameLabel])
	assert.Equal(t, model.LabelValue("0"), result[0].Metric["gpu"])
	assert.Equal(t, model.LabelValue("test1"), result[0].Metric["pod"])
	assert.Equal(t, model.LabelValue("GPU-0fad1988-2940-49d6-e05a-713ae4a9ea37"), result[0].Metric["UUID"])
	assert.Equal(t, 21.0, float64(result[0].Value))

	assert.Equal(t, model.LabelValue("DCGM_FI_DEV_MEM_COPY_UTIL"), result[1].Metric[model.MetricNameLabel])
	assert.Equal(t, model.LabelValue("test2"), result[1].Metric["pod"])
	assert.Equal(t, 2.0, float64(result[1].Value))
}

func makeGoodEndpointSlice(ipAddress string, nodeName string) *discovery.EndpointSlice {
	var portNum int32 = 9400
	ready := true
	serving := true
	terminating := false
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

func testResponseText() string {
	return `# HELP DCGM_FI_DEV_MEM_COPY_UTIL Memory utilization (in %).
	# TYPE DCGM_FI_DEV_MEM_COPY_UTIL gauge
	DCGM_FI_DEV_MEM_COPY_UTIL{gpu="0",UUID="GPU-0fad1988-2940-49d6-e05a-713ae4a9ea37",device="nvidia0",modelName="Tesla V100-SXM2-32GB",Hostname="nvidia-dcgm-exporter-sv6g2",container="gputest",namespace="gpu-operator",pod="test1"} 21
	DCGM_FI_DEV_MEM_COPY_UTIL{gpu="1",UUID="GPU-4020fc4b-b520-24af-5a2d-b77b33a194a5",device="nvidia1",modelName="Tesla V100-SXM2-32GB",Hostname="nvidia-dcgm-exporter-sv6g2",container="gputest",namespace="gpu-operator",pod="test2"} 2
	# HELP DCGM_FI_DEV_ENC_UTIL Encoder utilization (in %).
	# TYPE DCGM_FI_DEV_ENC_UTIL gauge
	DCGM_FI_DEV_ENC_UTIL{gpu="0",UUID="GPU-0fad1988-2940-49d6-e05a-713ae4a9ea37",device="nvidia0",modelName="Tesla V100-SXM2-32GB",Hostname="nvidia-dcgm-exporter-sv6g2",container="gputest",namespace="gpu-operator",pod="test1"} 0
	DCGM_FI_DEV_ENC_UTIL{gpu="1",UUID="GPU-4020fc4b-b520-24af-5a2d-b77b33a194a5",device="nvidia1",modelName="Tesla V100-SXM2-32GB",Hostname="nvidia-dcgm-exporter-sv6g2",container="gputest",namespace="gpu-operator",pod="test2"} 0
	`
}

func testResponseHeader() http.Header {
	return http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}}
}

func makeTestResponse() *http.Response {
	return makeResponse(testResponseHeader(), testResponseText())
}

func makeResponse(header http.Header, body string) *http.Response {
	response := http.Response{
		Header: header,
		Body:   ioutil.NopCloser(strings.NewReader((body))),
	}
	return &response
}

func makeMockHttpGetter() *mockHttpGetter {
	return &mockHttpGetter{
		responses: map[string]struct {
			http.Header
			string
			error
		}{},
	}
}

type mockHttpGetter struct {
	responses map[string]struct {
		http.Header
		string
		error
	}
}

func (g mockHttpGetter) Get(url string) (response *http.Response, err error) {
	result := g.responses[url]
	if result.error != nil {
		return nil, result.error
	}
	return makeResponse(result.Header, result.string), nil
}

func (g mockHttpGetter) addSuccessResponse(url string, header http.Header, body string) {
	g.addResponse(url, header, body, nil)
}

func (g mockHttpGetter) addFailureResponse(url string, err error) {
	g.addResponse(url, nil, "", err)
}

func (g mockHttpGetter) addResponse(url string, header http.Header, body string, err error) {
	g.responses[url] = struct {
		http.Header
		string
		error
	}{header, body, err}
}
