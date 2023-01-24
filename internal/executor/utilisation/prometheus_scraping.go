package utilisation

import (
	"fmt"
	"io"
	"net/http"
	"sync"

	discovery "k8s.io/api/discovery/v1"

	commonUtil "github.com/armadaproject/armada/internal/common/util"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

func getUrlsToScrape(endpointSlices []*discovery.EndpointSlice, nodeNames []string) []string {
	nodeNamesSet := commonUtil.StringListToSet(nodeNames)

	var urlsToScrape []string
	for _, endpointSlice := range endpointSlices {
		if len(endpointSlice.Ports) < 1 {
			continue
		}
		port := *endpointSlice.Ports[0].Port
		for _, endpoint := range endpointSlice.Endpoints {
			if !nodeNamesSet[*endpoint.NodeName] {
				continue
			}
			if !*endpoint.Conditions.Ready {
				continue
			}
			if !*endpoint.Conditions.Serving {
				continue
			}
			if *endpoint.Conditions.Terminating {
				continue
			}
			if len(endpoint.Addresses) < 1 {
				continue
			}
			url := fmt.Sprintf("http://%s:%d/metrics", endpoint.Addresses[0], port)
			urlsToScrape = append(urlsToScrape, url)
		}
	}
	return urlsToScrape
}

func scrapeUrls(urls []string, metricNames []string, client http.Client) model.Vector {
	vectors := make(chan model.Vector, len(urls))

	wg := sync.WaitGroup{}
	for _, url := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			vector, err := scrapeUrl(url, metricNames, client)
			if err != nil {
				log.Warnf("Error scraping custom prometheus stats from  url %s: %v", url, err)
				return
			}
			vectors <- vector
		}(url)
	}
	go func() {
		wg.Wait()
		close(vectors)
	}()

	var allVectors model.Vector
	for v := range vectors {
		for _, sample := range v {
			allVectors = append(allVectors, sample)
		}
	}
	return allVectors
}

func scrapeUrl(url string, metricNamesWanted []string, httpClient http.Client) (model.Vector, error) {
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return parseResponse(resp, metricNamesWanted)
}

func parseResponse(resp *http.Response, metricNamesWanted []string) (model.Vector, error) {
	metricNamesWantedSet := commonUtil.StringListToSet(metricNamesWanted)

	decoder := &expfmt.SampleDecoder{
		Dec:  expfmt.NewDecoder(resp.Body, expfmt.ResponseFormat(resp.Header)),
		Opts: &expfmt.DecodeOptions{},
	}

	var allSamples model.Vector
	for {
		var samples model.Vector
		err := decoder.Decode(&samples)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		for _, sample := range samples {
			metricName := sample.Metric[model.MetricNameLabel]
			if _, ok := metricNamesWantedSet[string(metricName)]; ok {
				allSamples = append(allSamples, sample)
			}
		}
	}
	return allSamples, nil
}
