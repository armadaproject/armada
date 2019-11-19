package kerberos

import (
	"encoding/base64"
	"net"
	"net/url"
)

type ClientConfig struct {
	Enabled bool
}

func urlToSpn(apiUrl string) (string, error) {
	parsedUrl, e := url.Parse(apiUrl)
	if e != nil {
		return "", e
	}

	host, _, e := net.SplitHostPort(parsedUrl.Host)
	if e != nil {
		return "", e
	}
	return "HTTP/" + host, nil
}

func negotiateHeader(token []byte) map[string]string {
	return map[string]string{
		"authorization": "Negotiate " + base64.StdEncoding.EncodeToString(token),
	}
}
