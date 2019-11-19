package kerberos

import (
	"net"
	"net/url"
)

type ClientConfig struct {
	Enabled        bool
	KeytabLocation string
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
