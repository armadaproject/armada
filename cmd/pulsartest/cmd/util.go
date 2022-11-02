package cmd

import (
	"github.com/spf13/pflag"
)

type connFlags struct {
	url        string
	authEnable bool
	authType   string
	jwtPath    string
	topic      string
}

func processCmdFlags(flags *pflag.FlagSet) (*connFlags, error) {
	url, err := flags.GetString("url")
	if err != nil {
		return nil, err
	}
	authEnable, err := flags.GetBool("authenticationEnabled")
	if err != nil {
		return nil, err
	}
	authType, err := flags.GetString("authenticationType")
	if err != nil {
		return nil, err
	}
	jwtPath, err := flags.GetString("jwtTokenPath")
	if err != nil {
		return nil, err
	}
	jsTopic, err := flags.GetString("jobsetEventsTopic")
	if err != nil {
		return nil, err
	}

	return &connFlags{
		url:        url,
		authEnable: authEnable,
		authType:   authType,
		jwtPath:    jwtPath,
		topic:      jsTopic,
	}, nil
}
