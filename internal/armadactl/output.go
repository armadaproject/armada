package armadactl

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/pkg/client"
)

// OutputFormat selects how armadactl renders its output.
type OutputFormat string

const (
	// OutputText is the default, human-oriented output.
	OutputText OutputFormat = ""
	// OutputYaml renders results as a YAML document.
	OutputYaml OutputFormat = "yaml"
	// OutputJson renders results as a JSON document.
	OutputJson OutputFormat = "json"
)

// ParseOutputFormat converts the value of the --output flag into an OutputFormat.
func ParseOutputFormat(s string) (OutputFormat, error) {
	switch f := OutputFormat(s); f {
	case OutputText, OutputYaml, OutputJson:
		return f, nil
	default:
		return OutputText, errors.Errorf("invalid output format %q: must be one of yaml, json", s)
	}
}

// structured reports whether a structured output format was requested.
func (a *App) structured() bool {
	return a.Params.Output != OutputText
}

// emitDocument writes a resource document: a YAML header (apiVersion and kind) followed by the YAML
// for body. This is the single place the get commands marshal their output. For text and yaml output
// the YAML is written as is, which is what they have always printed. For json it is converted.
func (a *App) emitDocument(kind client.ResourceKind, body any) error {
	header, err := yaml.Marshal(client.Resource{Version: client.APIVersionV1, Kind: kind})
	if err != nil {
		return errors.Errorf("error marshalling resource header: %s", err)
	}
	b, err := yaml.Marshal(body)
	if err != nil {
		return errors.Errorf("error marshalling output: %s", err)
	}
	doc := string(header) + string(b)
	if a.Params.Output != OutputJson {
		fmt.Fprint(a.Out, doc)
		return nil
	}
	j, err := yaml.YAMLToJSON([]byte(doc))
	if err != nil {
		return errors.Errorf("error converting output to json: %s", err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, j, "", "  "); err != nil {
		return errors.Errorf("error formatting json output: %s", err)
	}
	fmt.Fprintf(a.Out, "%s\n", out.Bytes())
	return nil
}
