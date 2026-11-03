package armadactl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/pkg/client"
)

// OutputFormat selects how armadactl renders its output.
type OutputFormat string

const (
	OutputText OutputFormat = ""
	OutputYaml OutputFormat = "yaml"
	OutputJson OutputFormat = "json"
)

var OutputFormats = []string{string(OutputYaml), string(OutputJson)}

// ParseOutputFormat converts the value of the --output flag into an OutputFormat. It is case-insensitive.
func ParseOutputFormat(s string) (OutputFormat, error) {
	switch f := OutputFormat(strings.ToLower(strings.TrimSpace(s))); f {
	case OutputText, OutputYaml, OutputJson:
		return f, nil
	default:
		return OutputText, errors.Errorf("invalid output format %q: must be one of %s", s, strings.Join(OutputFormats, ", "))
	}
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
