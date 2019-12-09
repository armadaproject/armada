package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-openapi/analysis"
	"github.com/go-openapi/jsonreference"
	"github.com/go-openapi/loads"
	"github.com/go-openapi/spec"
)

func main() {

	grpcDoc, err := loads.Spec("internal/armada/api/api.swagger.json")
	if err != nil {
		panic(err)
	}

	definitionDoc, err := loads.Spec("internal/armada/api/api.swagger.definitions.json")
	if err != nil {
		panic(err)
	}

	definitionsSpec := definitionDoc.Spec()
	removeGoPackage("github.com/G-Research/armada/internal/armada/api", definitionsSpec.Definitions)
	prefixTypeWithGoPackageName(definitionsSpec.Definitions)

	grpcSpec := grpcDoc.Spec()
	analysis.Mixin(definitionsSpec, grpcSpec)

	resultSpec := definitionsSpec

	// Hack: Generated resourceQuantity type needs to be fixed to be string instead of object
	resultSpec.Definitions["resourceQuantity"].Type[0] = "string"

	// Hack: Easiest way to make ndjson streaming work in generated clients is to pretend the stream is actually a file
	resultSpec.Paths.Paths["/v1/job-set/{Id}"].Post.Produces = []string{"application/ndjson-stream"}
	streamDefinitions, _ := resultSpec.Extensions["x-stream-definitions"].(map[string]interface{})
	streamMessageType, _ := streamDefinitions["apiEventStreamMessage"].(map[string]interface{})
	streamMessageType["type"] = "file"

	result, err := json.MarshalIndent(resultSpec, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Print(string(result))
}

func removeGoPackage(packageName string, definitions spec.Definitions) {
	for t, def := range definitions {
		if def.VendorExtensible.Extensions["x-go-package"] == packageName {
			delete(definitions, t)
		}
	}
}

func prefixTypeWithGoPackageName(definitions spec.Definitions) {
	renames := make(map[string]string)
	for t, def := range definitions {
		pkg := def.VendorExtensible.Extensions["x-go-package"]
		path := strings.Split(fmt.Sprintf("%v", pkg), "/")
		prefix := path[len(path)-1]
		renames[t] = prefix + t
	}
	for from, to := range renames {
		renameDefinition(definitions, from, to)
	}

}

func renameDefinition(definitions spec.Definitions, from, to string) {
	definitions[to] = definitions[from]
	delete(definitions, from)

	fromRef := "#/definitions/" + from
	toRef := "#/definitions/" + to

	for _, def := range definitions {
		for pk, prop := range def.Properties {
			if len(prop.Type) > 0 && prop.Type[0] == "array" {
				s := prop.Items.Schema.Ref.String()
				if s == fromRef {
					prop.Items.Schema.Ref.Ref = jsonreference.MustCreateRef(toRef)
				}
			} else {
				s := prop.SchemaProps.Ref.String()
				if s == fromRef {
					prop.Ref.Ref = jsonreference.MustCreateRef(toRef)
				}
			}
			// make sure original object was updated
			def.Properties[pk] = prop
		}
		if def.AdditionalProperties != nil {
			additionalProperties := def.AdditionalProperties.Schema.Ref.String()
			if additionalProperties == fromRef {
				def.AdditionalProperties.Schema.Ref.Ref = jsonreference.MustCreateRef(toRef)
			}
		}
	}
}
