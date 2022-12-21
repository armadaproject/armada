package main

import (
	"strings"

	"github.com/gogo/protobuf/gogoproto"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	"github.com/gogo/protobuf/protoc-gen-gogo/generator"
	"github.com/gogo/protobuf/vanity"
	"github.com/gogo/protobuf/vanity/command"
)

// Used to compile proto files into go.
// Identical to protoc-gen-gogofaster, but with json tags in camelCase.
func main() {
	req := command.Read()
	files := req.GetProtoFile()
	files = vanity.FilterFiles(files, vanity.NotGoogleProtobufDescriptorProto)

	vanity.ForEachFile(files, vanity.TurnOnMarshalerAll)
	vanity.ForEachFile(files, vanity.TurnOnSizerAll)
	vanity.ForEachFile(files, vanity.TurnOnUnmarshalerAll)

	vanity.ForEachFieldInFiles(files, JsonTagCamelCase)
	vanity.ForEachFieldInFilesExcludingExtensions(vanity.OnlyProto2(files), vanity.TurnOffNullableForNativeTypesWithoutDefaultsOnly)
	vanity.ForEachFile(files, vanity.TurnOffGoUnrecognizedAll)
	vanity.ForEachFile(files, vanity.TurnOffGoUnkeyedAll)
	vanity.ForEachFile(files, vanity.TurnOffGoSizecacheAll)

	resp := command.Generate(req)
	command.Write(resp)
}

func JsonTagCamelCase(field *descriptor.FieldDescriptorProto) {
	if gogoproto.IsNullable(field) {
		SetStringFieldOption(gogoproto.E_Jsontag, AppendOmitempty(FixCasing(generator.CamelCase(*field.Name))))(field)
	} else {
		SetStringFieldOption(gogoproto.E_Jsontag, FixCasing(generator.CamelCase(*field.Name)))(field)
	}
}

func SetStringFieldOption(extension *proto.ExtensionDesc, value string) func(field *descriptor.FieldDescriptorProto) {
	return func(field *descriptor.FieldDescriptorProto) {
		if field.Options == nil {
			field.Options = &descriptor.FieldOptions{}
		}
		if err := proto.SetExtension(field.Options, extension, &value); err != nil {
			panic(err)
		}
	}
}

// Necessary for consistency with the legacy method of generating json tags.
func FixCasing(s string) string {
	if len(s) == 0 {
		return s
	}
	s = strings.ReplaceAll(s, "k8S", "k8s")
	s = strings.ReplaceAll(s, "K8S", "K8s")
	return strings.ToLower(s[:1]) + s[1:]
}

// Necessary for consistency with the legacy method of generating json tags.
func AppendOmitempty(s string) string {
	return s + ",omitempty"
}
