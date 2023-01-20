package main

import (
	"strings"
	"unicode"

	"github.com/gogo/protobuf/gogoproto"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	"github.com/gogo/protobuf/protoc-gen-gogo/generator"
	"github.com/gogo/protobuf/vanity"
	"github.com/gogo/protobuf/vanity/command"
)

// Used to compile proto files into go via gogo.
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
	s := generator.CamelCase(*field.Name)

	// For consistency with previous tooling.
	if gogoproto.IsNullable(field) {
		s = s + ",omitempty"
	}
	s = strings.ReplaceAll(s, "k8S", "k8s")
	s = strings.ReplaceAll(s, "K8S", "K8s")

	// Preserve casing of the first char.
	// Necessary for consistency with previous tooling.
	if unicode.IsLower(rune((*field.Name)[0])) {
		s = strings.ToLower(s[:1]) + s[1:]
	} else {
		s = strings.ToUpper(s[:1]) + s[1:]
	}

	SetStringFieldOption(gogoproto.E_Jsontag, s)(field)
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
