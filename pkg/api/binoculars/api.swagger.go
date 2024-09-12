/*
 * CODE GENERATED AUTOMATICALLY WITH
 *    github.com/wlbr/templify
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */

package binoculars

// SwaggerJsonTemplate is a generated function returning the template as a string.
// That string should be parsed by the functions of the golang's template package.
func SwaggerJsonTemplate() string {
	var tmpl = "{\n" +
		"  \"consumes\": [\n" +
		"    \"application/json\"\n" +
		"  ],\n" +
		"  \"produces\": [\n" +
		"    \"application/json\"\n" +
		"  ],\n" +
		"  \"swagger\": \"2.0\",\n" +
		"  \"info\": {\n" +
		"    \"title\": \"pkg/api/binoculars/binoculars.proto\",\n" +
		"    \"version\": \"version not set\"\n" +
		"  },\n" +
		"  \"paths\": {\n" +
		"    \"/v1/binoculars/cordon\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Binoculars\"\n" +
		"        ],\n" +
		"        \"operationId\": \"Cordon\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/binocularsCordonRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {}\n" +
		"          },\n" +
		"          \"default\": {\n" +
		"            \"description\": \"An unexpected error response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/runtimeError\"\n" +
		"            }\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"/v1/binoculars/log\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Binoculars\"\n" +
		"        ],\n" +
		"        \"operationId\": \"Logs\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/binocularsLogRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/binocularsLogResponse\"\n" +
		"            }\n" +
		"          },\n" +
		"          \"default\": {\n" +
		"            \"description\": \"An unexpected error response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/runtimeError\"\n" +
		"            }\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    }\n" +
		"  },\n" +
		"  \"definitions\": {\n" +
		"    \"binocularsCordonRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"nodeName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"binocularsLogLine\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"line\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"timestamp\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"binocularsLogRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"logOptions\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodLogOptions\"\n" +
		"        },\n" +
		"        \"podNamespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNumber\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"sinceTime\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"binocularsLogResponse\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"log\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/binocularsLogLine\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"protobufAny\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"typeUrl\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"byte\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"runtimeError\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"code\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"details\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/protobufAny\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"error\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"message\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1PodLogOptions\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"PodLogOptions is the query options for a Pod's logs REST call.\",\n" +
		"      \"properties\": {\n" +
		"        \"apiVersion\": {\n" +
		"          \"description\": \"APIVersion defines the versioned schema of this representation of an object.\\nServers should convert recognized schemas to the latest internal value, and\\nmay reject unrecognized values.\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"APIVersion\"\n" +
		"        },\n" +
		"        \"container\": {\n" +
		"          \"description\": \"The container for which to stream logs. Defaults to only container if there is one container in the pod.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Container\"\n" +
		"        },\n" +
		"        \"follow\": {\n" +
		"          \"description\": \"Follow the log stream of the pod. Defaults to false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Follow\"\n" +
		"        },\n" +
		"        \"insecureSkipTLSVerifyBackend\": {\n" +
		"          \"description\": \"insecureSkipTLSVerifyBackend indicates that the apiserver should not confirm the validity of the\\nserving certificate of the backend it is connecting to.  This will make the HTTPS connection between the apiserver\\nand the backend insecure. This means the apiserver cannot verify the log data it is receiving came from the real\\nkubelet.  If the kubelet is configured to verify the apiserver's TLS credentials, it does not mean the\\nconnection to the real kubelet is vulnerable to a man in the middle attack (e.g. an attacker could not intercept\\nthe actual log data coming from the real kubelet).\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"InsecureSkipTLSVerifyBackend\"\n" +
		"        },\n" +
		"        \"kind\": {\n" +
		"          \"description\": \"Kind is a string value representing the REST resource this object represents.\\nServers may infer this from the endpoint the client submits requests to.\\nCannot be updated.\\nIn CamelCase.\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Kind\"\n" +
		"        },\n" +
		"        \"limitBytes\": {\n" +
		"          \"description\": \"If set, the number of bytes to read from the server before terminating the\\nlog output. This may not display a complete final line of logging, and may return\\nslightly more or slightly less than the specified limit.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"LimitBytes\"\n" +
		"        },\n" +
		"        \"previous\": {\n" +
		"          \"description\": \"Return previous terminated container logs. Defaults to false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Previous\"\n" +
		"        },\n" +
		"        \"sinceSeconds\": {\n" +
		"          \"description\": \"A relative time in seconds before the current time from which to show logs. If this value\\nprecedes the time a pod was started, only logs since the pod start will be returned.\\nIf this value is in the future, no logs will be returned.\\nOnly one of sinceSeconds or sinceTime may be specified.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"SinceSeconds\"\n" +
		"        },\n" +
		"        \"sinceTime\": {\n" +
		"          \"description\": \"An RFC3339 timestamp from which to show logs. If this value\\nprecedes the time a pod was started, only logs since the pod start will be returned.\\nIf this value is in the future, no logs will be returned.\\nOnly one of sinceSeconds or sinceTime may be specified.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"SinceTime\"\n" +
		"        },\n" +
		"        \"tailLines\": {\n" +
		"          \"description\": \"If set, the number of lines from the end of the logs to show. If not specified,\\nlogs are shown from the creation of the container or sinceSeconds or sinceTime\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"TailLines\"\n" +
		"        },\n" +
		"        \"timestamps\": {\n" +
		"          \"description\": \"If true, add an RFC3339 or RFC3339Nano timestamp at the beginning of every line\\nof log output. Defaults to false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Timestamps\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    }\n" +
		"  }\n" +
		"}"
	return tmpl
}
