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
		"          \"$ref\": \"#/definitions/v1Time\"\n" +
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
		"    },\n" +
		"    \"v1Time\": {\n" +
		"      \"description\": \"Programs using times should typically store and pass them as values,\\nnot pointers. That is, time variables and struct fields should be of\\ntype time.Time, not *time.Time.\\n\\nA Time value can be used by multiple goroutines simultaneously except\\nthat the methods GobDecode, UnmarshalBinary, UnmarshalJSON and\\nUnmarshalText are not concurrency-safe.\\n\\nTime instants can be compared using the Before, After, and Equal methods.\\nThe Sub method subtracts two instants, producing a Duration.\\nThe Add method adds a Time and a Duration, producing a Time.\\n\\nThe zero value of type Time is January 1, year 1, 00:00:00.000000000 UTC.\\nAs this time is unlikely to come up in practice, the IsZero method gives\\na simple way of detecting a time that has not been initialized explicitly.\\n\\nEach Time has associated with it a Location, consulted when computing the\\npresentation form of the time, such as in the Format, Hour, and Year methods.\\nThe methods Local, UTC, and In return a Time with a specific location.\\nChanging the location in this way changes only the presentation; it does not\\nchange the instant in time being denoted and therefore does not affect the\\ncomputations described in earlier paragraphs.\\n\\nRepresentations of a Time value saved by the GobEncode, MarshalBinary,\\nMarshalJSON, and MarshalText methods store the Time.Location's offset, but not\\nthe location name. They therefore lose information about Daylight Saving Time.\\n\\nIn addition to the required “wall clock” reading, a Time may contain an optional\\nreading of the current process's monotonic clock, to provide additional precision\\nfor comparison or subtraction.\\nSee the “Monotonic Clocks” section in the package documentation for details.\\n\\nNote that the Go == operator compares not just the time instant but also the\\nLocation and the monotonic clock reading. Therefore, Time values should not\\nbe used as map or database keys without first guaranteeing that the\\nidentical Location has been set for all values, which can be achieved\\nthrough use of the UTC or Local method, and that the monotonic clock reading\\nhas been stripped by setting t = t.Round(0). In general, prefer t.Equal(u)\\nto t == u, since t.Equal uses the most accurate comparison available and\\ncorrectly handles the case when only one of its arguments has a monotonic\\nclock reading.\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"format\": \"date-time\",\n" +
		"      \"title\": \"A Time represents an instant in time with nanosecond precision.\",\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/apis/meta/v1\"\n" +
		"    }\n" +
		"  }\n" +
		"}"
	return tmpl
}
