/*
 * CODE GENERATED AUTOMATICALLY WITH
 *    github.com/wlbr/templify
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */

package introspection

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
		"    \"title\": \"pkg/api/introspection/introspection.proto\",\n" +
		"    \"version\": \"version not set\"\n" +
		"  },\n" +
		"  \"paths\": {\n" +
		"    \"/v1/job/logs:stream\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Introspection\"\n" +
		"        ],\n" +
		"        \"summary\": \"stream logs for the pod corresponding to a job\\nif follow = true the server keeps the stream open\\nelse it sends a finite set of lines and then closes\",\n" +
		"        \"operationId\": \"GetJobLogs\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionGetJobLogsRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.(streaming responses)\",\n" +
		"            \"schema\": {\n" +
		"              \"type\": \"object\",\n" +
		"              \"title\": \"Stream result of introspectionLogLine\",\n" +
		"              \"properties\": {\n" +
		"                \"error\": {\n" +
		"                  \"$ref\": \"#/definitions/runtimeStreamError\"\n" +
		"                },\n" +
		"                \"result\": {\n" +
		"                  \"$ref\": \"#/definitions/introspectionLogLine\"\n" +
		"                }\n" +
		"              }\n" +
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
		"    },\n" +
		"    \"/v1/job/node/describe\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Introspection\"\n" +
		"        ],\n" +
		"        \"operationId\": \"DescribeNodeByJobRun\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeByJobRunRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeResponse\"\n" +
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
		"    },\n" +
		"    \"/v1/job/node/describe-by-id\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Introspection\"\n" +
		"        ],\n" +
		"        \"operationId\": \"DescribeNodeByJobId\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeByJobIdRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeResponse\"\n" +
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
		"    },\n" +
		"    \"/v1/job/node/describe-by-id-informer\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Introspection\"\n" +
		"        ],\n" +
		"        \"operationId\": \"KubeDescribeNodeByJobId\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeByJobIdRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeResponse\"\n" +
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
		"    },\n" +
		"    \"/v1/job/node/describe-informer\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Introspection\"\n" +
		"        ],\n" +
		"        \"operationId\": \"KubeDescribeNodeByJobRun\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeByJobRunRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeResponse\"\n" +
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
		"    },\n" +
		"    \"/v1/job/pod/describe\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Introspection\"\n" +
		"        ],\n" +
		"        \"operationId\": \"DescribeJobPod\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeJobPodRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeJobPodResponse\"\n" +
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
		"    },\n" +
		"    \"/v1/job/pod/describe-informer\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Introspection\"\n" +
		"        ],\n" +
		"        \"operationId\": \"KubeDescribeJobPod\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeJobPodRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeJobPodResponse\"\n" +
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
		"    },\n" +
		"    \"/v1/job/pod/describe-kubectl-cache\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Introspection\"\n" +
		"        ],\n" +
		"        \"operationId\": \"CachedKubectlDescribePod\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionCachedKubectlDescribeJobPodRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionCachedKubectlDescribeResponse\"\n" +
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
		"    },\n" +
		"    \"/v1/node/describe\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Introspection\"\n" +
		"        ],\n" +
		"        \"operationId\": \"DescribeNode\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeResponse\"\n" +
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
		"    },\n" +
		"    \"/v1/node/describe-informer\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Introspection\"\n" +
		"        ],\n" +
		"        \"operationId\": \"KubeDescribeNode\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionDescribeNodeResponse\"\n" +
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
		"    },\n" +
		"    \"/v1/node/describe-kubectl-cache\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Introspection\"\n" +
		"        ],\n" +
		"        \"operationId\": \"CachedKubectlDescribeNode\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionCachedKubectlDescribeNodeRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/introspectionCachedKubectlDescribeResponse\"\n" +
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
		"    \"introspectionCachedKubectlDescribeJobPodRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionCachedKubectlDescribeNodeRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"runId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionCachedKubectlDescribeResponse\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"cachedAt\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"output\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionContainerStatus\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"message\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"ready\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"restartCount\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"state\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionDescribeJobPodRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"cluster\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"includeEvents\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"includeRaw\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"runId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionDescribeJobPodResponse\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"cluster\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"conditions\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/introspectionPodCondition\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"containers\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/introspectionContainerStatus\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"events\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/introspectionPodEvent\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"nodeName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"phase\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNamespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podUid\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"rawPodJson\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"byte\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionDescribeNodeByJobIdRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"includeRaw\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionDescribeNodeByJobRunRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"cluster\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"includeRaw\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"runId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionDescribeNodeRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"cluster\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"includeRaw\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"nodeId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionDescribeNodeResponse\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"addresses\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/introspectionNodeAddress\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"allocatable\": {\n" +
		"          \"$ref\": \"#/definitions/introspectionResourceList\"\n" +
		"        },\n" +
		"        \"capacity\": {\n" +
		"          \"$ref\": \"#/definitions/introspectionResourceList\"\n" +
		"        },\n" +
		"        \"cluster\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"conditions\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/introspectionNodeCondition\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"labels\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"nodeName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"nodeUid\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"rawNodeJson\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"byte\"\n" +
		"        },\n" +
		"        \"taints\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/introspectionTaint\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionGetJobLogsRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"cluster\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"container\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"follow\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNamespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"previous\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"runId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"sinceTime\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"tailLines\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"timestamps\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionLogLine\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"line\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"stream\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"timestamp\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionNodeAddress\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"address\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionNodeCondition\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"lastTransitionTime\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"message\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"status\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionPodCondition\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"lastTransitionTime\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"message\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"status\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionPodEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"count\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"firstTimestamp\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"lastTimestamp\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"message\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionResourceList\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"resources\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"introspectionTaint\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"effect\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"key\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"type\": \"string\"\n" +
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
		"    \"runtimeStreamError\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"details\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/protobufAny\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"grpcCode\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"httpCode\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"httpStatus\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"message\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    }\n" +
		"  }\n" +
		"}"
	return tmpl
}
