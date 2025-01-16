/*
 * CODE GENERATED AUTOMATICALLY WITH
 *    github.com/wlbr/templify
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */

package schedulerobjects

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
		"    \"title\": \"pkg/api/schedulerobjects/scheduler_reporting.proto\",\n" +
		"    \"version\": \"version not set\"\n" +
		"  },\n" +
		"  \"paths\": {\n" +
		"    \"/v1/job/{jobId}/scheduler-report\": {\n" +
		"      \"get\": {\n" +
		"        \"tags\": [\n" +
		"          \"SchedulerReporting\"\n" +
		"        ],\n" +
		"        \"summary\": \"Return the most recent scheduling report for each executor for the given job.\",\n" +
		"        \"operationId\": \"GetJobReport\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"type\": \"string\",\n" +
		"            \"name\": \"jobId\",\n" +
		"            \"in\": \"path\",\n" +
		"            \"required\": true\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/schedulerobjectsJobReport\"\n" +
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
		"    \"/v1/queue/{queueName}/scheduler-report\": {\n" +
		"      \"get\": {\n" +
		"        \"tags\": [\n" +
		"          \"SchedulerReporting\"\n" +
		"        ],\n" +
		"        \"summary\": \"Return the most recent report scheduling for each executor for the given queue.\",\n" +
		"        \"operationId\": \"GetQueueReport\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"type\": \"string\",\n" +
		"            \"name\": \"queueName\",\n" +
		"            \"in\": \"path\",\n" +
		"            \"required\": true\n" +
		"          },\n" +
		"          {\n" +
		"            \"type\": \"integer\",\n" +
		"            \"format\": \"int32\",\n" +
		"            \"name\": \"verbosity\",\n" +
		"            \"in\": \"query\"\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/schedulerobjectsQueueReport\"\n" +
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
		"    \"/v1/scheduling-report\": {\n" +
		"      \"get\": {\n" +
		"        \"tags\": [\n" +
		"          \"SchedulerReporting\"\n" +
		"        ],\n" +
		"        \"summary\": \"Return the most recent scheduling report for each executor.\",\n" +
		"        \"operationId\": \"GetSchedulingReport\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"type\": \"string\",\n" +
		"            \"name\": \"mostRecentForQueue.queueName\",\n" +
		"            \"in\": \"query\"\n" +
		"          },\n" +
		"          {\n" +
		"            \"type\": \"string\",\n" +
		"            \"name\": \"mostRecentForJob.jobId\",\n" +
		"            \"in\": \"query\"\n" +
		"          },\n" +
		"          {\n" +
		"            \"type\": \"integer\",\n" +
		"            \"format\": \"int32\",\n" +
		"            \"name\": \"verbosity\",\n" +
		"            \"in\": \"query\"\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/schedulerobjectsSchedulingReport\"\n" +
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
		"    \"schedulerobjectsJobReport\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"report\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"schedulerobjectsQueueReport\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"report\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"schedulerobjectsSchedulingReport\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"report\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    }\n" +
		"  }\n" +
		"}"
	return tmpl
}
