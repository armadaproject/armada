/*
 * CODE GENERATED AUTOMATICALLY WITH
 *    github.com/wlbr/templify
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */

package api

// SwaggerJsonTemplate is a generated function returning the template as a string.
// That string should be parsed by the functions of the golang's template package.
func SwaggerJsonTemplate() string {
	var tmpl = "{\n" +
		"  \"swagger\": \"2.0\",\n" +
		"  \"info\": {\n" +
		"    \"title\": \"internal/armada/api/event.proto\",\n" +
		"    \"version\": \"version not set\"\n" +
		"  },\n" +
		"  \"consumes\": [\n" +
		"    \"application/json\"\n" +
		"  ],\n" +
		"  \"produces\": [\n" +
		"    \"application/json\"\n" +
		"  ],\n" +
		"  \"paths\": {\n" +
		"    \"/v1/job-set/{Id}\": {\n" +
		"      \"post\": {\n" +
		"        \"operationId\": \"GetJobSetEvents\",\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.(streaming responses)\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/x-stream-definitions/apiEventStreamMessage\"\n" +
		"            }\n" +
		"          }\n" +
		"        },\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"Id\",\n" +
		"            \"in\": \"path\",\n" +
		"            \"required\": true,\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiJobSetRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"tags\": [\n" +
		"          \"Event\"\n" +
		"        ],\n" +
		"        \"produces\": [\n" +
		"          \"application/ndjson-stream\"\n" +
		"        ]\n" +
		"      }\n" +
		"    },\n" +
		"    \"/v1/job/cancel\": {\n" +
		"      \"post\": {\n" +
		"        \"operationId\": \"CancelJobs\",\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiCancellationResult\"\n" +
		"            }\n" +
		"          }\n" +
		"        },\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiJobCancelRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ]\n" +
		"      }\n" +
		"    },\n" +
		"    \"/v1/job/submit\": {\n" +
		"      \"post\": {\n" +
		"        \"operationId\": \"SubmitJobs\",\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiJobSubmitResponse\"\n" +
		"            }\n" +
		"          }\n" +
		"        },\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiJobSubmitRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ]\n" +
		"      }\n" +
		"    },\n" +
		"    \"/v1/queue/{Name}\": {\n" +
		"      \"put\": {\n" +
		"        \"operationId\": \"CreateQueue\",\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"properties\": {}\n" +
		"            }\n" +
		"          }\n" +
		"        },\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"Name\",\n" +
		"            \"in\": \"path\",\n" +
		"            \"required\": true,\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiQueue\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ]\n" +
		"      }\n" +
		"    }\n" +
		"  },\n" +
		"  \"definitions\": {\n" +
		"    \"apiCancellationResult\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"CancelledIds\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiEventMessage\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"submitted\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobSubmittedEvent\"\n" +
		"        },\n" +
		"        \"queued\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobQueuedEvent\"\n" +
		"        },\n" +
		"        \"leased\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobLeasedEvent\"\n" +
		"        },\n" +
		"        \"leaseReturned\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobLeaseReturnedEvent\"\n" +
		"        },\n" +
		"        \"leaseExpired\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobLeaseExpiredEvent\"\n" +
		"        },\n" +
		"        \"pending\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobPendingEvent\"\n" +
		"        },\n" +
		"        \"running\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobRunningEvent\"\n" +
		"        },\n" +
		"        \"unableToSchedule\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobUnableToScheduleEvent\"\n" +
		"        },\n" +
		"        \"failed\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobFailedEvent\"\n" +
		"        },\n" +
		"        \"succeeded\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobSucceededEvent\"\n" +
		"        },\n" +
		"        \"reprioritized\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobReprioritizedEvent\"\n" +
		"        },\n" +
		"        \"cancelling\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobCancellingEvent\"\n" +
		"        },\n" +
		"        \"cancelled\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobCancelledEvent\"\n" +
		"        },\n" +
		"        \"terminated\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobTerminatedEvent\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiEventStreamMessage\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"Id\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"message\": {\n" +
		"          \"$ref\": \"#/definitions/apiEventMessage\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJob\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"Id\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Namespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Owner\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Priority\": {\n" +
		"          \"type\": \"number\",\n" +
		"          \"format\": \"double\"\n" +
		"        },\n" +
		"        \"PodSpec\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodSpec\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobCancelRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobCancelledEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobCancellingEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobFailedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"ClusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobLeaseExpiredEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobLeaseReturnedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"ClusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobLeasedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"ClusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobPendingEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"ClusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobQueuedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobReprioritizedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobRunningEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"ClusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSetRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"Id\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Watch\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\"\n" +
		"        },\n" +
		"        \"FromMessageId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSubmitRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobRequestItems\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiJobSubmitRequestItem\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSubmitRequestItem\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"Priority\": {\n" +
		"          \"type\": \"number\",\n" +
		"          \"format\": \"double\"\n" +
		"        },\n" +
		"        \"Namespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"PodSpec\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodSpec\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSubmitResponse\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobResponseItems\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiJobSubmitResponseItem\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSubmitResponseItem\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Error\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSubmittedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"Job\": {\n" +
		"          \"$ref\": \"#/definitions/apiJob\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSucceededEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"ClusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobTerminatedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"ClusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobUnableToScheduleEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"JobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"JobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"ClusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiQueue\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"Name\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"PriorityFactor\": {\n" +
		"          \"type\": \"number\",\n" +
		"          \"format\": \"double\"\n" +
		"        },\n" +
		"        \"UserOwners\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"GroupOwners\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"intstrIntOrString\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"type\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\"\n" +
		"        },\n" +
		"        \"intVal\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"strVal\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"+protobuf=true\\n+protobuf.options.(gogoproto.goproto_stringer)=false\\n+k8s:openapi-gen=true\",\n" +
		"      \"title\": \"IntOrString is a type that can hold an int32 or a string.  When used in\\nJSON or YAML marshalling and unmarshalling, it produces or consumes the\\ninner type.  This allows you to have, for example, a JSON field that can\\naccept a name or number.\\nTODO: Rename to Int32OrString\"\n" +
		"    },\n" +
		"    \"protobufAny\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"type_url\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"byte\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"resourceQuantity\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"description\": \"Quantity is a fixed-point representation of a number.\\nIt provides convenient marshaling/unmarshaling in JSON and YAML,\\nin addition to String() and Int64() accessors.\\n\\nThe serialization format is:\\n\\n<quantity>        ::= <signedNumber><suffix>\\n  (Note that <suffix> may be empty, from the \\\"\\\" case in <decimalSI>.)\\n<digit>           ::= 0 | 1 | ... | 9\\n<digits>          ::= <digit> | <digit><digits>\\n<number>          ::= <digits> | <digits>.<digits> | <digits>. | .<digits>\\n<sign>            ::= \\\"+\\\" | \\\"-\\\"\\n<signedNumber>    ::= <number> | <sign><number>\\n<suffix>          ::= <binarySI> | <decimalExponent> | <decimalSI>\\n<binarySI>        ::= Ki | Mi | Gi | Ti | Pi | Ei\\n  (International System of units; See: http://physics.nist.gov/cuu/Units/binary.html)\\n<decimalSI>       ::= m | \\\"\\\" | k | M | G | T | P | E\\n  (Note that 1024 = 1Ki but 1000 = 1k; I didn't choose the capitalization.)\\n<decimalExponent> ::= \\\"e\\\" <signedNumber> | \\\"E\\\" <signedNumber>\\n\\nNo matter which of the three exponent forms is used, no quantity may represent\\na number greater than 2^63-1 in magnitude, nor may it have more than 3 decimal\\nplaces. Numbers larger or more precise will be capped or rounded up.\\n(E.g.: 0.1m will rounded up to 1m.)\\nThis may be extended in the future if we require larger or smaller quantities.\\n\\nWhen a Quantity is parsed from a string, it will remember the type of suffix\\nit had, and will use the same type again when it is serialized.\\n\\nBefore serializing, Quantity will be put in \\\"canonical form\\\".\\nThis means that Exponent/suffix will be adjusted up or down (with a\\ncorresponding increase or decrease in Mantissa) such that:\\n  a. No precision is lost\\n  b. No fractional digits will be emitted\\n  c. The exponent (or suffix) is as large as possible.\\nThe sign will be omitted unless the number is negative.\\n\\nExamples:\\n  1.5 will be serialized as \\\"1500m\\\"\\n  1.5Gi will be serialized as \\\"1536Mi\\\"\\n\\nNote that the quantity will NEVER be internally represented by a\\nfloating point number. That is the whole point of this exercise.\\n\\nNon-canonical values will still parse as long as they are well formed,\\nbut will be re-emitted in their canonical form. (So always use canonical\\nform, or don't diff.)\\n\\nThis format is intended to make it difficult to use these numbers without\\nwriting some sort of special handling code in the hopes that that will\\ncause implementors to also use a fixed point implementation.\\n\\n+protobuf=true\\n+protobuf.embed=string\\n+protobuf.options.marshal=false\\n+protobuf.options.(gogoproto.goproto_stringer)=false\\n+k8s:deepcopy-gen=true\\n+k8s:openapi-gen=true\"\n" +
		"    },\n" +
		"    \"runtimeStreamError\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"grpc_code\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"http_code\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"message\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"http_status\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"details\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/protobufAny\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1AWSElasticBlockStoreVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"volumeID\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Unique ID of the persistent disk resource in AWS (Amazon EBS volume).\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type of the volume that you want to mount.\\nTip: Ensure that the filesystem type is supported by the host operating system.\\nExamples: \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore\\nTODO: how do we prevent errors in the filesystem from compromising the machine\\n+optional\"\n" +
		"        },\n" +
		"        \"partition\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"The partition in the volume that you want to mount.\\nIf omitted, the default is to mount by volume name.\\nExamples: For volume /dev/sda1, you specify the partition as \\\"1\\\".\\nSimilarly, the volume partition for /dev/sda is \\\"0\\\" (or you can leave the property empty).\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Specify \\\"true\\\" to force and set the ReadOnly property in VolumeMounts to \\\"true\\\".\\nIf omitted, the default is \\\"false\\\".\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a Persistent Disk resource in AWS.\\n\\nAn AWS EBS disk must exist before mounting to a container. The disk\\nmust also be in the same AWS zone as the kubelet. An AWS EBS disk\\ncan only be mounted as read/write once. AWS EBS volumes support\\nownership management and SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1Affinity\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"nodeAffinity\": {\n" +
		"          \"$ref\": \"#/definitions/v1NodeAffinity\",\n" +
		"          \"title\": \"Describes node affinity scheduling rules for the pod.\\n+optional\"\n" +
		"        },\n" +
		"        \"podAffinity\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodAffinity\",\n" +
		"          \"title\": \"Describes pod affinity scheduling rules (e.g. co-locate this pod in the same node, zone, etc. as some other pod(s)).\\n+optional\"\n" +
		"        },\n" +
		"        \"podAntiAffinity\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodAntiAffinity\",\n" +
		"          \"title\": \"Describes pod anti-affinity scheduling rules (e.g. avoid putting this pod in the same node, zone, etc. as some other pod(s)).\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Affinity is a group of affinity scheduling rules.\"\n" +
		"    },\n" +
		"    \"v1AzureDiskVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"diskName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The Name of the data disk in the blob storage\"\n" +
		"        },\n" +
		"        \"diskURI\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The URI the data disk in the blob storage\"\n" +
		"        },\n" +
		"        \"cachingMode\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Host Caching mode: None, Read Only, Read Write.\\n+optional\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\"\n" +
		"        },\n" +
		"        \"kind\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Expected values Shared: multiple blob disks per storage account  Dedicated: single blob disk per storage account  Managed: azure managed data disk (only in managed availability set). defaults to shared\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"AzureDisk represents an Azure Data Disk mount on the host and bind mount to the pod.\"\n" +
		"    },\n" +
		"    \"v1AzureFileVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"secretName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"the name of secret that contains Azure Storage Account Name and Key\"\n" +
		"        },\n" +
		"        \"shareName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Share Name\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"AzureFile represents an Azure File Service mount on the host and bind mount to the pod.\"\n" +
		"    },\n" +
		"    \"v1CSIVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"driver\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Driver is the name of the CSI driver that handles this volume.\\nConsult with your admin for the correct name as registered in the cluster.\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Specifies a read-only configuration for the volume.\\nDefaults to false (read/write).\\n+optional\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type to mount. Ex. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\".\\nIf not provided, the empty value is passed to the associated CSI driver\\nwhich will determine the default filesystem to apply.\\n+optional\"\n" +
		"        },\n" +
		"        \"volumeAttributes\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"VolumeAttributes stores driver-specific properties that are passed to the CSI\\ndriver. Consult your driver's documentation for supported values.\\n+optional\"\n" +
		"        },\n" +
		"        \"nodePublishSecretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"title\": \"NodePublishSecretRef is a reference to the secret object containing\\nsensitive information to pass to the CSI driver to complete the CSI\\nNodePublishVolume and NodeUnpublishVolume calls.\\nThis field is optional, and  may be empty if no secret is required. If the\\nsecret object contains more than one secret, all secret references are passed.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"Represents a source location of a volume to mount, managed by an external CSI driver\"\n" +
		"    },\n" +
		"    \"v1Capabilities\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"add\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"Added capabilities\\n+optional\"\n" +
		"        },\n" +
		"        \"drop\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"Removed capabilities\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Adds and removes POSIX capabilities from running containers.\"\n" +
		"    },\n" +
		"    \"v1CephFSVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"monitors\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"Required: Monitors is a collection of Ceph monitors\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/cephfs/README.md#how-to-use-it\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Optional: Used as the mounted root, rather than the full Ceph tree, default is /\\n+optional\"\n" +
		"        },\n" +
		"        \"user\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Optional: User is the rados user name, default is admin\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/cephfs/README.md#how-to-use-it\\n+optional\"\n" +
		"        },\n" +
		"        \"secretFile\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Optional: SecretFile is the path to key ring for User, default is /etc/ceph/user.secret\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/cephfs/README.md#how-to-use-it\\n+optional\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"title\": \"Optional: SecretRef is reference to the authentication secret for User, default is empty.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/cephfs/README.md#how-to-use-it\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Optional: Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/cephfs/README.md#how-to-use-it\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a Ceph Filesystem mount that lasts the lifetime of a pod\\nCephfs volumes do not support ownership management or SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1CinderVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"volumeID\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"volume id used to identify the volume in cinder\\nMore info: https://releases.k8s.io/HEAD/examples/mysql-cinder-pd/README.md\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nExamples: \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nMore info: https://releases.k8s.io/HEAD/examples/mysql-cinder-pd/README.md\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Optional: Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\nMore info: https://releases.k8s.io/HEAD/examples/mysql-cinder-pd/README.md\\n+optional\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"title\": \"Optional: points to a secret object containing parameters used to connect\\nto OpenStack.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a cinder volume resource in Openstack.\\nA Cinder volume must exist before mounting to a container.\\nThe volume must also be in the same region as the kubelet.\\nCinder volumes support ownership management and SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1ConfigMapEnvSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"localObjectReference\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"description\": \"The ConfigMap to select from.\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Specify whether the ConfigMap must be defined\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"ConfigMapEnvSource selects a ConfigMap to populate the environment\\nvariables with.\\n\\nThe contents of the target ConfigMap's Data field will represent the\\nkey-value pairs as environment variables.\"\n" +
		"    },\n" +
		"    \"v1ConfigMapKeySelector\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"localObjectReference\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"description\": \"The ConfigMap to select from.\"\n" +
		"        },\n" +
		"        \"key\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"The key to select.\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Specify whether the ConfigMap or its key must be defined\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Selects a key from a ConfigMap.\"\n" +
		"    },\n" +
		"    \"v1ConfigMapProjection\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"localObjectReference\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"        },\n" +
		"        \"items\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1KeyToPath\"\n" +
		"          },\n" +
		"          \"title\": \"If unspecified, each key-value pair in the Data field of the referenced\\nConfigMap will be projected into the volume as a file whose name is the\\nkey and content is the value. If specified, the listed keys will be\\nprojected into the specified paths, and unlisted keys will not be\\npresent. If a key is specified which is not present in the ConfigMap,\\nthe volume setup will error unless it is marked optional. Paths must be\\nrelative and may not contain the '..' path or start with '..'.\\n+optional\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Specify whether the ConfigMap or its keys must be defined\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Adapts a ConfigMap into a projected volume.\\n\\nThe contents of the target ConfigMap's Data field will be presented in a\\nprojected volume as files using the keys in the Data field as the file names,\\nunless the items element is populated with specific mappings of keys to paths.\\nNote that this is identical to a configmap volume source without the default\\nmode.\"\n" +
		"    },\n" +
		"    \"v1ConfigMapVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"localObjectReference\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"        },\n" +
		"        \"items\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1KeyToPath\"\n" +
		"          },\n" +
		"          \"title\": \"If unspecified, each key-value pair in the Data field of the referenced\\nConfigMap will be projected into the volume as a file whose name is the\\nkey and content is the value. If specified, the listed keys will be\\nprojected into the specified paths, and unlisted keys will not be\\npresent. If a key is specified which is not present in the ConfigMap,\\nthe volume setup will error unless it is marked optional. Paths must be\\nrelative and may not contain the '..' path or start with '..'.\\n+optional\"\n" +
		"        },\n" +
		"        \"defaultMode\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Optional: mode bits to use on created files by default. Must be a\\nvalue between 0 and 0777. Defaults to 0644.\\nDirectories within the path are not affected by this setting.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Specify whether the ConfigMap or its keys must be defined\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Adapts a ConfigMap into a volume.\\n\\nThe contents of the target ConfigMap's Data field will be presented in a\\nvolume as files using the keys in the Data field as the file names, unless\\nthe items element is populated with specific mappings of keys to paths.\\nConfigMap volumes support ownership management and SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1Container\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Name of the container specified as a DNS_LABEL.\\nEach container in a pod must have a unique name (DNS_LABEL).\\nCannot be updated.\"\n" +
		"        },\n" +
		"        \"image\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Docker image name.\\nMore info: https://kubernetes.io/docs/concepts/containers/images\\nThis field is optional to allow higher level config management to default or override\\ncontainer images in workload controllers like Deployments and StatefulSets.\\n+optional\"\n" +
		"        },\n" +
		"        \"command\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"Entrypoint array. Not executed within a shell.\\nThe docker image's ENTRYPOINT is used if this is not provided.\\nVariable references $(VAR_NAME) are expanded using the container's environment. If a variable\\ncannot be resolved, the reference in the input string will be unchanged. The $(VAR_NAME) syntax\\ncan be escaped with a double $$, ie: $$(VAR_NAME). Escaped references will never be expanded,\\nregardless of whether the variable exists or not.\\nCannot be updated.\\nMore info: https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell\\n+optional\"\n" +
		"        },\n" +
		"        \"args\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"Arguments to the entrypoint.\\nThe docker image's CMD is used if this is not provided.\\nVariable references $(VAR_NAME) are expanded using the container's environment. If a variable\\ncannot be resolved, the reference in the input string will be unchanged. The $(VAR_NAME) syntax\\ncan be escaped with a double $$, ie: $$(VAR_NAME). Escaped references will never be expanded,\\nregardless of whether the variable exists or not.\\nCannot be updated.\\nMore info: https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell\\n+optional\"\n" +
		"        },\n" +
		"        \"workingDir\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Container's working directory.\\nIf not specified, the container runtime's default will be used, which\\nmight be configured in the container image.\\nCannot be updated.\\n+optional\"\n" +
		"        },\n" +
		"        \"ports\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1ContainerPort\"\n" +
		"          },\n" +
		"          \"title\": \"List of ports to expose from the container. Exposing a port here gives\\nthe system additional information about the network connections a\\ncontainer uses, but is primarily informational. Not specifying a port here\\nDOES NOT prevent that port from being exposed. Any port which is\\nlistening on the default \\\"0.0.0.0\\\" address inside a container will be\\naccessible from the network.\\nCannot be updated.\\n+optional\\n+patchMergeKey=containerPort\\n+patchStrategy=merge\\n+listType=map\\n+listMapKey=containerPort\\n+listMapKey=protocol\"\n" +
		"        },\n" +
		"        \"envFrom\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1EnvFromSource\"\n" +
		"          },\n" +
		"          \"title\": \"List of sources to populate environment variables in the container.\\nThe keys defined within a source must be a C_IDENTIFIER. All invalid keys\\nwill be reported as an event when the container is starting. When a key exists in multiple\\nsources, the value associated with the last source will take precedence.\\nValues defined by an Env with a duplicate key will take precedence.\\nCannot be updated.\\n+optional\"\n" +
		"        },\n" +
		"        \"env\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1EnvVar\"\n" +
		"          },\n" +
		"          \"title\": \"List of environment variables to set in the container.\\nCannot be updated.\\n+optional\\n+patchMergeKey=name\\n+patchStrategy=merge\"\n" +
		"        },\n" +
		"        \"resources\": {\n" +
		"          \"$ref\": \"#/definitions/v1ResourceRequirements\",\n" +
		"          \"title\": \"Compute Resources required by this container.\\nCannot be updated.\\nMore info: https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/\\n+optional\"\n" +
		"        },\n" +
		"        \"volumeMounts\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1VolumeMount\"\n" +
		"          },\n" +
		"          \"title\": \"Pod volumes to mount into the container's filesystem.\\nCannot be updated.\\n+optional\\n+patchMergeKey=mountPath\\n+patchStrategy=merge\"\n" +
		"        },\n" +
		"        \"volumeDevices\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1VolumeDevice\"\n" +
		"          },\n" +
		"          \"title\": \"volumeDevices is the list of block devices to be used by the container.\\nThis is a beta feature.\\n+patchMergeKey=devicePath\\n+patchStrategy=merge\\n+optional\"\n" +
		"        },\n" +
		"        \"livenessProbe\": {\n" +
		"          \"$ref\": \"#/definitions/v1Probe\",\n" +
		"          \"title\": \"Periodic probe of container liveness.\\nContainer will be restarted if the probe fails.\\nCannot be updated.\\nMore info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes\\n+optional\"\n" +
		"        },\n" +
		"        \"readinessProbe\": {\n" +
		"          \"$ref\": \"#/definitions/v1Probe\",\n" +
		"          \"title\": \"Periodic probe of container service readiness.\\nContainer will be removed from service endpoints if the probe fails.\\nCannot be updated.\\nMore info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes\\n+optional\"\n" +
		"        },\n" +
		"        \"lifecycle\": {\n" +
		"          \"$ref\": \"#/definitions/v1Lifecycle\",\n" +
		"          \"title\": \"Actions that the management system should take in response to container lifecycle events.\\nCannot be updated.\\n+optional\"\n" +
		"        },\n" +
		"        \"terminationMessagePath\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Optional: Path at which the file to which the container's termination message\\nwill be written is mounted into the container's filesystem.\\nMessage written is intended to be brief final status, such as an assertion failure message.\\nWill be truncated by the node if greater than 4096 bytes. The total message length across\\nall containers will be limited to 12kb.\\nDefaults to /dev/termination-log.\\nCannot be updated.\\n+optional\"\n" +
		"        },\n" +
		"        \"terminationMessagePolicy\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Indicate how the termination message should be populated. File will use the contents of\\nterminationMessagePath to populate the container status message on both success and failure.\\nFallbackToLogsOnError will use the last chunk of container log output if the termination\\nmessage file is empty and the container exited with an error.\\nThe log output is limited to 2048 bytes or 80 lines, whichever is smaller.\\nDefaults to File.\\nCannot be updated.\\n+optional\"\n" +
		"        },\n" +
		"        \"imagePullPolicy\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Image pull policy.\\nOne of Always, Never, IfNotPresent.\\nDefaults to Always if :latest tag is specified, or IfNotPresent otherwise.\\nCannot be updated.\\nMore info: https://kubernetes.io/docs/concepts/containers/images#updating-images\\n+optional\"\n" +
		"        },\n" +
		"        \"securityContext\": {\n" +
		"          \"$ref\": \"#/definitions/v1SecurityContext\",\n" +
		"          \"title\": \"Security options the pod should run with.\\nMore info: https://kubernetes.io/docs/concepts/policy/security-context/\\nMore info: https://kubernetes.io/docs/tasks/configure-pod-container/security-context/\\n+optional\"\n" +
		"        },\n" +
		"        \"stdin\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Whether this container should allocate a buffer for stdin in the container runtime. If this\\nis not set, reads from stdin in the container will always result in EOF.\\nDefault is false.\\n+optional\"\n" +
		"        },\n" +
		"        \"stdinOnce\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Whether the container runtime should close the stdin channel after it has been opened by\\na single attach. When stdin is true the stdin stream will remain open across multiple attach\\nsessions. If stdinOnce is set to true, stdin is opened on container start, is empty until the\\nfirst client attaches to stdin, and then remains open and accepts data until the client disconnects,\\nat which time stdin is closed and remains closed until the container is restarted. If this\\nflag is false, a container processes that reads from stdin will never receive an EOF.\\nDefault is false\\n+optional\"\n" +
		"        },\n" +
		"        \"tty\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Whether this container should allocate a TTY for itself, also requires 'stdin' to be true.\\nDefault is false.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"A single application container that you want to run within a pod.\"\n" +
		"    },\n" +
		"    \"v1ContainerPort\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"If specified, this must be an IANA_SVC_NAME and unique within the pod. Each\\nnamed port in a pod must have a unique name. Name for the port that can be\\nreferred to by services.\\n+optional\"\n" +
		"        },\n" +
		"        \"hostPort\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Number of port to expose on the host.\\nIf specified, this must be a valid port number, 0 < x < 65536.\\nIf HostNetwork is specified, this must match ContainerPort.\\nMost containers do not need this.\\n+optional\"\n" +
		"        },\n" +
		"        \"containerPort\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"description\": \"Number of port to expose on the pod's IP address.\\nThis must be a valid port number, 0 < x < 65536.\"\n" +
		"        },\n" +
		"        \"protocol\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Protocol for port. Must be UDP, TCP, or SCTP.\\nDefaults to \\\"TCP\\\".\\n+optional\"\n" +
		"        },\n" +
		"        \"hostIP\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"What host IP to bind the external port to.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"ContainerPort represents a network port in a single container.\"\n" +
		"    },\n" +
		"    \"v1DownwardAPIProjection\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"items\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1DownwardAPIVolumeFile\"\n" +
		"          },\n" +
		"          \"title\": \"Items is a list of DownwardAPIVolume file\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents downward API info for projecting into a projected volume.\\nNote that this is identical to a downwardAPI volume source without the default\\nmode.\"\n" +
		"    },\n" +
		"    \"v1DownwardAPIVolumeFile\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"path\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Required: Path is  the relative path name of the file to be created. Must not be absolute or contain the '..' path. Must be utf-8 encoded. The first item of the relative path must not start with '..'\"\n" +
		"        },\n" +
		"        \"fieldRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ObjectFieldSelector\",\n" +
		"          \"title\": \"Required: Selects a field of the pod: only annotations, labels, name and namespace are supported.\\n+optional\"\n" +
		"        },\n" +
		"        \"resourceFieldRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ResourceFieldSelector\",\n" +
		"          \"title\": \"Selects a resource of the container: only resources limits and requests\\n(limits.cpu, limits.memory, requests.cpu and requests.memory) are currently supported.\\n+optional\"\n" +
		"        },\n" +
		"        \"mode\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Optional: mode bits to use on this file, must be a value between 0\\nand 0777. If not specified, the volume defaultMode will be used.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"DownwardAPIVolumeFile represents information to create the file containing the pod field\"\n" +
		"    },\n" +
		"    \"v1DownwardAPIVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"items\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1DownwardAPIVolumeFile\"\n" +
		"          },\n" +
		"          \"title\": \"Items is a list of downward API volume file\\n+optional\"\n" +
		"        },\n" +
		"        \"defaultMode\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Optional: mode bits to use on created files by default. Must be a\\nvalue between 0 and 0777. Defaults to 0644.\\nDirectories within the path are not affected by this setting.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"DownwardAPIVolumeSource represents a volume containing downward API info.\\nDownward API volumes support ownership management and SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1EmptyDirVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"medium\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"What type of storage medium should back this directory.\\nThe default is \\\"\\\" which means to use the node's default medium.\\nMust be an empty string (default) or Memory.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#emptydir\\n+optional\"\n" +
		"        },\n" +
		"        \"sizeLimit\": {\n" +
		"          \"$ref\": \"#/definitions/resourceQuantity\",\n" +
		"          \"title\": \"Total amount of local storage required for this EmptyDir volume.\\nThe size limit is also applicable for memory medium.\\nThe maximum usage on memory medium EmptyDir would be the minimum value between\\nthe SizeLimit specified here and the sum of memory limits of all containers in a pod.\\nThe default is nil which means that the limit is undefined.\\nMore info: http://kubernetes.io/docs/user-guide/volumes#emptydir\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents an empty directory for a pod.\\nEmpty directory volumes support ownership management and SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1EnvFromSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"prefix\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"An optional identifier to prepend to each key in the ConfigMap. Must be a C_IDENTIFIER.\\n+optional\"\n" +
		"        },\n" +
		"        \"configMapRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ConfigMapEnvSource\",\n" +
		"          \"title\": \"The ConfigMap to select from\\n+optional\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1SecretEnvSource\",\n" +
		"          \"title\": \"The Secret to select from\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"EnvFromSource represents the source of a set of ConfigMaps\"\n" +
		"    },\n" +
		"    \"v1EnvVar\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Name of the environment variable. Must be a C_IDENTIFIER.\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Variable references $(VAR_NAME) are expanded\\nusing the previous defined environment variables in the container and\\nany service environment variables. If a variable cannot be resolved,\\nthe reference in the input string will be unchanged. The $(VAR_NAME)\\nsyntax can be escaped with a double $$, ie: $$(VAR_NAME). Escaped\\nreferences will never be expanded, regardless of whether the variable\\nexists or not.\\nDefaults to \\\"\\\".\\n+optional\"\n" +
		"        },\n" +
		"        \"valueFrom\": {\n" +
		"          \"$ref\": \"#/definitions/v1EnvVarSource\",\n" +
		"          \"title\": \"Source for the environment variable's value. Cannot be used if value is not empty.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"EnvVar represents an environment variable present in a Container.\"\n" +
		"    },\n" +
		"    \"v1EnvVarSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"fieldRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ObjectFieldSelector\",\n" +
		"          \"title\": \"Selects a field of the pod: supports metadata.name, metadata.namespace, metadata.labels, metadata.annotations,\\nspec.nodeName, spec.serviceAccountName, status.hostIP, status.podIP.\\n+optional\"\n" +
		"        },\n" +
		"        \"resourceFieldRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ResourceFieldSelector\",\n" +
		"          \"title\": \"Selects a resource of the container: only resources limits and requests\\n(limits.cpu, limits.memory, limits.ephemeral-storage, requests.cpu, requests.memory and requests.ephemeral-storage) are currently supported.\\n+optional\"\n" +
		"        },\n" +
		"        \"configMapKeyRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ConfigMapKeySelector\",\n" +
		"          \"title\": \"Selects a key of a ConfigMap.\\n+optional\"\n" +
		"        },\n" +
		"        \"secretKeyRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1SecretKeySelector\",\n" +
		"          \"title\": \"Selects a key of a secret in the pod's namespace\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"EnvVarSource represents a source for the value of an EnvVar.\"\n" +
		"    },\n" +
		"    \"v1ExecAction\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"command\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"Command is the command line to execute inside the container, the working directory for the\\ncommand  is root ('/') in the container's filesystem. The command is simply exec'd, it is\\nnot run inside a shell, so traditional shell instructions ('|', etc) won't work. To use\\na shell, you need to explicitly call out to that shell.\\nExit status of 0 is treated as live/healthy and non-zero is unhealthy.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"ExecAction describes a \\\"run in container\\\" action.\"\n" +
		"    },\n" +
		"    \"v1FCVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"targetWWNs\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"Optional: FC target worldwide names (WWNs)\\n+optional\"\n" +
		"        },\n" +
		"        \"lun\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Optional: FC target lun number\\n+optional\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nTODO: how do we prevent errors in the filesystem from compromising the machine\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Optional: Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\"\n" +
		"        },\n" +
		"        \"wwids\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"Optional: FC volume world wide identifiers (wwids)\\nEither wwids or combination of targetWWNs and lun must be set, but not both simultaneously.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a Fibre Channel volume.\\nFibre Channel volumes can only be mounted as read/write once.\\nFibre Channel volumes support ownership management and SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1FlexVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"driver\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Driver is the name of the driver to use for this volume.\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". The default filesystem depends on FlexVolume script.\\n+optional\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"title\": \"Optional: SecretRef is reference to the secret object containing\\nsensitive information to pass to the plugin scripts. This may be\\nempty if no secret object is specified. If the secret object\\ncontains more than one secret, all secrets are passed to the plugin\\nscripts.\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Optional: Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\"\n" +
		"        },\n" +
		"        \"options\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"Optional: Extra command options if any.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"FlexVolume represents a generic volume resource that is\\nprovisioned/attached using an exec based plugin.\"\n" +
		"    },\n" +
		"    \"v1FlockerVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"datasetName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Name of the dataset stored as metadata -> name on the dataset for Flocker\\nshould be considered as deprecated\\n+optional\"\n" +
		"        },\n" +
		"        \"datasetUUID\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"UUID of the dataset. This is unique identifier of a Flocker dataset\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a Flocker volume mounted by the Flocker agent.\\nOne and only one of datasetName and datasetUUID should be set.\\nFlocker volumes do not support ownership management or SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1GCEPersistentDiskVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"pdName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Unique name of the PD resource in GCE. Used to identify the disk in GCE.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type of the volume that you want to mount.\\nTip: Ensure that the filesystem type is supported by the host operating system.\\nExamples: \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk\\nTODO: how do we prevent errors in the filesystem from compromising the machine\\n+optional\"\n" +
		"        },\n" +
		"        \"partition\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"The partition in the volume that you want to mount.\\nIf omitted, the default is to mount by volume name.\\nExamples: For volume /dev/sda1, you specify the partition as \\\"1\\\".\\nSimilarly, the volume partition for /dev/sda is \\\"0\\\" (or you can leave the property empty).\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"ReadOnly here will force the ReadOnly setting in VolumeMounts.\\nDefaults to false.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a Persistent Disk resource in Google Compute Engine.\\n\\nA GCE PD must exist before mounting to a container. The disk must\\nalso be in the same GCE project and zone as the kubelet. A GCE PD\\ncan only be mounted as read/write once or read-only many times. GCE\\nPDs support ownership management and SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1GitRepoVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"repository\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Repository URL\"\n" +
		"        },\n" +
		"        \"revision\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Commit hash for the specified revision.\\n+optional\"\n" +
		"        },\n" +
		"        \"directory\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Target directory name.\\nMust not contain or start with '..'.  If '.' is supplied, the volume directory will be the\\ngit repository.  Otherwise, if specified, the volume will contain the git repository in\\nthe subdirectory with the given name.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a volume that is populated with the contents of a git repository.\\nGit repo volumes do not support ownership management.\\nGit repo volumes support SELinux relabeling.\\n\\nDEPRECATED: GitRepo is deprecated. To provision a container with a git repo, mount an\\nEmptyDir into an InitContainer that clones the repo using git, then mount the EmptyDir\\ninto the Pod's container.\"\n" +
		"    },\n" +
		"    \"v1GlusterfsVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"endpoints\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"EndpointsName is the endpoint name that details Glusterfs topology.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/glusterfs/README.md#create-a-pod\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Path is the Glusterfs volume path.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/glusterfs/README.md#create-a-pod\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"ReadOnly here will force the Glusterfs volume to be mounted with read-only permissions.\\nDefaults to false.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/glusterfs/README.md#create-a-pod\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a Glusterfs mount that lasts the lifetime of a pod.\\nGlusterfs volumes do not support ownership management or SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1HTTPGetAction\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"path\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Path to access on the HTTP server.\\n+optional\"\n" +
		"        },\n" +
		"        \"port\": {\n" +
		"          \"$ref\": \"#/definitions/intstrIntOrString\",\n" +
		"          \"description\": \"Name or number of the port to access on the container.\\nNumber must be in the range 1 to 65535.\\nName must be an IANA_SVC_NAME.\"\n" +
		"        },\n" +
		"        \"host\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Host name to connect to, defaults to the pod IP. You probably want to set\\n\\\"Host\\\" in httpHeaders instead.\\n+optional\"\n" +
		"        },\n" +
		"        \"scheme\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Scheme to use for connecting to the host.\\nDefaults to HTTP.\\n+optional\"\n" +
		"        },\n" +
		"        \"httpHeaders\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1HTTPHeader\"\n" +
		"          },\n" +
		"          \"title\": \"Custom headers to set in the request. HTTP allows repeated headers.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"HTTPGetAction describes an action based on HTTP Get requests.\"\n" +
		"    },\n" +
		"    \"v1HTTPHeader\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The header field name\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The header field value\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"HTTPHeader describes a custom header to be used in HTTP probes\"\n" +
		"    },\n" +
		"    \"v1Handler\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"exec\": {\n" +
		"          \"$ref\": \"#/definitions/v1ExecAction\",\n" +
		"          \"title\": \"One and only one of the following should be specified.\\nExec specifies the action to take.\\n+optional\"\n" +
		"        },\n" +
		"        \"httpGet\": {\n" +
		"          \"$ref\": \"#/definitions/v1HTTPGetAction\",\n" +
		"          \"title\": \"HTTPGet specifies the http request to perform.\\n+optional\"\n" +
		"        },\n" +
		"        \"tcpSocket\": {\n" +
		"          \"$ref\": \"#/definitions/v1TCPSocketAction\",\n" +
		"          \"title\": \"TCPSocket specifies an action involving a TCP port.\\nTCP hooks not yet supported\\nTODO: implement a realistic TCP lifecycle hook\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Handler defines a specific action that should be taken\\nTODO: pass structured data to these actions, and document that data here.\"\n" +
		"    },\n" +
		"    \"v1HostAlias\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"ip\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"IP address of the host file entry.\"\n" +
		"        },\n" +
		"        \"hostnames\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"description\": \"Hostnames for the above IP address.\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"HostAlias holds the mapping between IP and hostnames that will be injected as an entry in the\\npod's hosts file.\"\n" +
		"    },\n" +
		"    \"v1HostPathVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"path\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Path of the directory on the host.\\nIf the path is a symlink, it will follow the link to the real path.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#hostpath\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Type for HostPath Volume\\nDefaults to \\\"\\\"\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#hostpath\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a host path mapped into a pod.\\nHost path volumes do not support ownership management or SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1ISCSIVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"targetPortal\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"iSCSI Target Portal. The Portal is either an IP or ip_addr:port if the port\\nis other than default (typically TCP ports 860 and 3260).\"\n" +
		"        },\n" +
		"        \"iqn\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Target iSCSI Qualified Name.\"\n" +
		"        },\n" +
		"        \"lun\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"description\": \"iSCSI Target Lun number.\"\n" +
		"        },\n" +
		"        \"iscsiInterface\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"iSCSI Interface Name that uses an iSCSI transport.\\nDefaults to 'default' (tcp).\\n+optional\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type of the volume that you want to mount.\\nTip: Ensure that the filesystem type is supported by the host operating system.\\nExamples: \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#iscsi\\nTODO: how do we prevent errors in the filesystem from compromising the machine\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"ReadOnly here will force the ReadOnly setting in VolumeMounts.\\nDefaults to false.\\n+optional\"\n" +
		"        },\n" +
		"        \"portals\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"iSCSI Target Portal List. The portal is either an IP or ip_addr:port if the port\\nis other than default (typically TCP ports 860 and 3260).\\n+optional\"\n" +
		"        },\n" +
		"        \"chapAuthDiscovery\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"whether support iSCSI Discovery CHAP authentication\\n+optional\"\n" +
		"        },\n" +
		"        \"chapAuthSession\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"whether support iSCSI Session CHAP authentication\\n+optional\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"title\": \"CHAP Secret for iSCSI target and initiator authentication\\n+optional\"\n" +
		"        },\n" +
		"        \"initiatorName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Custom iSCSI Initiator Name.\\nIf initiatorName is specified with iscsiInterface simultaneously, new iSCSI interface\\n<target portal>:<volume name> will be created for the connection.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents an ISCSI disk.\\nISCSI volumes can only be mounted as read/write once.\\nISCSI volumes support ownership management and SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1KeyToPath\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"key\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"The key to project.\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"The relative path of the file to map the key to.\\nMay not be an absolute path.\\nMay not contain the path element '..'.\\nMay not start with the string '..'.\"\n" +
		"        },\n" +
		"        \"mode\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Optional: mode bits to use on this file, must be a value between 0\\nand 0777. If not specified, the volume defaultMode will be used.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Maps a string key to a path within a volume.\"\n" +
		"    },\n" +
		"    \"v1LabelSelector\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"matchLabels\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"matchLabels is a map of {key,value} pairs. A single {key,value} in the matchLabels\\nmap is equivalent to an element of matchExpressions, whose key field is \\\"key\\\", the\\noperator is \\\"In\\\", and the values array contains only \\\"value\\\". The requirements are ANDed.\\n+optional\"\n" +
		"        },\n" +
		"        \"matchExpressions\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1LabelSelectorRequirement\"\n" +
		"          },\n" +
		"          \"title\": \"matchExpressions is a list of label selector requirements. The requirements are ANDed.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"A label selector is a label query over a set of resources. The result of matchLabels and\\nmatchExpressions are ANDed. An empty label selector matches all objects. A null\\nlabel selector matches no objects.\"\n" +
		"    },\n" +
		"    \"v1LabelSelectorRequirement\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"key\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"key is the label key that the selector applies to.\\n+patchMergeKey=key\\n+patchStrategy=merge\"\n" +
		"        },\n" +
		"        \"operator\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"operator represents a key's relationship to a set of values.\\nValid operators are In, NotIn, Exists and DoesNotExist.\"\n" +
		"        },\n" +
		"        \"values\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"values is an array of string values. If the operator is In or NotIn,\\nthe values array must be non-empty. If the operator is Exists or DoesNotExist,\\nthe values array must be empty. This array is replaced during a strategic\\nmerge patch.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"A label selector requirement is a selector that contains values, a key, and an operator that\\nrelates the key and values.\"\n" +
		"    },\n" +
		"    \"v1Lifecycle\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"postStart\": {\n" +
		"          \"$ref\": \"#/definitions/v1Handler\",\n" +
		"          \"title\": \"PostStart is called immediately after a container is created. If the handler fails,\\nthe container is terminated and restarted according to its restart policy.\\nOther management of the container blocks until the hook completes.\\nMore info: https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks\\n+optional\"\n" +
		"        },\n" +
		"        \"preStop\": {\n" +
		"          \"$ref\": \"#/definitions/v1Handler\",\n" +
		"          \"title\": \"PreStop is called immediately before a container is terminated due to an\\nAPI request or management event such as liveness probe failure,\\npreemption, resource contention, etc. The handler is not called if the\\ncontainer crashes or exits. The reason for termination is passed to the\\nhandler. The Pod's termination grace period countdown begins before the\\nPreStop hooked is executed. Regardless of the outcome of the handler, the\\ncontainer will eventually terminate within the Pod's termination grace\\nperiod. Other management of the container blocks until the hook completes\\nor until the termination grace period is reached.\\nMore info: https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Lifecycle describes actions that the management system should take in response to container lifecycle\\nevents. For the PostStart and PreStop lifecycle handlers, management of the container blocks\\nuntil the action is complete, unless the container process fails, in which case the handler is aborted.\"\n" +
		"    },\n" +
		"    \"v1LocalObjectReference\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Name of the referent.\\nMore info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names\\nTODO: Add other useful fields. apiVersion, kind, uid?\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"LocalObjectReference contains enough information to let you locate the\\nreferenced object inside the same namespace.\"\n" +
		"    },\n" +
		"    \"v1NFSVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"server\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Server is the hostname or IP address of the NFS server.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#nfs\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Path that is exported by the NFS server.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#nfs\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"ReadOnly here will force\\nthe NFS export to be mounted with read-only permissions.\\nDefaults to false.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#nfs\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents an NFS mount that lasts the lifetime of a pod.\\nNFS volumes do not support ownership management or SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1NodeAffinity\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"requiredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"$ref\": \"#/definitions/v1NodeSelector\",\n" +
		"          \"title\": \"If the affinity requirements specified by this field are not met at\\nscheduling time, the pod will not be scheduled onto the node.\\nIf the affinity requirements specified by this field cease to be met\\nat some point during pod execution (e.g. due to an update), the system\\nmay or may not try to eventually evict the pod from its node.\\n+optional\"\n" +
		"        },\n" +
		"        \"preferredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PreferredSchedulingTerm\"\n" +
		"          },\n" +
		"          \"title\": \"The scheduler will prefer to schedule pods to nodes that satisfy\\nthe affinity expressions specified by this field, but it may choose\\na node that violates one or more of the expressions. The node that is\\nmost preferred is the one with the greatest sum of weights, i.e.\\nfor each node that meets all of the scheduling requirements (resource\\nrequest, requiredDuringScheduling affinity expressions, etc.),\\ncompute a sum by iterating through the elements of this field and adding\\n\\\"weight\\\" to the sum if the node matches the corresponding matchExpressions; the\\nnode(s) with the highest sum are the most preferred.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Node affinity is a group of node affinity scheduling rules.\"\n" +
		"    },\n" +
		"    \"v1NodeSelector\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"nodeSelectorTerms\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1NodeSelectorTerm\"\n" +
		"          },\n" +
		"          \"description\": \"Required. A list of node selector terms. The terms are ORed.\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"A node selector represents the union of the results of one or more label queries\\nover a set of nodes; that is, it represents the OR of the selectors represented\\nby the node selector terms.\"\n" +
		"    },\n" +
		"    \"v1NodeSelectorRequirement\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"key\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"The label key that the selector applies to.\"\n" +
		"        },\n" +
		"        \"operator\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Represents a key's relationship to a set of values.\\nValid operators are In, NotIn, Exists, DoesNotExist. Gt, and Lt.\"\n" +
		"        },\n" +
		"        \"values\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"An array of string values. If the operator is In or NotIn,\\nthe values array must be non-empty. If the operator is Exists or DoesNotExist,\\nthe values array must be empty. If the operator is Gt or Lt, the values\\narray must have a single element, which will be interpreted as an integer.\\nThis array is replaced during a strategic merge patch.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"A node selector requirement is a selector that contains values, a key, and an operator\\nthat relates the key and values.\"\n" +
		"    },\n" +
		"    \"v1NodeSelectorTerm\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"matchExpressions\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1NodeSelectorRequirement\"\n" +
		"          },\n" +
		"          \"title\": \"A list of node selector requirements by node's labels.\\n+optional\"\n" +
		"        },\n" +
		"        \"matchFields\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1NodeSelectorRequirement\"\n" +
		"          },\n" +
		"          \"title\": \"A list of node selector requirements by node's fields.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"A null or empty node selector term matches no objects. The requirements of\\nthem are ANDed.\\nThe TopologySelectorTerm type implements a subset of the NodeSelectorTerm.\"\n" +
		"    },\n" +
		"    \"v1ObjectFieldSelector\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"apiVersion\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Version of the schema the FieldPath is written in terms of, defaults to \\\"v1\\\".\\n+optional\"\n" +
		"        },\n" +
		"        \"fieldPath\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Path of the field to select in the specified API version.\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"ObjectFieldSelector selects an APIVersioned field of an object.\"\n" +
		"    },\n" +
		"    \"v1PersistentVolumeClaimVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"claimName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"ClaimName is the name of a PersistentVolumeClaim in the same namespace as the pod using this volume.\\nMore info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Will force the ReadOnly setting in VolumeMounts.\\nDefault false.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"PersistentVolumeClaimVolumeSource references the user's PVC in the same namespace.\\nThis volume finds the bound PV and mounts that volume for the pod. A\\nPersistentVolumeClaimVolumeSource is, essentially, a wrapper around another\\ntype of volume that is owned by someone else (the system).\"\n" +
		"    },\n" +
		"    \"v1PhotonPersistentDiskVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"pdID\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"ID that identifies Photon Controller persistent disk\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a Photon Controller persistent disk resource.\"\n" +
		"    },\n" +
		"    \"v1PodAffinity\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"requiredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PodAffinityTerm\"\n" +
		"          },\n" +
		"          \"title\": \"If the affinity requirements specified by this field are not met at\\nscheduling time, the pod will not be scheduled onto the node.\\nIf the affinity requirements specified by this field cease to be met\\nat some point during pod execution (e.g. due to a pod label update), the\\nsystem may or may not try to eventually evict the pod from its node.\\nWhen there are multiple elements, the lists of nodes corresponding to each\\npodAffinityTerm are intersected, i.e. all terms must be satisfied.\\n+optional\"\n" +
		"        },\n" +
		"        \"preferredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1WeightedPodAffinityTerm\"\n" +
		"          },\n" +
		"          \"title\": \"The scheduler will prefer to schedule pods to nodes that satisfy\\nthe affinity expressions specified by this field, but it may choose\\na node that violates one or more of the expressions. The node that is\\nmost preferred is the one with the greatest sum of weights, i.e.\\nfor each node that meets all of the scheduling requirements (resource\\nrequest, requiredDuringScheduling affinity expressions, etc.),\\ncompute a sum by iterating through the elements of this field and adding\\n\\\"weight\\\" to the sum if the node has pods which matches the corresponding podAffinityTerm; the\\nnode(s) with the highest sum are the most preferred.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Pod affinity is a group of inter pod affinity scheduling rules.\"\n" +
		"    },\n" +
		"    \"v1PodAffinityTerm\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"labelSelector\": {\n" +
		"          \"$ref\": \"#/definitions/v1LabelSelector\",\n" +
		"          \"title\": \"A label query over a set of resources, in this case pods.\\n+optional\"\n" +
		"        },\n" +
		"        \"namespaces\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"namespaces specifies which namespaces the labelSelector applies to (matches against);\\nnull or empty list means \\\"this pod's namespace\\\"\\n+optional\"\n" +
		"        },\n" +
		"        \"topologyKey\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"This pod should be co-located (affinity) or not co-located (anti-affinity) with the pods matching\\nthe labelSelector in the specified namespaces, where co-located is defined as running on a node\\nwhose value of the label with key topologyKey matches that of any node on which any of the\\nselected pods is running.\\nEmpty topologyKey is not allowed.\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"Defines a set of pods (namely those matching the labelSelector\\nrelative to the given namespace(s)) that this pod should be\\nco-located (affinity) or not co-located (anti-affinity) with,\\nwhere co-located is defined as running on a node whose value of\\nthe label with key <topologyKey> matches that of any node on which\\na pod of the set of pods is running\"\n" +
		"    },\n" +
		"    \"v1PodAntiAffinity\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"requiredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PodAffinityTerm\"\n" +
		"          },\n" +
		"          \"title\": \"If the anti-affinity requirements specified by this field are not met at\\nscheduling time, the pod will not be scheduled onto the node.\\nIf the anti-affinity requirements specified by this field cease to be met\\nat some point during pod execution (e.g. due to a pod label update), the\\nsystem may or may not try to eventually evict the pod from its node.\\nWhen there are multiple elements, the lists of nodes corresponding to each\\npodAffinityTerm are intersected, i.e. all terms must be satisfied.\\n+optional\"\n" +
		"        },\n" +
		"        \"preferredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1WeightedPodAffinityTerm\"\n" +
		"          },\n" +
		"          \"title\": \"The scheduler will prefer to schedule pods to nodes that satisfy\\nthe anti-affinity expressions specified by this field, but it may choose\\na node that violates one or more of the expressions. The node that is\\nmost preferred is the one with the greatest sum of weights, i.e.\\nfor each node that meets all of the scheduling requirements (resource\\nrequest, requiredDuringScheduling anti-affinity expressions, etc.),\\ncompute a sum by iterating through the elements of this field and adding\\n\\\"weight\\\" to the sum if the node has pods which matches the corresponding podAffinityTerm; the\\nnode(s) with the highest sum are the most preferred.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Pod anti affinity is a group of inter pod anti affinity scheduling rules.\"\n" +
		"    },\n" +
		"    \"v1PodDNSConfig\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"nameservers\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"A list of DNS name server IP addresses.\\nThis will be appended to the base nameservers generated from DNSPolicy.\\nDuplicated nameservers will be removed.\\n+optional\"\n" +
		"        },\n" +
		"        \"searches\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"A list of DNS search domains for host-name lookup.\\nThis will be appended to the base search paths generated from DNSPolicy.\\nDuplicated search paths will be removed.\\n+optional\"\n" +
		"        },\n" +
		"        \"options\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PodDNSConfigOption\"\n" +
		"          },\n" +
		"          \"title\": \"A list of DNS resolver options.\\nThis will be merged with the base options generated from DNSPolicy.\\nDuplicated entries will be removed. Resolution options given in Options\\nwill override those that appear in the base DNSPolicy.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"PodDNSConfig defines the DNS parameters of a pod in addition to\\nthose generated from DNSPolicy.\"\n" +
		"    },\n" +
		"    \"v1PodDNSConfigOption\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Required.\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"PodDNSConfigOption defines DNS resolver options of a pod.\"\n" +
		"    },\n" +
		"    \"v1PodReadinessGate\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"conditionType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"ConditionType refers to a condition in the pod's condition list with matching type.\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"PodReadinessGate contains the reference to a pod condition\"\n" +
		"    },\n" +
		"    \"v1PodSecurityContext\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"seLinuxOptions\": {\n" +
		"          \"$ref\": \"#/definitions/v1SELinuxOptions\",\n" +
		"          \"title\": \"The SELinux context to be applied to all containers.\\nIf unspecified, the container runtime will allocate a random SELinux context for each\\ncontainer.  May also be set in SecurityContext.  If set in\\nboth SecurityContext and PodSecurityContext, the value specified in SecurityContext\\ntakes precedence for that container.\\n+optional\"\n" +
		"        },\n" +
		"        \"windowsOptions\": {\n" +
		"          \"$ref\": \"#/definitions/v1WindowsSecurityContextOptions\",\n" +
		"          \"title\": \"Windows security options.\\n+optional\"\n" +
		"        },\n" +
		"        \"runAsUser\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"title\": \"The UID to run the entrypoint of the container process.\\nDefaults to user specified in image metadata if unspecified.\\nMay also be set in SecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence\\nfor that container.\\n+optional\"\n" +
		"        },\n" +
		"        \"runAsGroup\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"title\": \"The GID to run the entrypoint of the container process.\\nUses runtime default if unset.\\nMay also be set in SecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence\\nfor that container.\\n+optional\"\n" +
		"        },\n" +
		"        \"runAsNonRoot\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Indicates that the container must run as a non-root user.\\nIf true, the Kubelet will validate the image at runtime to ensure that it\\ndoes not run as UID 0 (root) and fail to start the container if it does.\\nIf unset or false, no such validation will be performed.\\nMay also be set in SecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence.\\n+optional\"\n" +
		"        },\n" +
		"        \"supplementalGroups\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\",\n" +
		"            \"format\": \"int64\"\n" +
		"          },\n" +
		"          \"title\": \"A list of groups applied to the first process run in each container, in addition\\nto the container's primary GID.  If unspecified, no groups will be added to\\nany container.\\n+optional\"\n" +
		"        },\n" +
		"        \"fsGroup\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"description\": \"1. The owning GID will be the FSGroup\\n2. The setgid bit is set (new files created in the volume will be owned by FSGroup)\\n3. The permission bits are OR'd with rw-rw----\\n\\nIf unset, the Kubelet will not modify the ownership and permissions of any volume.\\n+optional\",\n" +
		"          \"title\": \"A special supplemental group that applies to all containers in a pod.\\nSome volume types allow the Kubelet to change the ownership of that volume\\nto be owned by the pod:\"\n" +
		"        },\n" +
		"        \"sysctls\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Sysctl\"\n" +
		"          },\n" +
		"          \"title\": \"Sysctls hold a list of namespaced sysctls used for the pod. Pods with unsupported\\nsysctls (by the container runtime) might fail to launch.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"PodSecurityContext holds pod-level security attributes and common container settings.\\nSome fields are also present in container.securityContext.  Field values of\\ncontainer.securityContext take precedence over field values of PodSecurityContext.\"\n" +
		"    },\n" +
		"    \"v1PodSpec\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"volumes\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Volume\"\n" +
		"          },\n" +
		"          \"title\": \"List of volumes that can be mounted by containers belonging to the pod.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes\\n+optional\\n+patchMergeKey=name\\n+patchStrategy=merge,retainKeys\"\n" +
		"        },\n" +
		"        \"initContainers\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Container\"\n" +
		"          },\n" +
		"          \"title\": \"List of initialization containers belonging to the pod.\\nInit containers are executed in order prior to containers being started. If any\\ninit container fails, the pod is considered to have failed and is handled according\\nto its restartPolicy. The name for an init container or normal container must be\\nunique among all containers.\\nInit containers may not have Lifecycle actions, Readiness probes, or Liveness probes.\\nThe resourceRequirements of an init container are taken into account during scheduling\\nby finding the highest request/limit for each resource type, and then using the max of\\nof that value or the sum of the normal containers. Limits are applied to init containers\\nin a similar fashion.\\nInit containers cannot currently be added or removed.\\nCannot be updated.\\nMore info: https://kubernetes.io/docs/concepts/workloads/pods/init-containers/\\n+patchMergeKey=name\\n+patchStrategy=merge\"\n" +
		"        },\n" +
		"        \"containers\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Container\"\n" +
		"          },\n" +
		"          \"title\": \"List of containers belonging to the pod.\\nContainers cannot currently be added or removed.\\nThere must be at least one container in a Pod.\\nCannot be updated.\\n+patchMergeKey=name\\n+patchStrategy=merge\"\n" +
		"        },\n" +
		"        \"restartPolicy\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Restart policy for all containers within the pod.\\nOne of Always, OnFailure, Never.\\nDefault to Always.\\nMore info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy\\n+optional\"\n" +
		"        },\n" +
		"        \"terminationGracePeriodSeconds\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"title\": \"Optional duration in seconds the pod needs to terminate gracefully. May be decreased in delete request.\\nValue must be non-negative integer. The value zero indicates delete immediately.\\nIf this value is nil, the default grace period will be used instead.\\nThe grace period is the duration in seconds after the processes running in the pod are sent\\na termination signal and the time when the processes are forcibly halted with a kill signal.\\nSet this value longer than the expected cleanup time for your process.\\nDefaults to 30 seconds.\\n+optional\"\n" +
		"        },\n" +
		"        \"activeDeadlineSeconds\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"title\": \"Optional duration in seconds the pod may be active on the node relative to\\nStartTime before the system will actively try to mark it failed and kill associated containers.\\nValue must be a positive integer.\\n+optional\"\n" +
		"        },\n" +
		"        \"dnsPolicy\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Set DNS policy for the pod.\\nDefaults to \\\"ClusterFirst\\\".\\nValid values are 'ClusterFirstWithHostNet', 'ClusterFirst', 'Default' or 'None'.\\nDNS parameters given in DNSConfig will be merged with the policy selected with DNSPolicy.\\nTo have DNS options set along with hostNetwork, you have to specify DNS policy\\nexplicitly to 'ClusterFirstWithHostNet'.\\n+optional\"\n" +
		"        },\n" +
		"        \"nodeSelector\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"NodeSelector is a selector which must be true for the pod to fit on a node.\\nSelector which must match a node's labels for the pod to be scheduled on that node.\\nMore info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/\\n+optional\"\n" +
		"        },\n" +
		"        \"serviceAccountName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"ServiceAccountName is the name of the ServiceAccount to use to run this pod.\\nMore info: https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/\\n+optional\"\n" +
		"        },\n" +
		"        \"serviceAccount\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"DeprecatedServiceAccount is a depreciated alias for ServiceAccountName.\\nDeprecated: Use serviceAccountName instead.\\n+k8s:conversion-gen=false\\n+optional\"\n" +
		"        },\n" +
		"        \"automountServiceAccountToken\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"AutomountServiceAccountToken indicates whether a service account token should be automatically mounted.\\n+optional\"\n" +
		"        },\n" +
		"        \"nodeName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"NodeName is a request to schedule this pod onto a specific node. If it is non-empty,\\nthe scheduler simply schedules this pod onto that node, assuming that it fits resource\\nrequirements.\\n+optional\"\n" +
		"        },\n" +
		"        \"hostNetwork\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Host networking requested for this pod. Use the host's network namespace.\\nIf this option is set, the ports that will be used must be specified.\\nDefault to false.\\n+k8s:conversion-gen=false\\n+optional\"\n" +
		"        },\n" +
		"        \"hostPID\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Use the host's pid namespace.\\nOptional: Default to false.\\n+k8s:conversion-gen=false\\n+optional\"\n" +
		"        },\n" +
		"        \"hostIPC\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Use the host's ipc namespace.\\nOptional: Default to false.\\n+k8s:conversion-gen=false\\n+optional\"\n" +
		"        },\n" +
		"        \"shareProcessNamespace\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Share a single process namespace between all of the containers in a pod.\\nWhen this is set containers will be able to view and signal processes from other containers\\nin the same pod, and the first process in each container will not be assigned PID 1.\\nHostPID and ShareProcessNamespace cannot both be set.\\nOptional: Default to false.\\nThis field is beta-level and may be disabled with the PodShareProcessNamespace feature.\\n+k8s:conversion-gen=false\\n+optional\"\n" +
		"        },\n" +
		"        \"securityContext\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodSecurityContext\",\n" +
		"          \"title\": \"SecurityContext holds pod-level security attributes and common container settings.\\nOptional: Defaults to empty.  See type description for default values of each field.\\n+optional\"\n" +
		"        },\n" +
		"        \"imagePullSecrets\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"          },\n" +
		"          \"title\": \"ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec.\\nIf specified, these secrets will be passed to individual puller implementations for them to use. For example,\\nin the case of docker, only DockerConfig type secrets are honored.\\nMore info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod\\n+optional\\n+patchMergeKey=name\\n+patchStrategy=merge\"\n" +
		"        },\n" +
		"        \"hostname\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Specifies the hostname of the Pod\\nIf not specified, the pod's hostname will be set to a system-defined value.\\n+optional\"\n" +
		"        },\n" +
		"        \"subdomain\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"If specified, the fully qualified Pod hostname will be \\\"<hostname>.<subdomain>.<pod namespace>.svc.<cluster domain>\\\".\\nIf not specified, the pod will not have a domainname at all.\\n+optional\"\n" +
		"        },\n" +
		"        \"affinity\": {\n" +
		"          \"$ref\": \"#/definitions/v1Affinity\",\n" +
		"          \"title\": \"If specified, the pod's scheduling constraints\\n+optional\"\n" +
		"        },\n" +
		"        \"schedulerName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"If specified, the pod will be dispatched by specified scheduler.\\nIf not specified, the pod will be dispatched by default scheduler.\\n+optional\"\n" +
		"        },\n" +
		"        \"tolerations\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Toleration\"\n" +
		"          },\n" +
		"          \"title\": \"If specified, the pod's tolerations.\\n+optional\"\n" +
		"        },\n" +
		"        \"hostAliases\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1HostAlias\"\n" +
		"          },\n" +
		"          \"title\": \"HostAliases is an optional list of hosts and IPs that will be injected into the pod's hosts\\nfile if specified. This is only valid for non-hostNetwork pods.\\n+optional\\n+patchMergeKey=ip\\n+patchStrategy=merge\"\n" +
		"        },\n" +
		"        \"priorityClassName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"If specified, indicates the pod's priority. \\\"system-node-critical\\\" and\\n\\\"system-cluster-critical\\\" are two special keywords which indicate the\\nhighest priorities with the former being the highest priority. Any other\\nname must be defined by creating a PriorityClass object with that name.\\nIf not specified, the pod priority will be default or zero if there is no\\ndefault.\\n+optional\"\n" +
		"        },\n" +
		"        \"priority\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"The priority value. Various system components use this field to find the\\npriority of the pod. When Priority Admission Controller is enabled, it\\nprevents users from setting this field. The admission controller populates\\nthis field from PriorityClassName.\\nThe higher the value, the higher the priority.\\n+optional\"\n" +
		"        },\n" +
		"        \"dnsConfig\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodDNSConfig\",\n" +
		"          \"title\": \"Specifies the DNS parameters of a pod.\\nParameters specified here will be merged to the generated DNS\\nconfiguration based on DNSPolicy.\\n+optional\"\n" +
		"        },\n" +
		"        \"readinessGates\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PodReadinessGate\"\n" +
		"          },\n" +
		"          \"title\": \"If specified, all readiness gates will be evaluated for pod readiness.\\nA pod is ready when all its containers are ready AND\\nall conditions specified in the readiness gates have status equal to \\\"True\\\"\\nMore info: https://git.k8s.io/enhancements/keps/sig-network/0007-pod-ready%2B%2B.md\\n+optional\"\n" +
		"        },\n" +
		"        \"runtimeClassName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"RuntimeClassName refers to a RuntimeClass object in the node.k8s.io group, which should be used\\nto run this pod.  If no RuntimeClass resource matches the named class, the pod will not be run.\\nIf unset or empty, the \\\"legacy\\\" RuntimeClass will be used, which is an implicit class with an\\nempty definition that uses the default runtime handler.\\nMore info: https://git.k8s.io/enhancements/keps/sig-node/runtime-class.md\\nThis is a beta feature as of Kubernetes v1.14.\\n+optional\"\n" +
		"        },\n" +
		"        \"enableServiceLinks\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"EnableServiceLinks indicates whether information about services should be injected into pod's\\nenvironment variables, matching the syntax of Docker links.\\nOptional: Defaults to true.\\n+optional\"\n" +
		"        },\n" +
		"        \"preemptionPolicy\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"PreemptionPolicy is the Policy for preempting pods with lower priority.\\nOne of Never, PreemptLowerPriority.\\nDefaults to PreemptLowerPriority if unset.\\nThis field is alpha-level and is only honored by servers that enable the NonPreemptingPriority feature.\\n+optional\"\n" +
		"        },\n" +
		"        \"overhead\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"$ref\": \"#/definitions/resourceQuantity\"\n" +
		"          },\n" +
		"          \"title\": \"Overhead represents the resource overhead associated with running a pod for a given RuntimeClass.\\nThis field will be autopopulated at admission time by the RuntimeClass admission controller. If\\nthe RuntimeClass admission controller is enabled, overhead must not be set in Pod create requests.\\nThe RuntimeClass admission controller will reject Pod create requests which have the overhead already\\nset. If RuntimeClass is configured and selected in the PodSpec, Overhead will be set to the value\\ndefined in the corresponding RuntimeClass, otherwise it will remain unset and treated as zero.\\nMore info: https://git.k8s.io/enhancements/keps/sig-node/20190226-pod-overhead.md\\nThis field is alpha-level as of Kubernetes v1.16, and is only honored by servers that enable the PodOverhead feature.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"PodSpec is a description of a pod.\"\n" +
		"    },\n" +
		"    \"v1PortworxVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"volumeID\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"VolumeID uniquely identifies a Portworx volume\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"FSType represents the filesystem type to mount\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"PortworxVolumeSource represents a Portworx volume resource.\"\n" +
		"    },\n" +
		"    \"v1PreferredSchedulingTerm\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"weight\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"description\": \"Weight associated with matching the corresponding nodeSelectorTerm, in the range 1-100.\"\n" +
		"        },\n" +
		"        \"preference\": {\n" +
		"          \"$ref\": \"#/definitions/v1NodeSelectorTerm\",\n" +
		"          \"description\": \"A node selector term, associated with the corresponding weight.\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"An empty preferred scheduling term matches all objects with implicit weight 0\\n(i.e. it's a no-op). A null preferred scheduling term matches no objects (i.e. is also a no-op).\"\n" +
		"    },\n" +
		"    \"v1Probe\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"handler\": {\n" +
		"          \"$ref\": \"#/definitions/v1Handler\",\n" +
		"          \"title\": \"The action taken to determine the health of a container\"\n" +
		"        },\n" +
		"        \"initialDelaySeconds\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Number of seconds after the container has started before liveness probes are initiated.\\nMore info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes\\n+optional\"\n" +
		"        },\n" +
		"        \"timeoutSeconds\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Number of seconds after which the probe times out.\\nDefaults to 1 second. Minimum value is 1.\\nMore info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes\\n+optional\"\n" +
		"        },\n" +
		"        \"periodSeconds\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"How often (in seconds) to perform the probe.\\nDefault to 10 seconds. Minimum value is 1.\\n+optional\"\n" +
		"        },\n" +
		"        \"successThreshold\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Minimum consecutive successes for the probe to be considered successful after having failed.\\nDefaults to 1. Must be 1 for liveness. Minimum value is 1.\\n+optional\"\n" +
		"        },\n" +
		"        \"failureThreshold\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Minimum consecutive failures for the probe to be considered failed after having succeeded.\\nDefaults to 3. Minimum value is 1.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Probe describes a health check to be performed against a container to determine whether it is\\nalive or ready to receive traffic.\"\n" +
		"    },\n" +
		"    \"v1ProjectedVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"sources\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1VolumeProjection\"\n" +
		"          },\n" +
		"          \"title\": \"list of volume projections\"\n" +
		"        },\n" +
		"        \"defaultMode\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Mode bits to use on created files by default. Must be a value between\\n0 and 0777.\\nDirectories within the path are not affected by this setting.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"Represents a projected volume source\"\n" +
		"    },\n" +
		"    \"v1QuobyteVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"registry\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Registry represents a single or multiple Quobyte Registry services\\nspecified as a string as host:port pair (multiple entries are separated with commas)\\nwhich acts as the central registry for volumes\"\n" +
		"        },\n" +
		"        \"volume\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Volume is a string that references an already created Quobyte volume by name.\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"ReadOnly here will force the Quobyte volume to be mounted with read-only permissions.\\nDefaults to false.\\n+optional\"\n" +
		"        },\n" +
		"        \"user\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"User to map volume access to\\nDefaults to serivceaccount user\\n+optional\"\n" +
		"        },\n" +
		"        \"group\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Group to map volume access to\\nDefault is no group\\n+optional\"\n" +
		"        },\n" +
		"        \"tenant\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Tenant owning the given Quobyte volume in the Backend\\nUsed with dynamically provisioned Quobyte volumes, value is set by the plugin\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a Quobyte mount that lasts the lifetime of a pod.\\nQuobyte volumes do not support ownership management or SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1RBDVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"monitors\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"title\": \"A collection of Ceph monitors.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/rbd/README.md#how-to-use-it\"\n" +
		"        },\n" +
		"        \"image\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The rados image name.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/rbd/README.md#how-to-use-it\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type of the volume that you want to mount.\\nTip: Ensure that the filesystem type is supported by the host operating system.\\nExamples: \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#rbd\\nTODO: how do we prevent errors in the filesystem from compromising the machine\\n+optional\"\n" +
		"        },\n" +
		"        \"pool\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The rados pool name.\\nDefault is rbd.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/rbd/README.md#how-to-use-it\\n+optional\"\n" +
		"        },\n" +
		"        \"user\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The rados user name.\\nDefault is admin.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/rbd/README.md#how-to-use-it\\n+optional\"\n" +
		"        },\n" +
		"        \"keyring\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Keyring is the path to key ring for RBDUser.\\nDefault is /etc/ceph/keyring.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/rbd/README.md#how-to-use-it\\n+optional\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"title\": \"SecretRef is name of the authentication secret for RBDUser. If provided\\noverrides keyring.\\nDefault is nil.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/rbd/README.md#how-to-use-it\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"ReadOnly here will force the ReadOnly setting in VolumeMounts.\\nDefaults to false.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/rbd/README.md#how-to-use-it\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a Rados Block Device mount that lasts the lifetime of a pod.\\nRBD volumes support ownership management and SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1ResourceFieldSelector\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"containerName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Container name: required for volumes, optional for env vars\\n+optional\"\n" +
		"        },\n" +
		"        \"resource\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Required: resource to select\"\n" +
		"        },\n" +
		"        \"divisor\": {\n" +
		"          \"$ref\": \"#/definitions/resourceQuantity\",\n" +
		"          \"title\": \"Specifies the output format of the exposed resources, defaults to \\\"1\\\"\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"ResourceFieldSelector represents container resources (cpu, memory) and their output format\"\n" +
		"    },\n" +
		"    \"v1ResourceRequirements\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"limits\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"$ref\": \"#/definitions/resourceQuantity\"\n" +
		"          },\n" +
		"          \"title\": \"Limits describes the maximum amount of compute resources allowed.\\nMore info: https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/\\n+optional\"\n" +
		"        },\n" +
		"        \"requests\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"$ref\": \"#/definitions/resourceQuantity\"\n" +
		"          },\n" +
		"          \"title\": \"Requests describes the minimum amount of compute resources required.\\nIf Requests is omitted for a container, it defaults to Limits if that is explicitly specified,\\notherwise to an implementation-defined value.\\nMore info: https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"ResourceRequirements describes the compute resource requirements.\"\n" +
		"    },\n" +
		"    \"v1SELinuxOptions\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"user\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"User is a SELinux user label that applies to the container.\\n+optional\"\n" +
		"        },\n" +
		"        \"role\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Role is a SELinux role label that applies to the container.\\n+optional\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Type is a SELinux type label that applies to the container.\\n+optional\"\n" +
		"        },\n" +
		"        \"level\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Level is SELinux level label that applies to the container.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"SELinuxOptions are the labels to be applied to the container\"\n" +
		"    },\n" +
		"    \"v1ScaleIOVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"gateway\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"The host address of the ScaleIO API Gateway.\"\n" +
		"        },\n" +
		"        \"system\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"The name of the storage system as configured in ScaleIO.\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"description\": \"SecretRef references to the secret for ScaleIO user and other\\nsensitive information. If this is not provided, Login operation will fail.\"\n" +
		"        },\n" +
		"        \"sslEnabled\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Flag to enable/disable SSL communication with Gateway, default false\\n+optional\"\n" +
		"        },\n" +
		"        \"protectionDomain\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The name of the ScaleIO Protection Domain for the configured storage.\\n+optional\"\n" +
		"        },\n" +
		"        \"storagePool\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The ScaleIO Storage Pool associated with the protection domain.\\n+optional\"\n" +
		"        },\n" +
		"        \"storageMode\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Indicates whether the storage for a volume should be ThickProvisioned or ThinProvisioned.\\nDefault is ThinProvisioned.\\n+optional\"\n" +
		"        },\n" +
		"        \"volumeName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"The name of a volume already created in the ScaleIO system\\nthat is associated with this volume source.\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\".\\nDefault is \\\"xfs\\\".\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"ScaleIOVolumeSource represents a persistent ScaleIO volume\"\n" +
		"    },\n" +
		"    \"v1SecretEnvSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"localObjectReference\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"description\": \"The Secret to select from.\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Specify whether the Secret must be defined\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"SecretEnvSource selects a Secret to populate the environment\\nvariables with.\\n\\nThe contents of the target Secret's Data field will represent the\\nkey-value pairs as environment variables.\"\n" +
		"    },\n" +
		"    \"v1SecretKeySelector\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"localObjectReference\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"description\": \"The name of the secret in the pod's namespace to select from.\"\n" +
		"        },\n" +
		"        \"key\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"The key of the secret to select from.  Must be a valid secret key.\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Specify whether the Secret or its key must be defined\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"SecretKeySelector selects a key of a Secret.\"\n" +
		"    },\n" +
		"    \"v1SecretProjection\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"localObjectReference\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"        },\n" +
		"        \"items\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1KeyToPath\"\n" +
		"          },\n" +
		"          \"title\": \"If unspecified, each key-value pair in the Data field of the referenced\\nSecret will be projected into the volume as a file whose name is the\\nkey and content is the value. If specified, the listed keys will be\\nprojected into the specified paths, and unlisted keys will not be\\npresent. If a key is specified which is not present in the Secret,\\nthe volume setup will error unless it is marked optional. Paths must be\\nrelative and may not contain the '..' path or start with '..'.\\n+optional\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Specify whether the Secret or its key must be defined\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Adapts a secret into a projected volume.\\n\\nThe contents of the target Secret's Data field will be presented in a\\nprojected volume as files using the keys in the Data field as the file names.\\nNote that this is identical to a secret volume source without the default\\nmode.\"\n" +
		"    },\n" +
		"    \"v1SecretVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"secretName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Name of the secret in the pod's namespace to use.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#secret\\n+optional\"\n" +
		"        },\n" +
		"        \"items\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1KeyToPath\"\n" +
		"          },\n" +
		"          \"title\": \"If unspecified, each key-value pair in the Data field of the referenced\\nSecret will be projected into the volume as a file whose name is the\\nkey and content is the value. If specified, the listed keys will be\\nprojected into the specified paths, and unlisted keys will not be\\npresent. If a key is specified which is not present in the Secret,\\nthe volume setup will error unless it is marked optional. Paths must be\\nrelative and may not contain the '..' path or start with '..'.\\n+optional\"\n" +
		"        },\n" +
		"        \"defaultMode\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Optional: mode bits to use on created files by default. Must be a\\nvalue between 0 and 0777. Defaults to 0644.\\nDirectories within the path are not affected by this setting.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Specify whether the Secret or its keys must be defined\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Adapts a Secret into a volume.\\n\\nThe contents of the target Secret's Data field will be presented in a volume\\nas files using the keys in the Data field as the file names.\\nSecret volumes support ownership management and SELinux relabeling.\"\n" +
		"    },\n" +
		"    \"v1SecurityContext\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"capabilities\": {\n" +
		"          \"$ref\": \"#/definitions/v1Capabilities\",\n" +
		"          \"title\": \"The capabilities to add/drop when running containers.\\nDefaults to the default set of capabilities granted by the container runtime.\\n+optional\"\n" +
		"        },\n" +
		"        \"privileged\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Run container in privileged mode.\\nProcesses in privileged containers are essentially equivalent to root on the host.\\nDefaults to false.\\n+optional\"\n" +
		"        },\n" +
		"        \"seLinuxOptions\": {\n" +
		"          \"$ref\": \"#/definitions/v1SELinuxOptions\",\n" +
		"          \"title\": \"The SELinux context to be applied to the container.\\nIf unspecified, the container runtime will allocate a random SELinux context for each\\ncontainer.  May also be set in PodSecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence.\\n+optional\"\n" +
		"        },\n" +
		"        \"windowsOptions\": {\n" +
		"          \"$ref\": \"#/definitions/v1WindowsSecurityContextOptions\",\n" +
		"          \"title\": \"Windows security options.\\n+optional\"\n" +
		"        },\n" +
		"        \"runAsUser\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"title\": \"The UID to run the entrypoint of the container process.\\nDefaults to user specified in image metadata if unspecified.\\nMay also be set in PodSecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence.\\n+optional\"\n" +
		"        },\n" +
		"        \"runAsGroup\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"title\": \"The GID to run the entrypoint of the container process.\\nUses runtime default if unset.\\nMay also be set in PodSecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence.\\n+optional\"\n" +
		"        },\n" +
		"        \"runAsNonRoot\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Indicates that the container must run as a non-root user.\\nIf true, the Kubelet will validate the image at runtime to ensure that it\\ndoes not run as UID 0 (root) and fail to start the container if it does.\\nIf unset or false, no such validation will be performed.\\nMay also be set in PodSecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence.\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnlyRootFilesystem\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Whether this container has a read-only root filesystem.\\nDefault is false.\\n+optional\"\n" +
		"        },\n" +
		"        \"allowPrivilegeEscalation\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"AllowPrivilegeEscalation controls whether a process can gain more\\nprivileges than its parent process. This bool directly controls if\\nthe no_new_privs flag will be set on the container process.\\nAllowPrivilegeEscalation is true always when the container is:\\n1) run as Privileged\\n2) has CAP_SYS_ADMIN\\n+optional\"\n" +
		"        },\n" +
		"        \"procMount\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"procMount denotes the type of proc mount to use for the containers.\\nThe default is DefaultProcMount which uses the container runtime defaults for\\nreadonly paths and masked paths.\\nThis requires the ProcMountType feature flag to be enabled.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"SecurityContext holds security configuration that will be applied to a container.\\nSome fields are present in both SecurityContext and PodSecurityContext.  When both\\nare set, the values in SecurityContext take precedence.\"\n" +
		"    },\n" +
		"    \"v1ServiceAccountTokenProjection\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"audience\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Audience is the intended audience of the token. A recipient of a token\\nmust identify itself with an identifier specified in the audience of the\\ntoken, and otherwise should reject the token. The audience defaults to the\\nidentifier of the apiserver.\\n+optional\"\n" +
		"        },\n" +
		"        \"expirationSeconds\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"title\": \"ExpirationSeconds is the requested duration of validity of the service\\naccount token. As the token approaches expiration, the kubelet volume\\nplugin will proactively rotate the service account token. The kubelet will\\nstart trying to rotate the token if the token is older than 80 percent of\\nits time to live or if the token is older than 24 hours.Defaults to 1 hour\\nand must be at least 10 minutes.\\n+optional\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Path is the path relative to the mount point of the file to project the\\ntoken into.\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"ServiceAccountTokenProjection represents a projected service account token\\nvolume. This projection can be used to insert a service account token into\\nthe pods runtime filesystem for use against APIs (Kubernetes API Server or\\notherwise).\"\n" +
		"    },\n" +
		"    \"v1StorageOSVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"volumeName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"VolumeName is the human-readable name of the StorageOS volume.  Volume\\nnames are only unique within a namespace.\"\n" +
		"        },\n" +
		"        \"volumeNamespace\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"VolumeNamespace specifies the scope of the volume within StorageOS.  If no\\nnamespace is specified then the Pod's namespace will be used.  This allows the\\nKubernetes name scoping to be mirrored within StorageOS for tighter integration.\\nSet VolumeName to any name to override the default behaviour.\\nSet to \\\"default\\\" if you are not using namespaces within StorageOS.\\nNamespaces that do not pre-exist within StorageOS will be created.\\n+optional\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\n+optional\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\",\n" +
		"          \"title\": \"SecretRef specifies the secret to use for obtaining the StorageOS API\\ncredentials.  If not specified, default values will be attempted.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a StorageOS persistent volume resource.\"\n" +
		"    },\n" +
		"    \"v1Sysctl\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Name of a property to set\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Value of a property to set\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"Sysctl defines a kernel parameter to be set\"\n" +
		"    },\n" +
		"    \"v1TCPSocketAction\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"port\": {\n" +
		"          \"$ref\": \"#/definitions/intstrIntOrString\",\n" +
		"          \"description\": \"Number or name of the port to access on the container.\\nNumber must be in the range 1 to 65535.\\nName must be an IANA_SVC_NAME.\"\n" +
		"        },\n" +
		"        \"host\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Optional: Host name to connect to, defaults to the pod IP.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"TCPSocketAction describes an action based on opening a socket\"\n" +
		"    },\n" +
		"    \"v1Toleration\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"key\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Key is the taint key that the toleration applies to. Empty means match all taint keys.\\nIf the key is empty, operator must be Exists; this combination means to match all values and all keys.\\n+optional\"\n" +
		"        },\n" +
		"        \"operator\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Operator represents a key's relationship to the value.\\nValid operators are Exists and Equal. Defaults to Equal.\\nExists is equivalent to wildcard for value, so that a pod can\\ntolerate all taints of a particular category.\\n+optional\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Value is the taint value the toleration matches to.\\nIf the operator is Exists, the value should be empty, otherwise just a regular string.\\n+optional\"\n" +
		"        },\n" +
		"        \"effect\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Effect indicates the taint effect to match. Empty means match all taint effects.\\nWhen specified, allowed values are NoSchedule, PreferNoSchedule and NoExecute.\\n+optional\"\n" +
		"        },\n" +
		"        \"tolerationSeconds\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"title\": \"TolerationSeconds represents the period of time the toleration (which must be\\nof effect NoExecute, otherwise this field is ignored) tolerates the taint. By default,\\nit is not set, which means tolerate the taint forever (do not evict). Zero and\\nnegative values will be treated as 0 (evict immediately) by the system.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"The pod this Toleration is attached to tolerates any taint that matches\\nthe triple <key,value,effect> using the matching operator <operator>.\"\n" +
		"    },\n" +
		"    \"v1Volume\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Volume's name.\\nMust be a DNS_LABEL and unique within the pod.\\nMore info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names\"\n" +
		"        },\n" +
		"        \"volumeSource\": {\n" +
		"          \"$ref\": \"#/definitions/v1VolumeSource\",\n" +
		"          \"description\": \"VolumeSource represents the location and type of the mounted volume.\\nIf not specified, the Volume is implied to be an EmptyDir.\\nThis implied behavior is deprecated and will be removed in a future version.\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Volume represents a named volume in a pod that may be accessed by any container in the pod.\"\n" +
		"    },\n" +
		"    \"v1VolumeDevice\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"name must match the name of a persistentVolumeClaim in the pod\"\n" +
		"        },\n" +
		"        \"devicePath\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"devicePath is the path inside of the container that the device will be mapped to.\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"volumeDevice describes a mapping of a raw block device within a container.\"\n" +
		"    },\n" +
		"    \"v1VolumeMount\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"This must match the Name of a Volume.\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"format\": \"boolean\",\n" +
		"          \"title\": \"Mounted read-only if true, read-write otherwise (false or unspecified).\\nDefaults to false.\\n+optional\"\n" +
		"        },\n" +
		"        \"mountPath\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"description\": \"Path within the container at which the volume should be mounted.  Must\\nnot contain ':'.\"\n" +
		"        },\n" +
		"        \"subPath\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Path within the volume from which the container's volume should be mounted.\\nDefaults to \\\"\\\" (volume's root).\\n+optional\"\n" +
		"        },\n" +
		"        \"mountPropagation\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"mountPropagation determines how mounts are propagated from the host\\nto container and the other way around.\\nWhen not set, MountPropagationNone is used.\\nThis field is beta in 1.10.\\n+optional\"\n" +
		"        },\n" +
		"        \"subPathExpr\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Expanded path within the volume from which the container's volume should be mounted.\\nBehaves similarly to SubPath but environment variable references $(VAR_NAME) are expanded using the container's environment.\\nDefaults to \\\"\\\" (volume's root).\\nSubPathExpr and SubPath are mutually exclusive.\\nThis field is beta in 1.15.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"VolumeMount describes a mounting of a Volume within a container.\"\n" +
		"    },\n" +
		"    \"v1VolumeProjection\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"secret\": {\n" +
		"          \"$ref\": \"#/definitions/v1SecretProjection\",\n" +
		"          \"title\": \"information about the secret data to project\\n+optional\"\n" +
		"        },\n" +
		"        \"downwardAPI\": {\n" +
		"          \"$ref\": \"#/definitions/v1DownwardAPIProjection\",\n" +
		"          \"title\": \"information about the downwardAPI data to project\\n+optional\"\n" +
		"        },\n" +
		"        \"configMap\": {\n" +
		"          \"$ref\": \"#/definitions/v1ConfigMapProjection\",\n" +
		"          \"title\": \"information about the configMap data to project\\n+optional\"\n" +
		"        },\n" +
		"        \"serviceAccountToken\": {\n" +
		"          \"$ref\": \"#/definitions/v1ServiceAccountTokenProjection\",\n" +
		"          \"title\": \"information about the serviceAccountToken data to project\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"Projection that may be projected along with other supported volume types\"\n" +
		"    },\n" +
		"    \"v1VolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"hostPath\": {\n" +
		"          \"$ref\": \"#/definitions/v1HostPathVolumeSource\",\n" +
		"          \"title\": \"HostPath represents a pre-existing file or directory on the host\\nmachine that is directly exposed to the container. This is generally\\nused for system agents or other privileged things that are allowed\\nto see the host machine. Most containers will NOT need this.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#hostpath\\n---\\nTODO(jonesdl) We need to restrict who can use host directory mounts and who can/can not\\nmount host directories as read/write.\\n+optional\"\n" +
		"        },\n" +
		"        \"emptyDir\": {\n" +
		"          \"$ref\": \"#/definitions/v1EmptyDirVolumeSource\",\n" +
		"          \"title\": \"EmptyDir represents a temporary directory that shares a pod's lifetime.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#emptydir\\n+optional\"\n" +
		"        },\n" +
		"        \"gcePersistentDisk\": {\n" +
		"          \"$ref\": \"#/definitions/v1GCEPersistentDiskVolumeSource\",\n" +
		"          \"title\": \"GCEPersistentDisk represents a GCE Disk resource that is attached to a\\nkubelet's host machine and then exposed to the pod.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk\\n+optional\"\n" +
		"        },\n" +
		"        \"awsElasticBlockStore\": {\n" +
		"          \"$ref\": \"#/definitions/v1AWSElasticBlockStoreVolumeSource\",\n" +
		"          \"title\": \"AWSElasticBlockStore represents an AWS Disk resource that is attached to a\\nkubelet's host machine and then exposed to the pod.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore\\n+optional\"\n" +
		"        },\n" +
		"        \"gitRepo\": {\n" +
		"          \"$ref\": \"#/definitions/v1GitRepoVolumeSource\",\n" +
		"          \"title\": \"GitRepo represents a git repository at a particular revision.\\nDEPRECATED: GitRepo is deprecated. To provision a container with a git repo, mount an\\nEmptyDir into an InitContainer that clones the repo using git, then mount the EmptyDir\\ninto the Pod's container.\\n+optional\"\n" +
		"        },\n" +
		"        \"secret\": {\n" +
		"          \"$ref\": \"#/definitions/v1SecretVolumeSource\",\n" +
		"          \"title\": \"Secret represents a secret that should populate this volume.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#secret\\n+optional\"\n" +
		"        },\n" +
		"        \"nfs\": {\n" +
		"          \"$ref\": \"#/definitions/v1NFSVolumeSource\",\n" +
		"          \"title\": \"NFS represents an NFS mount on the host that shares a pod's lifetime\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#nfs\\n+optional\"\n" +
		"        },\n" +
		"        \"iscsi\": {\n" +
		"          \"$ref\": \"#/definitions/v1ISCSIVolumeSource\",\n" +
		"          \"title\": \"ISCSI represents an ISCSI Disk resource that is attached to a\\nkubelet's host machine and then exposed to the pod.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/iscsi/README.md\\n+optional\"\n" +
		"        },\n" +
		"        \"glusterfs\": {\n" +
		"          \"$ref\": \"#/definitions/v1GlusterfsVolumeSource\",\n" +
		"          \"title\": \"Glusterfs represents a Glusterfs mount on the host that shares a pod's lifetime.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/glusterfs/README.md\\n+optional\"\n" +
		"        },\n" +
		"        \"persistentVolumeClaim\": {\n" +
		"          \"$ref\": \"#/definitions/v1PersistentVolumeClaimVolumeSource\",\n" +
		"          \"title\": \"PersistentVolumeClaimVolumeSource represents a reference to a\\nPersistentVolumeClaim in the same namespace.\\nMore info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims\\n+optional\"\n" +
		"        },\n" +
		"        \"rbd\": {\n" +
		"          \"$ref\": \"#/definitions/v1RBDVolumeSource\",\n" +
		"          \"title\": \"RBD represents a Rados Block Device mount on the host that shares a pod's lifetime.\\nMore info: https://releases.k8s.io/HEAD/examples/volumes/rbd/README.md\\n+optional\"\n" +
		"        },\n" +
		"        \"flexVolume\": {\n" +
		"          \"$ref\": \"#/definitions/v1FlexVolumeSource\",\n" +
		"          \"title\": \"FlexVolume represents a generic volume resource that is\\nprovisioned/attached using an exec based plugin.\\n+optional\"\n" +
		"        },\n" +
		"        \"cinder\": {\n" +
		"          \"$ref\": \"#/definitions/v1CinderVolumeSource\",\n" +
		"          \"title\": \"Cinder represents a cinder volume attached and mounted on kubelets host machine\\nMore info: https://releases.k8s.io/HEAD/examples/mysql-cinder-pd/README.md\\n+optional\"\n" +
		"        },\n" +
		"        \"cephfs\": {\n" +
		"          \"$ref\": \"#/definitions/v1CephFSVolumeSource\",\n" +
		"          \"title\": \"CephFS represents a Ceph FS mount on the host that shares a pod's lifetime\\n+optional\"\n" +
		"        },\n" +
		"        \"flocker\": {\n" +
		"          \"$ref\": \"#/definitions/v1FlockerVolumeSource\",\n" +
		"          \"title\": \"Flocker represents a Flocker volume attached to a kubelet's host machine. This depends on the Flocker control service being running\\n+optional\"\n" +
		"        },\n" +
		"        \"downwardAPI\": {\n" +
		"          \"$ref\": \"#/definitions/v1DownwardAPIVolumeSource\",\n" +
		"          \"title\": \"DownwardAPI represents downward API about the pod that should populate this volume\\n+optional\"\n" +
		"        },\n" +
		"        \"fc\": {\n" +
		"          \"$ref\": \"#/definitions/v1FCVolumeSource\",\n" +
		"          \"title\": \"FC represents a Fibre Channel resource that is attached to a kubelet's host machine and then exposed to the pod.\\n+optional\"\n" +
		"        },\n" +
		"        \"azureFile\": {\n" +
		"          \"$ref\": \"#/definitions/v1AzureFileVolumeSource\",\n" +
		"          \"title\": \"AzureFile represents an Azure File Service mount on the host and bind mount to the pod.\\n+optional\"\n" +
		"        },\n" +
		"        \"configMap\": {\n" +
		"          \"$ref\": \"#/definitions/v1ConfigMapVolumeSource\",\n" +
		"          \"title\": \"ConfigMap represents a configMap that should populate this volume\\n+optional\"\n" +
		"        },\n" +
		"        \"vsphereVolume\": {\n" +
		"          \"$ref\": \"#/definitions/v1VsphereVirtualDiskVolumeSource\",\n" +
		"          \"title\": \"VsphereVolume represents a vSphere volume attached and mounted on kubelets host machine\\n+optional\"\n" +
		"        },\n" +
		"        \"quobyte\": {\n" +
		"          \"$ref\": \"#/definitions/v1QuobyteVolumeSource\",\n" +
		"          \"title\": \"Quobyte represents a Quobyte mount on the host that shares a pod's lifetime\\n+optional\"\n" +
		"        },\n" +
		"        \"azureDisk\": {\n" +
		"          \"$ref\": \"#/definitions/v1AzureDiskVolumeSource\",\n" +
		"          \"title\": \"AzureDisk represents an Azure Data Disk mount on the host and bind mount to the pod.\\n+optional\"\n" +
		"        },\n" +
		"        \"photonPersistentDisk\": {\n" +
		"          \"$ref\": \"#/definitions/v1PhotonPersistentDiskVolumeSource\",\n" +
		"          \"title\": \"PhotonPersistentDisk represents a PhotonController persistent disk attached and mounted on kubelets host machine\"\n" +
		"        },\n" +
		"        \"projected\": {\n" +
		"          \"$ref\": \"#/definitions/v1ProjectedVolumeSource\",\n" +
		"          \"title\": \"Items for all in one resources secrets, configmaps, and downward API\"\n" +
		"        },\n" +
		"        \"portworxVolume\": {\n" +
		"          \"$ref\": \"#/definitions/v1PortworxVolumeSource\",\n" +
		"          \"title\": \"PortworxVolume represents a portworx volume attached and mounted on kubelets host machine\\n+optional\"\n" +
		"        },\n" +
		"        \"scaleIO\": {\n" +
		"          \"$ref\": \"#/definitions/v1ScaleIOVolumeSource\",\n" +
		"          \"title\": \"ScaleIO represents a ScaleIO persistent volume attached and mounted on Kubernetes nodes.\\n+optional\"\n" +
		"        },\n" +
		"        \"storageos\": {\n" +
		"          \"$ref\": \"#/definitions/v1StorageOSVolumeSource\",\n" +
		"          \"title\": \"StorageOS represents a StorageOS volume attached and mounted on Kubernetes nodes.\\n+optional\"\n" +
		"        },\n" +
		"        \"csi\": {\n" +
		"          \"$ref\": \"#/definitions/v1CSIVolumeSource\",\n" +
		"          \"title\": \"CSI (Container Storage Interface) represents storage that is handled by an external CSI driver (Alpha feature).\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents the source of a volume to mount.\\nOnly one of its members may be specified.\"\n" +
		"    },\n" +
		"    \"v1VsphereVirtualDiskVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"volumePath\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Path that identifies vSphere volume vmdk\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\n+optional\"\n" +
		"        },\n" +
		"        \"storagePolicyName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Storage Policy Based Management (SPBM) profile name.\\n+optional\"\n" +
		"        },\n" +
		"        \"storagePolicyID\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Storage Policy Based Management (SPBM) profile ID associated with the StoragePolicyName.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"Represents a vSphere volume resource.\"\n" +
		"    },\n" +
		"    \"v1WeightedPodAffinityTerm\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"weight\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"description\": \"weight associated with matching the corresponding podAffinityTerm,\\nin the range 1-100.\"\n" +
		"        },\n" +
		"        \"podAffinityTerm\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodAffinityTerm\",\n" +
		"          \"description\": \"Required. A pod affinity term, associated with the corresponding weight.\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"The weights of all of the matched WeightedPodAffinityTerm fields are added per-node to find the most preferred node(s)\"\n" +
		"    },\n" +
		"    \"v1WindowsSecurityContextOptions\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"gmsaCredentialSpecName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"GMSACredentialSpecName is the name of the GMSA credential spec to use.\\nThis field is alpha-level and is only honored by servers that enable the WindowsGMSA feature flag.\\n+optional\"\n" +
		"        },\n" +
		"        \"gmsaCredentialSpec\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"GMSACredentialSpec is where the GMSA admission webhook\\n(https://github.com/kubernetes-sigs/windows-gmsa) inlines the contents of the\\nGMSA credential spec named by the GMSACredentialSpecName field.\\nThis field is alpha-level and is only honored by servers that enable the WindowsGMSA feature flag.\\n+optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"description\": \"WindowsSecurityContextOptions contain Windows-specific options and credentials.\"\n" +
		"    }\n" +
		"  },\n" +
		"  \"x-stream-definitions\": {\n" +
		"    \"apiEventStreamMessage\": {\n" +
		"      \"type\": \"file\",\n" +
		"      \"properties\": {\n" +
		"        \"result\": {\n" +
		"          \"$ref\": \"#/definitions/apiEventStreamMessage\"\n" +
		"        },\n" +
		"        \"error\": {\n" +
		"          \"$ref\": \"#/definitions/runtimeStreamError\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"title\": \"Stream result of apiEventStreamMessage\"\n" +
		"    }\n" +
		"  }\n" +
		"}\n" +
		""
	return tmpl
}
