/*
 * CODE GENERATED AUTOMATICALLY WITH
 *    github.com/wlbr/templify
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */

package api

// SwaggerJsonTemplate is a generated function returning the template as a string.
// That string should be parsed by the functions of the golang's template package.
func SwaggerJsonTemplate() string {
	tmpl := "{\n" +
		"  \"consumes\": [\n" +
		"    \"application/json\"\n" +
		"  ],\n" +
		"  \"produces\": [\n" +
		"    \"application/json\"\n" +
		"  ],\n" +
		"  \"swagger\": \"2.0\",\n" +
		"  \"info\": {\n" +
		"    \"title\": \"pkg/api/event.proto\",\n" +
		"    \"version\": \"version not set\"\n" +
		"  },\n" +
		"  \"paths\": {\n" +
		"    \"/v1/batched/create_queues\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ],\n" +
		"        \"operationId\": \"CreateQueues\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiQueueList\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiBatchQueueCreateResponse\"\n" +
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
		"    \"/v1/batched/update_queues\": {\n" +
		"      \"put\": {\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ],\n" +
		"        \"operationId\": \"UpdateQueues\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiQueueList\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiBatchQueueUpdateResponse\"\n" +
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
		"    \"/v1/job-set/{queue}/{id}\": {\n" +
		"      \"post\": {\n" +
		"        \"produces\": [\n" +
		"          \"application/ndjson-stream\"\n" +
		"        ],\n" +
		"        \"tags\": [\n" +
		"          \"Event\"\n" +
		"        ],\n" +
		"        \"operationId\": \"GetJobSetEvents\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"type\": \"string\",\n" +
		"            \"name\": \"queue\",\n" +
		"            \"in\": \"path\",\n" +
		"            \"required\": true\n" +
		"          },\n" +
		"          {\n" +
		"            \"type\": \"string\",\n" +
		"            \"name\": \"id\",\n" +
		"            \"in\": \"path\",\n" +
		"            \"required\": true\n" +
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
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.(streaming responses)\",\n" +
		"            \"schema\": {\n" +
		"              \"type\": \"file\",\n" +
		"              \"title\": \"Stream result of apiEventStreamMessage\",\n" +
		"              \"properties\": {\n" +
		"                \"error\": {\n" +
		"                  \"$ref\": \"#/definitions/runtimeStreamError\"\n" +
		"                },\n" +
		"                \"result\": {\n" +
		"                  \"$ref\": \"#/definitions/apiEventStreamMessage\"\n" +
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
		"    \"/v1/job/cancel\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ],\n" +
		"        \"operationId\": \"CancelJobs\",\n" +
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
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiCancellationResult\"\n" +
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
		"    \"/v1/job/reprioritize\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ],\n" +
		"        \"operationId\": \"ReprioritizeJobs\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiJobReprioritizeRequest\"\n" +
		"            }\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiJobReprioritizeResponse\"\n" +
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
		"    \"/v1/job/submit\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ],\n" +
		"        \"operationId\": \"SubmitJobs\",\n" +
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
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiJobSubmitResponse\"\n" +
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
		"    \"/v1/jobset/cancel\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ],\n" +
		"        \"operationId\": \"CancelJobSet\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiJobSetCancelRequest\"\n" +
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
		"    \"/v1/queue\": {\n" +
		"      \"post\": {\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ],\n" +
		"        \"operationId\": \"CreateQueue\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"name\": \"body\",\n" +
		"            \"in\": \"body\",\n" +
		"            \"required\": true,\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiQueue\"\n" +
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
		"    \"/v1/queue/{name}\": {\n" +
		"      \"get\": {\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ],\n" +
		"        \"operationId\": \"GetQueue\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"type\": \"string\",\n" +
		"            \"name\": \"name\",\n" +
		"            \"in\": \"path\",\n" +
		"            \"required\": true\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiQueue\"\n" +
		"            }\n" +
		"          },\n" +
		"          \"default\": {\n" +
		"            \"description\": \"An unexpected error response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/runtimeError\"\n" +
		"            }\n" +
		"          }\n" +
		"        }\n" +
		"      },\n" +
		"      \"put\": {\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ],\n" +
		"        \"operationId\": \"UpdateQueue\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"type\": \"string\",\n" +
		"            \"name\": \"name\",\n" +
		"            \"in\": \"path\",\n" +
		"            \"required\": true\n" +
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
		"      },\n" +
		"      \"delete\": {\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ],\n" +
		"        \"operationId\": \"DeleteQueue\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"type\": \"string\",\n" +
		"            \"name\": \"name\",\n" +
		"            \"in\": \"path\",\n" +
		"            \"required\": true\n" +
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
		"    \"/v1/queue/{name}/info\": {\n" +
		"      \"get\": {\n" +
		"        \"tags\": [\n" +
		"          \"Submit\"\n" +
		"        ],\n" +
		"        \"operationId\": \"GetQueueInfo\",\n" +
		"        \"parameters\": [\n" +
		"          {\n" +
		"            \"type\": \"string\",\n" +
		"            \"name\": \"name\",\n" +
		"            \"in\": \"path\",\n" +
		"            \"required\": true\n" +
		"          }\n" +
		"        ],\n" +
		"        \"responses\": {\n" +
		"          \"200\": {\n" +
		"            \"description\": \"A successful response.\",\n" +
		"            \"schema\": {\n" +
		"              \"$ref\": \"#/definitions/apiQueueInfo\"\n" +
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
		"    \"PermissionsSubject\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"kind\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"QueuePermissions\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"subjects\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/PermissionsSubject\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"verbs\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiBatchQueueCreateResponse\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"failedQueues\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiQueueCreateResponse\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiBatchQueueUpdateResponse\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"failedQueues\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiQueueUpdateResponse\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiCancellationResult\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"cancelledIds\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiCause\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"default\": \"Error\",\n" +
		"      \"enum\": [\n" +
		"        \"Error\",\n" +
		"        \"Evicted\",\n" +
		"        \"OOM\",\n" +
		"        \"DeadlineExceeded\"\n" +
		"      ]\n" +
		"    },\n" +
		"    \"apiContainerStatus\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"cause\": {\n" +
		"          \"$ref\": \"#/definitions/apiCause\"\n" +
		"        },\n" +
		"        \"exitCode\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"message\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiEventMessage\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"cancelled\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobCancelledEvent\"\n" +
		"        },\n" +
		"        \"cancelling\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobCancellingEvent\"\n" +
		"        },\n" +
		"        \"duplicateFound\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobDuplicateFoundEvent\"\n" +
		"        },\n" +
		"        \"failed\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobFailedEvent\"\n" +
		"        },\n" +
		"        \"failedCompressed\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobFailedEventCompressed\"\n" +
		"        },\n" +
		"        \"ingressInfo\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobIngressInfoEvent\"\n" +
		"        },\n" +
		"        \"leaseExpired\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobLeaseExpiredEvent\"\n" +
		"        },\n" +
		"        \"leaseReturned\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobLeaseReturnedEvent\"\n" +
		"        },\n" +
		"        \"leased\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobLeasedEvent\"\n" +
		"        },\n" +
		"        \"pending\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobPendingEvent\"\n" +
		"        },\n" +
		"        \"preempted\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobPreemptedEvent\"\n" +
		"        },\n" +
		"        \"queued\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobQueuedEvent\"\n" +
		"        },\n" +
		"        \"reprioritized\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobReprioritizedEvent\"\n" +
		"        },\n" +
		"        \"reprioritizing\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobReprioritizingEvent\"\n" +
		"        },\n" +
		"        \"running\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobRunningEvent\"\n" +
		"        },\n" +
		"        \"submitted\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobSubmittedEvent\"\n" +
		"        },\n" +
		"        \"succeeded\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobSucceededEvent\"\n" +
		"        },\n" +
		"        \"terminated\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobTerminatedEvent\"\n" +
		"        },\n" +
		"        \"unableToSchedule\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobUnableToScheduleEvent\"\n" +
		"        },\n" +
		"        \"updated\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobUpdatedEvent\"\n" +
		"        },\n" +
		"        \"utilisation\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobUtilisationEvent\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiEventStreamMessage\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"id\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"message\": {\n" +
		"          \"$ref\": \"#/definitions/apiEventMessage\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiIngressConfig\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"annotations\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"certName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"ports\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"integer\",\n" +
		"            \"format\": \"int64\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"tlsEnabled\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"$ref\": \"#/definitions/apiIngressType\"\n" +
		"        },\n" +
		"        \"useClusterIP\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiIngressType\": {\n" +
		"      \"description\": \"Ingress type is being kept here to maintain backwards compatibility for a while.\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"default\": \"Ingress\",\n" +
		"      \"enum\": [\n" +
		"        \"Ingress\"\n" +
		"      ]\n" +
		"    },\n" +
		"    \"apiJob\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"annotations\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"clientId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"compressedQueueOwnershipUserGroups\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"byte\"\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"id\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"ingress\": {\n" +
		"          \"description\": \"Services can be provided either as Armada-specific config objects or as proper k8s objects.\\nThese options are exclusive, i.e., if either ingress or services is provided,\\nthen neither of k8s_ingress or k8s_service can be provided, and vice versa.\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiIngressConfig\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"k8sIngress\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Ingress\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"k8sService\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Service\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"labels\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"namespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"owner\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podSpec\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodSpec\"\n" +
		"        },\n" +
		"        \"podSpecs\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PodSpec\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"priority\": {\n" +
		"          \"type\": \"number\",\n" +
		"          \"format\": \"double\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queueOwnershipUserGroups\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"queueTtlSeconds\": {\n" +
		"          \"description\": \"Queuing TTL for this job in seconds. If this job queues for more than this duration it will be cancelled. Zero indicates an infinite lifetime.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\"\n" +
		"        },\n" +
		"        \"requiredNodeLabels\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"scheduler\": {\n" +
		"          \"description\": \"Indicates which scheduler should manage this job.\\nIf empty, the default scheduler is used.\",\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"schedulingResourceRequirements\": {\n" +
		"          \"description\": \"max(\\n\\n\\tsum across all containers,\\n\\tmax over all init containers,\\n\\n)\\n\\nThis is because containers run in parallel, whereas initContainers run serially.\\nThis field is populated automatically at submission.\\nSubmitting a job with this field already populated results in an error.\",\n" +
		"          \"title\": \"Resource requests and limits necessary for scheduling the main pod of this job.\\nThe requests and limits herein are set to:\",\n" +
		"          \"$ref\": \"#/definitions/v1ResourceRequirements\"\n" +
		"        },\n" +
		"        \"services\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiServiceConfig\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobCancelRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobIds\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobCancelledEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"requestor\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobCancellingEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"requestor\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobDuplicateFoundEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"originalJobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobFailedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"cause\": {\n" +
		"          \"$ref\": \"#/definitions/apiCause\"\n" +
		"        },\n" +
		"        \"clusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"containerStatuses\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiContainerStatus\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"exitCodes\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"integer\",\n" +
		"            \"format\": \"int32\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"kubernetesId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"nodeName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNamespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNumber\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobFailedEventCompressed\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Only used internally by Armada\",\n" +
		"      \"properties\": {\n" +
		"        \"event\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"byte\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobIngressInfoEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"clusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"ingressAddresses\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"kubernetesId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"nodeName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNamespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNumber\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobLeaseExpiredEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobLeaseReturnedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"clusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"kubernetesId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNumber\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"runAttempted\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobLeasedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"clusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobPendingEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"clusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"kubernetesId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNamespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNumber\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobPreemptedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"clusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"preemptiveJobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"preemptiveRunId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"runId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobQueuedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobReprioritizeRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"jobIds\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"newPriority\": {\n" +
		"          \"type\": \"number\",\n" +
		"          \"format\": \"double\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobReprioritizeResponse\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"reprioritizationResults\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobReprioritizedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"newPriority\": {\n" +
		"          \"type\": \"number\",\n" +
		"          \"format\": \"double\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"requestor\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobReprioritizingEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"newPriority\": {\n" +
		"          \"type\": \"number\",\n" +
		"          \"format\": \"double\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"requestor\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobRunningEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"clusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"kubernetesId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"nodeName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNamespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNumber\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSetCancelRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"filter\": {\n" +
		"          \"$ref\": \"#/definitions/apiJobSetFilter\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSetFilter\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"states\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiJobState\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSetInfo\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"leasedJobs\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queuedJobs\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSetRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"errorIfMissing\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"forceLegacy\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"forceNew\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        },\n" +
		"        \"fromMessageId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"id\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"watch\": {\n" +
		"          \"type\": \"boolean\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobState\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"default\": \"QUEUED\",\n" +
		"      \"enum\": [\n" +
		"        \"QUEUED\",\n" +
		"        \"PENDING\",\n" +
		"        \"RUNNING\",\n" +
		"        \"SUCCEEDED\",\n" +
		"        \"FAILED\",\n" +
		"        \"UNKNOWN\"\n" +
		"      ]\n" +
		"    },\n" +
		"    \"apiJobSubmitRequest\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"jobRequestItems\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiJobSubmitRequestItem\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSubmitRequestItem\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"annotations\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"clientId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"ingress\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiIngressConfig\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"labels\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"namespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podSpec\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodSpec\"\n" +
		"        },\n" +
		"        \"podSpecs\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PodSpec\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"priority\": {\n" +
		"          \"type\": \"number\",\n" +
		"          \"format\": \"double\"\n" +
		"        },\n" +
		"        \"queueTtlSeconds\": {\n" +
		"          \"description\": \"Queuing TTL for this job in seconds. If this job queues for more than this duration it will be cancelled. Zero indicates an infinite lifetime.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\"\n" +
		"        },\n" +
		"        \"requiredNodeLabels\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"scheduler\": {\n" +
		"          \"description\": \"Indicates which scheduler should manage this job.\\nIf empty, the default scheduler is used.\",\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"services\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiServiceConfig\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSubmitResponse\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"jobResponseItems\": {\n" +
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
		"        \"error\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSubmittedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"job\": {\n" +
		"          \"$ref\": \"#/definitions/apiJob\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobSucceededEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"clusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"kubernetesId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"nodeName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNamespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNumber\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobTerminatedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"clusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"kubernetesId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNamespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNumber\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobUnableToScheduleEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"clusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"kubernetesId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"nodeName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNamespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNumber\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobUpdatedEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"job\": {\n" +
		"          \"$ref\": \"#/definitions/apiJob\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"requestor\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiJobUtilisationEvent\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"MaxResourcesForPeriod\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"$ref\": \"#/definitions/resourceQuantity\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"clusterId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"created\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"date-time\"\n" +
		"        },\n" +
		"        \"jobId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"jobSetId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"kubernetesId\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"nodeName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podName\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNamespace\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"podNumber\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"totalCumulativeUsage\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"$ref\": \"#/definitions/resourceQuantity\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiQueue\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"groupOwners\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"permissions\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/QueuePermissions\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"priorityFactor\": {\n" +
		"          \"type\": \"number\",\n" +
		"          \"format\": \"double\"\n" +
		"        },\n" +
		"        \"resourceLimits\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"number\",\n" +
		"            \"format\": \"double\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"userOwners\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiQueueCreateResponse\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"error\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"$ref\": \"#/definitions/apiQueue\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiQueueInfo\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"activeJobSets\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiJobSetInfo\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiQueueList\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"swagger:model\",\n" +
		"      \"properties\": {\n" +
		"        \"queues\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/apiQueue\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiQueueUpdateResponse\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"error\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"queue\": {\n" +
		"          \"$ref\": \"#/definitions/apiQueue\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiServiceConfig\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"ports\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"integer\",\n" +
		"            \"format\": \"int64\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"$ref\": \"#/definitions/apiServiceType\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"apiServiceType\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"default\": \"NodePort\",\n" +
		"      \"enum\": [\n" +
		"        \"NodePort\",\n" +
		"        \"Headless\"\n" +
		"      ]\n" +
		"    },\n" +
		"    \"intstrIntOrString\": {\n" +
		"      \"description\": \"+protobuf=true\\n+protobuf.options.(gogoproto.goproto_stringer)=false\\n+k8s:openapi-gen=true\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"IntOrString is a type that can hold an int32 or a string.  When used in\\nJSON or YAML marshalling and unmarshalling, it produces or consumes the\\ninner type.  This allows you to have, for example, a JSON field that can\\naccept a name or number.\\nTODO: Rename to Int32OrString\",\n" +
		"      \"properties\": {\n" +
		"        \"IntVal\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"StrVal\": {\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"Type\": {\n" +
		"          \"$ref\": \"#/definitions/intstrType\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/util/intstr\"\n" +
		"    },\n" +
		"    \"intstrType\": {\n" +
		"      \"type\": \"integer\",\n" +
		"      \"format\": \"int64\",\n" +
		"      \"title\": \"Type represents the stored type of IntOrString.\",\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/util/intstr\"\n" +
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
		"    \"resourceQuantity\": {\n" +
		"      \"description\": \"The serialization format is:\\n\\n\\u003cquantity\\u003e        ::= \\u003csignedNumber\\u003e\\u003csuffix\\u003e\\n(Note that \\u003csuffix\\u003e may be empty, from the \\\"\\\" case in \\u003cdecimalSI\\u003e.)\\n\\u003cdigit\\u003e           ::= 0 | 1 | ... | 9\\n\\u003cdigits\\u003e          ::= \\u003cdigit\\u003e | \\u003cdigit\\u003e\\u003cdigits\\u003e\\n\\u003cnumber\\u003e          ::= \\u003cdigits\\u003e | \\u003cdigits\\u003e.\\u003cdigits\\u003e | \\u003cdigits\\u003e. | .\\u003cdigits\\u003e\\n\\u003csign\\u003e            ::= \\\"+\\\" | \\\"-\\\"\\n\\u003csignedNumber\\u003e    ::= \\u003cnumber\\u003e | \\u003csign\\u003e\\u003cnumber\\u003e\\n\\u003csuffix\\u003e          ::= \\u003cbinarySI\\u003e | \\u003cdecimalExponent\\u003e | \\u003cdecimalSI\\u003e\\n\\u003cbinarySI\\u003e        ::= Ki | Mi | Gi | Ti | Pi | Ei\\n(International System of units; See: http://physics.nist.gov/cuu/Units/binary.html)\\n\\u003cdecimalSI\\u003e       ::= m | \\\"\\\" | k | M | G | T | P | E\\n(Note that 1024 = 1Ki but 1000 = 1k; I didn't choose the capitalization.)\\n\\u003cdecimalExponent\\u003e ::= \\\"e\\\" \\u003csignedNumber\\u003e | \\\"E\\\" \\u003csignedNumber\\u003e\\n\\nNo matter which of the three exponent forms is used, no quantity may represent\\na number greater than 2^63-1 in magnitude, nor may it have more than 3 decimal\\nplaces. Numbers larger or more precise will be capped or rounded up.\\n(E.g.: 0.1m will rounded up to 1m.)\\nThis may be extended in the future if we require larger or smaller quantities.\\n\\nWhen a Quantity is parsed from a string, it will remember the type of suffix\\nit had, and will use the same type again when it is serialized.\\n\\nBefore serializing, Quantity will be put in \\\"canonical form\\\".\\nThis means that Exponent/suffix will be adjusted up or down (with a\\ncorresponding increase or decrease in Mantissa) such that:\\na. No precision is lost\\nb. No fractional digits will be emitted\\nc. The exponent (or suffix) is as large as possible.\\nThe sign will be omitted unless the number is negative.\\n\\nExamples:\\n1.5 will be serialized as \\\"1500m\\\"\\n1.5Gi will be serialized as \\\"1536Mi\\\"\\n\\nNote that the quantity will NEVER be internally represented by a\\nfloating point number. That is the whole point of this exercise.\\n\\nNon-canonical values will still parse as long as they are well formed,\\nbut will be re-emitted in their canonical form. (So always use canonical\\nform, or don't diff.)\\n\\nThis format is intended to make it difficult to use these numbers without\\nwriting some sort of special handling code in the hopes that that will\\ncause implementors to also use a fixed point implementation.\\n\\n+protobuf=true\\n+protobuf.embed=string\\n+protobuf.options.marshal=false\\n+protobuf.options.(gogoproto.goproto_stringer)=false\\n+k8s:deepcopy-gen=true\\n+k8s:openapi-gen=true\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"Quantity is a fixed-point representation of a number.\\nIt provides convenient marshaling/unmarshaling in JSON and YAML,\\nin addition to String() and AsInt64() accessors.\",\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/api/resource\"\n" +
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
		"    },\n" +
		"    \"typesUID\": {\n" +
		"      \"description\": \"UID is a type that holds unique ID values, including UUIDs.  Because we\\ndon't ONLY use UUIDs, this is an alias to string.  Being a type captures\\nintent and helps make sure that UIDs and names do not get conflated.\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/types\"\n" +
		"    },\n" +
		"    \"v1AWSElasticBlockStoreVolumeSource\": {\n" +
		"      \"description\": \"An AWS EBS disk must exist before mounting to a container. The disk\\nmust also be in the same AWS zone as the kubelet. An AWS EBS disk\\ncan only be mounted as read/write once. AWS EBS volumes support\\nownership management and SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a Persistent Disk resource in AWS.\",\n" +
		"      \"properties\": {\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type of the volume that you want to mount.\\nTip: Ensure that the filesystem type is supported by the host operating system.\\nExamples: \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore\\nTODO: how do we prevent errors in the filesystem from compromising the machine\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"partition\": {\n" +
		"          \"description\": \"The partition in the volume that you want to mount.\\nIf omitted, the default is to mount by volume name.\\nExamples: For volume /dev/sda1, you specify the partition as \\\"1\\\".\\nSimilarly, the volume partition for /dev/sda is \\\"0\\\" (or you can leave the property empty).\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"Partition\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Specify \\\"true\\\" to force and set the ReadOnly property in VolumeMounts to \\\"true\\\".\\nIf omitted, the default is \\\"false\\\".\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"volumeID\": {\n" +
		"          \"description\": \"Unique ID of the persistent disk resource in AWS (Amazon EBS volume).\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"VolumeID\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1Affinity\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Affinity is a group of affinity scheduling rules.\",\n" +
		"      \"properties\": {\n" +
		"        \"nodeAffinity\": {\n" +
		"          \"$ref\": \"#/definitions/v1NodeAffinity\"\n" +
		"        },\n" +
		"        \"podAffinity\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodAffinity\"\n" +
		"        },\n" +
		"        \"podAntiAffinity\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodAntiAffinity\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1AzureDataDiskCachingMode\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1AzureDataDiskKind\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1AzureDiskVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"AzureDisk represents an Azure Data Disk mount on the host and bind mount to the pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"cachingMode\": {\n" +
		"          \"$ref\": \"#/definitions/v1AzureDataDiskCachingMode\"\n" +
		"        },\n" +
		"        \"diskName\": {\n" +
		"          \"description\": \"The Name of the data disk in the blob storage\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"DiskName\"\n" +
		"        },\n" +
		"        \"diskURI\": {\n" +
		"          \"description\": \"The URI the data disk in the blob storage\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"DataDiskURI\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"kind\": {\n" +
		"          \"$ref\": \"#/definitions/v1AzureDataDiskKind\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1AzureFileVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"AzureFile represents an Azure File Service mount on the host and bind mount to the pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"secretName\": {\n" +
		"          \"description\": \"the name of secret that contains Azure Storage Account Name and Key\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"SecretName\"\n" +
		"        },\n" +
		"        \"shareName\": {\n" +
		"          \"description\": \"Share Name\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"ShareName\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1CSIVolumeSource\": {\n" +
		"      \"description\": \"Represents a source location of a volume to mount, managed by an external CSI driver\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"driver\": {\n" +
		"          \"description\": \"Driver is the name of the CSI driver that handles this volume.\\nConsult with your admin for the correct name as registered in the cluster.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Driver\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type to mount. Ex. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\".\\nIf not provided, the empty value is passed to the associated CSI driver\\nwhich will determine the default filesystem to apply.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"nodePublishSecretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Specifies a read-only configuration for the volume.\\nDefaults to false (read/write).\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"volumeAttributes\": {\n" +
		"          \"description\": \"VolumeAttributes stores driver-specific properties that are passed to the CSI\\ndriver. Consult your driver's documentation for supported values.\\n+optional\",\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"VolumeAttributes\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1Capabilities\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Adds and removes POSIX capabilities from running containers.\",\n" +
		"      \"properties\": {\n" +
		"        \"add\": {\n" +
		"          \"description\": \"Added capabilities\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Capability\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Add\"\n" +
		"        },\n" +
		"        \"drop\": {\n" +
		"          \"description\": \"Removed capabilities\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Capability\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Drop\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1Capability\": {\n" +
		"      \"description\": \"Capability represent POSIX capabilities type\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1CephFSVolumeSource\": {\n" +
		"      \"description\": \"Represents a Ceph Filesystem mount that lasts the lifetime of a pod\\nCephfs volumes do not support ownership management or SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"monitors\": {\n" +
		"          \"description\": \"Required: Monitors is a collection of Ceph monitors\\nMore info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Monitors\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"description\": \"Optional: Used as the mounted root, rather than the full Ceph tree, default is /\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Path\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Optional: Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\nMore info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"secretFile\": {\n" +
		"          \"description\": \"Optional: SecretFile is the path to key ring for User, default is /etc/ceph/user.secret\\nMore info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"SecretFile\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"        },\n" +
		"        \"user\": {\n" +
		"          \"description\": \"Optional: User is the rados user name, default is admin\\nMore info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"User\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1CinderVolumeSource\": {\n" +
		"      \"description\": \"A Cinder volume must exist before mounting to a container.\\nThe volume must also be in the same region as the kubelet.\\nCinder volumes support ownership management and SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a cinder volume resource in Openstack.\",\n" +
		"      \"properties\": {\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nExamples: \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nMore info: https://examples.k8s.io/mysql-cinder-pd/README.md\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Optional: Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\nMore info: https://examples.k8s.io/mysql-cinder-pd/README.md\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"        },\n" +
		"        \"volumeID\": {\n" +
		"          \"description\": \"volume id used to identify the volume in cinder.\\nMore info: https://examples.k8s.io/mysql-cinder-pd/README.md\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"VolumeID\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ClientIPConfig\": {\n" +
		"      \"description\": \"ClientIPConfig represents the configurations of Client IP based session affinity.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"timeoutSeconds\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"timeoutSeconds specifies the seconds of ClientIP type session sticky time.\\nThe value must be \\u003e0 \\u0026\\u0026 \\u003c=86400(for 1 day) if ServiceAffinity == \\\"ClientIP\\\".\\nDefault value is 10800(for 3 hours).\\n+optional\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1Condition\": {\n" +
		"      \"description\": \"// other fields\\n}\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Condition contains details for one aspect of the current state of this API Resource.\\n---\\nThis struct is intended for direct use as an array at the field path .status.conditions.  For example,\\ntype FooStatus struct{\\n    // Represents the observations of a foo's current state.\\n    // Known .status.conditions.type are: \\\"Available\\\", \\\"Progressing\\\", and \\\"Degraded\\\"\\n    // +patchMergeKey=type\\n    // +patchStrategy=merge\\n    // +listType=map\\n    // +listMapKey=type\\n    Conditions []metav1.Condition `json:\\\"conditions,omitempty\\\" patchStrategy:\\\"merge\\\" patchMergeKey:\\\"type\\\" protobuf:\\\"bytes,1,rep,name=conditions\\\"`\",\n" +
		"      \"properties\": {\n" +
		"        \"lastTransitionTime\": {\n" +
		"          \"title\": \"lastTransitionTime is the last time the condition transitioned from one status to another.\\nThis should be when the underlying condition changed.  If that is not known, then using the time when the API field changed is acceptable.\\n+required\\n+kubebuilder:validation:Required\\n+kubebuilder:validation:Type=string\\n+kubebuilder:validation:Format=date-time\",\n" +
		"          \"$ref\": \"#/definitions/v1Time\"\n" +
		"        },\n" +
		"        \"message\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"message is a human readable message indicating details about the transition.\\nThis may be an empty string.\\n+required\\n+kubebuilder:validation:Required\\n+kubebuilder:validation:MaxLength=32768\"\n" +
		"        },\n" +
		"        \"observedGeneration\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"title\": \"observedGeneration represents the .metadata.generation that the condition was set based upon.\\nFor instance, if .metadata.generation is currently 12, but the .status.conditions[x].observedGeneration is 9, the condition is out of date\\nwith respect to the current state of the instance.\\n+optional\\n+kubebuilder:validation:Minimum=0\"\n" +
		"        },\n" +
		"        \"reason\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"reason contains a programmatic identifier indicating the reason for the condition's last transition.\\nProducers of specific condition types may define expected values and meanings for this field,\\nand whether the values are considered a guaranteed API.\\nThe value should be a CamelCase string.\\nThis field may not be empty.\\n+required\\n+kubebuilder:validation:Required\\n+kubebuilder:validation:MaxLength=1024\\n+kubebuilder:validation:MinLength=1\\n+kubebuilder:validation:Pattern=`^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`\"\n" +
		"        },\n" +
		"        \"status\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"status of the condition, one of True, False, Unknown.\\n+required\\n+kubebuilder:validation:Required\\n+kubebuilder:validation:Enum=True;False;Unknown\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"type of condition in CamelCase or in foo.example.com/CamelCase.\\n---\\nMany .condition.type values are consistent across resources like Available, but because arbitrary conditions can be\\nuseful (see .node.status.conditions), the ability to deconflict is important.\\nThe regex it matches is (dns1123SubdomainFmt/)?(qualifiedNameFmt)\\n+required\\n+kubebuilder:validation:Required\\n+kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\\\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$`\\n+kubebuilder:validation:MaxLength=316\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1ConfigMapEnvSource\": {\n" +
		"      \"description\": \"The contents of the target ConfigMap's Data field will represent the\\nkey-value pairs as environment variables.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"ConfigMapEnvSource selects a ConfigMap to populate the environment\\nvariables with.\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the referent.\\nMore info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names\\nTODO: Add other useful fields. apiVersion, kind, uid?\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"description\": \"Specify whether the ConfigMap must be defined\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ConfigMapKeySelector\": {\n" +
		"      \"description\": \"+structType=atomic\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Selects a key from a ConfigMap.\",\n" +
		"      \"properties\": {\n" +
		"        \"key\": {\n" +
		"          \"description\": \"The key to select.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Key\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the referent.\\nMore info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names\\nTODO: Add other useful fields. apiVersion, kind, uid?\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"description\": \"Specify whether the ConfigMap or its key must be defined\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ConfigMapProjection\": {\n" +
		"      \"description\": \"The contents of the target ConfigMap's Data field will be presented in a\\nprojected volume as files using the keys in the Data field as the file names,\\nunless the items element is populated with specific mappings of keys to paths.\\nNote that this is identical to a configmap volume source without the default\\nmode.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Adapts a ConfigMap into a projected volume.\",\n" +
		"      \"properties\": {\n" +
		"        \"items\": {\n" +
		"          \"description\": \"If unspecified, each key-value pair in the Data field of the referenced\\nConfigMap will be projected into the volume as a file whose name is the\\nkey and content is the value. If specified, the listed keys will be\\nprojected into the specified paths, and unlisted keys will not be\\npresent. If a key is specified which is not present in the ConfigMap,\\nthe volume setup will error unless it is marked optional. Paths must be\\nrelative and may not contain the '..' path or start with '..'.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1KeyToPath\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Items\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the referent.\\nMore info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names\\nTODO: Add other useful fields. apiVersion, kind, uid?\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"description\": \"Specify whether the ConfigMap or its keys must be defined\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ConfigMapVolumeSource\": {\n" +
		"      \"description\": \"The contents of the target ConfigMap's Data field will be presented in a\\nvolume as files using the keys in the Data field as the file names, unless\\nthe items element is populated with specific mappings of keys to paths.\\nConfigMap volumes support ownership management and SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Adapts a ConfigMap into a volume.\",\n" +
		"      \"properties\": {\n" +
		"        \"defaultMode\": {\n" +
		"          \"description\": \"Optional: mode bits used to set permissions on created files by default.\\nMust be an octal value between 0000 and 0777 or a decimal value between 0 and 511.\\nYAML accepts both octal and decimal values, JSON requires decimal values for mode bits.\\nDefaults to 0644.\\nDirectories within the path are not affected by this setting.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"DefaultMode\"\n" +
		"        },\n" +
		"        \"items\": {\n" +
		"          \"description\": \"If unspecified, each key-value pair in the Data field of the referenced\\nConfigMap will be projected into the volume as a file whose name is the\\nkey and content is the value. If specified, the listed keys will be\\nprojected into the specified paths, and unlisted keys will not be\\npresent. If a key is specified which is not present in the ConfigMap,\\nthe volume setup will error unless it is marked optional. Paths must be\\nrelative and may not contain the '..' path or start with '..'.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1KeyToPath\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Items\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the referent.\\nMore info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names\\nTODO: Add other useful fields. apiVersion, kind, uid?\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"description\": \"Specify whether the ConfigMap or its keys must be defined\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1Container\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"A single application container that you want to run within a pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"args\": {\n" +
		"          \"description\": \"Arguments to the entrypoint.\\nThe docker image's CMD is used if this is not provided.\\nVariable references $(VAR_NAME) are expanded using the container's environment. If a variable\\ncannot be resolved, the reference in the input string will be unchanged. Double $$ are reduced\\nto a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. \\\"$$(VAR_NAME)\\\" will\\nproduce the string literal \\\"$(VAR_NAME)\\\". Escaped references will never be expanded, regardless\\nof whether the variable exists or not. Cannot be updated.\\nMore info: https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Args\"\n" +
		"        },\n" +
		"        \"command\": {\n" +
		"          \"description\": \"Entrypoint array. Not executed within a shell.\\nThe docker image's ENTRYPOINT is used if this is not provided.\\nVariable references $(VAR_NAME) are expanded using the container's environment. If a variable\\ncannot be resolved, the reference in the input string will be unchanged. Double $$ are reduced\\nto a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. \\\"$$(VAR_NAME)\\\" will\\nproduce the string literal \\\"$(VAR_NAME)\\\". Escaped references will never be expanded, regardless\\nof whether the variable exists or not. Cannot be updated.\\nMore info: https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Command\"\n" +
		"        },\n" +
		"        \"env\": {\n" +
		"          \"description\": \"List of environment variables to set in the container.\\nCannot be updated.\\n+optional\\n+patchMergeKey=name\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1EnvVar\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Env\"\n" +
		"        },\n" +
		"        \"envFrom\": {\n" +
		"          \"description\": \"List of sources to populate environment variables in the container.\\nThe keys defined within a source must be a C_IDENTIFIER. All invalid keys\\nwill be reported as an event when the container is starting. When a key exists in multiple\\nsources, the value associated with the last source will take precedence.\\nValues defined by an Env with a duplicate key will take precedence.\\nCannot be updated.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1EnvFromSource\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"EnvFrom\"\n" +
		"        },\n" +
		"        \"image\": {\n" +
		"          \"description\": \"Docker image name.\\nMore info: https://kubernetes.io/docs/concepts/containers/images\\nThis field is optional to allow higher level config management to default or override\\ncontainer images in workload controllers like Deployments and StatefulSets.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Image\"\n" +
		"        },\n" +
		"        \"imagePullPolicy\": {\n" +
		"          \"$ref\": \"#/definitions/v1PullPolicy\"\n" +
		"        },\n" +
		"        \"lifecycle\": {\n" +
		"          \"$ref\": \"#/definitions/v1Lifecycle\"\n" +
		"        },\n" +
		"        \"livenessProbe\": {\n" +
		"          \"$ref\": \"#/definitions/v1Probe\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the container specified as a DNS_LABEL.\\nEach container in a pod must have a unique name (DNS_LABEL).\\nCannot be updated.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"ports\": {\n" +
		"          \"description\": \"List of ports to expose from the container. Exposing a port here gives\\nthe system additional information about the network connections a\\ncontainer uses, but is primarily informational. Not specifying a port here\\nDOES NOT prevent that port from being exposed. Any port which is\\nlistening on the default \\\"0.0.0.0\\\" address inside a container will be\\naccessible from the network.\\nCannot be updated.\\n+optional\\n+patchMergeKey=containerPort\\n+patchStrategy=merge\\n+listType=map\\n+listMapKey=containerPort\\n+listMapKey=protocol\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1ContainerPort\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Ports\"\n" +
		"        },\n" +
		"        \"readinessProbe\": {\n" +
		"          \"$ref\": \"#/definitions/v1Probe\"\n" +
		"        },\n" +
		"        \"resources\": {\n" +
		"          \"$ref\": \"#/definitions/v1ResourceRequirements\"\n" +
		"        },\n" +
		"        \"securityContext\": {\n" +
		"          \"$ref\": \"#/definitions/v1SecurityContext\"\n" +
		"        },\n" +
		"        \"startupProbe\": {\n" +
		"          \"$ref\": \"#/definitions/v1Probe\"\n" +
		"        },\n" +
		"        \"stdin\": {\n" +
		"          \"description\": \"Whether this container should allocate a buffer for stdin in the container runtime. If this\\nis not set, reads from stdin in the container will always result in EOF.\\nDefault is false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Stdin\"\n" +
		"        },\n" +
		"        \"stdinOnce\": {\n" +
		"          \"description\": \"Whether the container runtime should close the stdin channel after it has been opened by\\na single attach. When stdin is true the stdin stream will remain open across multiple attach\\nsessions. If stdinOnce is set to true, stdin is opened on container start, is empty until the\\nfirst client attaches to stdin, and then remains open and accepts data until the client disconnects,\\nat which time stdin is closed and remains closed until the container is restarted. If this\\nflag is false, a container processes that reads from stdin will never receive an EOF.\\nDefault is false\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"StdinOnce\"\n" +
		"        },\n" +
		"        \"terminationMessagePath\": {\n" +
		"          \"description\": \"Optional: Path at which the file to which the container's termination message\\nwill be written is mounted into the container's filesystem.\\nMessage written is intended to be brief final status, such as an assertion failure message.\\nWill be truncated by the node if greater than 4096 bytes. The total message length across\\nall containers will be limited to 12kb.\\nDefaults to /dev/termination-log.\\nCannot be updated.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"TerminationMessagePath\"\n" +
		"        },\n" +
		"        \"terminationMessagePolicy\": {\n" +
		"          \"$ref\": \"#/definitions/v1TerminationMessagePolicy\"\n" +
		"        },\n" +
		"        \"tty\": {\n" +
		"          \"description\": \"Whether this container should allocate a TTY for itself, also requires 'stdin' to be true.\\nDefault is false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"TTY\"\n" +
		"        },\n" +
		"        \"volumeDevices\": {\n" +
		"          \"description\": \"volumeDevices is the list of block devices to be used by the container.\\n+patchMergeKey=devicePath\\n+patchStrategy=merge\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1VolumeDevice\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"VolumeDevices\"\n" +
		"        },\n" +
		"        \"volumeMounts\": {\n" +
		"          \"description\": \"Pod volumes to mount into the container's filesystem.\\nCannot be updated.\\n+optional\\n+patchMergeKey=mountPath\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1VolumeMount\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"VolumeMounts\"\n" +
		"        },\n" +
		"        \"workingDir\": {\n" +
		"          \"description\": \"Container's working directory.\\nIf not specified, the container runtime's default will be used, which\\nmight be configured in the container image.\\nCannot be updated.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"WorkingDir\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ContainerPort\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"ContainerPort represents a network port in a single container.\",\n" +
		"      \"properties\": {\n" +
		"        \"containerPort\": {\n" +
		"          \"description\": \"Number of port to expose on the pod's IP address.\\nThis must be a valid port number, 0 \\u003c x \\u003c 65536.\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"ContainerPort\"\n" +
		"        },\n" +
		"        \"hostIP\": {\n" +
		"          \"description\": \"What host IP to bind the external port to.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"HostIP\"\n" +
		"        },\n" +
		"        \"hostPort\": {\n" +
		"          \"description\": \"Number of port to expose on the host.\\nIf specified, this must be a valid port number, 0 \\u003c x \\u003c 65536.\\nIf HostNetwork is specified, this must match ContainerPort.\\nMost containers do not need this.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"HostPort\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"If specified, this must be an IANA_SVC_NAME and unique within the pod. Each\\nnamed port in a pod must have a unique name. Name for the port that can be\\nreferred to by services.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"protocol\": {\n" +
		"          \"$ref\": \"#/definitions/v1Protocol\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1DNSPolicy\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"DNSPolicy defines how a pod's DNS will be configured.\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1DownwardAPIProjection\": {\n" +
		"      \"description\": \"Note that this is identical to a downwardAPI volume source without the default\\nmode.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents downward API info for projecting into a projected volume.\",\n" +
		"      \"properties\": {\n" +
		"        \"items\": {\n" +
		"          \"description\": \"Items is a list of DownwardAPIVolume file\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1DownwardAPIVolumeFile\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Items\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1DownwardAPIVolumeFile\": {\n" +
		"      \"description\": \"DownwardAPIVolumeFile represents information to create the file containing the pod field\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"fieldRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ObjectFieldSelector\"\n" +
		"        },\n" +
		"        \"mode\": {\n" +
		"          \"description\": \"Optional: mode bits used to set permissions on this file, must be an octal value\\nbetween 0000 and 0777 or a decimal value between 0 and 511.\\nYAML accepts both octal and decimal values, JSON requires decimal values for mode bits.\\nIf not specified, the volume defaultMode will be used.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"Mode\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"description\": \"Required: Path is  the relative path name of the file to be created. Must not be absolute or contain the '..' path. Must be utf-8 encoded. The first item of the relative path must not start with '..'\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Path\"\n" +
		"        },\n" +
		"        \"resourceFieldRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ResourceFieldSelector\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1DownwardAPIVolumeSource\": {\n" +
		"      \"description\": \"Downward API volumes support ownership management and SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"DownwardAPIVolumeSource represents a volume containing downward API info.\",\n" +
		"      \"properties\": {\n" +
		"        \"defaultMode\": {\n" +
		"          \"description\": \"Optional: mode bits to use on created files by default. Must be a\\nOptional: mode bits used to set permissions on created files by default.\\nMust be an octal value between 0000 and 0777 or a decimal value between 0 and 511.\\nYAML accepts both octal and decimal values, JSON requires decimal values for mode bits.\\nDefaults to 0644.\\nDirectories within the path are not affected by this setting.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"DefaultMode\"\n" +
		"        },\n" +
		"        \"items\": {\n" +
		"          \"description\": \"Items is a list of downward API volume file\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1DownwardAPIVolumeFile\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Items\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1EmptyDirVolumeSource\": {\n" +
		"      \"description\": \"Empty directory volumes support ownership management and SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents an empty directory for a pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"medium\": {\n" +
		"          \"$ref\": \"#/definitions/v1StorageMedium\"\n" +
		"        },\n" +
		"        \"sizeLimit\": {\n" +
		"          \"$ref\": \"#/definitions/resourceQuantity\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1EnvFromSource\": {\n" +
		"      \"description\": \"EnvFromSource represents the source of a set of ConfigMaps\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"configMapRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ConfigMapEnvSource\"\n" +
		"        },\n" +
		"        \"prefix\": {\n" +
		"          \"description\": \"An optional identifier to prepend to each key in the ConfigMap. Must be a C_IDENTIFIER.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Prefix\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1SecretEnvSource\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1EnvVar\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"EnvVar represents an environment variable present in a Container.\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the environment variable. Must be a C_IDENTIFIER.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"description\": \"Variable references $(VAR_NAME) are expanded\\nusing the previously defined environment variables in the container and\\nany service environment variables. If a variable cannot be resolved,\\nthe reference in the input string will be unchanged. Double $$ are reduced\\nto a single $, which allows for escaping the $(VAR_NAME) syntax: i.e.\\n\\\"$$(VAR_NAME)\\\" will produce the string literal \\\"$(VAR_NAME)\\\".\\nEscaped references will never be expanded, regardless of whether the variable\\nexists or not.\\nDefaults to \\\"\\\".\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Value\"\n" +
		"        },\n" +
		"        \"valueFrom\": {\n" +
		"          \"$ref\": \"#/definitions/v1EnvVarSource\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1EnvVarSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"EnvVarSource represents a source for the value of an EnvVar.\",\n" +
		"      \"properties\": {\n" +
		"        \"configMapKeyRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ConfigMapKeySelector\"\n" +
		"        },\n" +
		"        \"fieldRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ObjectFieldSelector\"\n" +
		"        },\n" +
		"        \"resourceFieldRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1ResourceFieldSelector\"\n" +
		"        },\n" +
		"        \"secretKeyRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1SecretKeySelector\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1EphemeralContainer\": {\n" +
		"      \"description\": \"An EphemeralContainer is a container that may be added temporarily to an existing pod for\\nuser-initiated activities such as debugging. Ephemeral containers have no resource or\\nscheduling guarantees, and they will not be restarted when they exit or when a pod is\\nremoved or restarted. If an ephemeral container causes a pod to exceed its resource\\nallocation, the pod may be evicted.\\nEphemeral containers may not be added by directly updating the pod spec. They must be added\\nvia the pod's ephemeralcontainers subresource, and they will appear in the pod spec\\nonce added.\\nThis is an alpha feature enabled by the EphemeralContainers feature flag.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"args\": {\n" +
		"          \"description\": \"Arguments to the entrypoint.\\nThe docker image's CMD is used if this is not provided.\\nVariable references $(VAR_NAME) are expanded using the container's environment. If a variable\\ncannot be resolved, the reference in the input string will be unchanged. Double $$ are reduced\\nto a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. \\\"$$(VAR_NAME)\\\" will\\nproduce the string literal \\\"$(VAR_NAME)\\\". Escaped references will never be expanded, regardless\\nof whether the variable exists or not. Cannot be updated.\\nMore info: https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Args\"\n" +
		"        },\n" +
		"        \"command\": {\n" +
		"          \"description\": \"Entrypoint array. Not executed within a shell.\\nThe docker image's ENTRYPOINT is used if this is not provided.\\nVariable references $(VAR_NAME) are expanded using the container's environment. If a variable\\ncannot be resolved, the reference in the input string will be unchanged. Double $$ are reduced\\nto a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. \\\"$$(VAR_NAME)\\\" will\\nproduce the string literal \\\"$(VAR_NAME)\\\". Escaped references will never be expanded, regardless\\nof whether the variable exists or not. Cannot be updated.\\nMore info: https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Command\"\n" +
		"        },\n" +
		"        \"env\": {\n" +
		"          \"description\": \"List of environment variables to set in the container.\\nCannot be updated.\\n+optional\\n+patchMergeKey=name\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1EnvVar\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Env\"\n" +
		"        },\n" +
		"        \"envFrom\": {\n" +
		"          \"description\": \"List of sources to populate environment variables in the container.\\nThe keys defined within a source must be a C_IDENTIFIER. All invalid keys\\nwill be reported as an event when the container is starting. When a key exists in multiple\\nsources, the value associated with the last source will take precedence.\\nValues defined by an Env with a duplicate key will take precedence.\\nCannot be updated.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1EnvFromSource\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"EnvFrom\"\n" +
		"        },\n" +
		"        \"image\": {\n" +
		"          \"description\": \"Docker image name.\\nMore info: https://kubernetes.io/docs/concepts/containers/images\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Image\"\n" +
		"        },\n" +
		"        \"imagePullPolicy\": {\n" +
		"          \"$ref\": \"#/definitions/v1PullPolicy\"\n" +
		"        },\n" +
		"        \"lifecycle\": {\n" +
		"          \"$ref\": \"#/definitions/v1Lifecycle\"\n" +
		"        },\n" +
		"        \"livenessProbe\": {\n" +
		"          \"$ref\": \"#/definitions/v1Probe\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the ephemeral container specified as a DNS_LABEL.\\nThis name must be unique among all containers, init containers and ephemeral containers.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"ports\": {\n" +
		"          \"description\": \"Ports are not allowed for ephemeral containers.\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1ContainerPort\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Ports\"\n" +
		"        },\n" +
		"        \"readinessProbe\": {\n" +
		"          \"$ref\": \"#/definitions/v1Probe\"\n" +
		"        },\n" +
		"        \"resources\": {\n" +
		"          \"$ref\": \"#/definitions/v1ResourceRequirements\"\n" +
		"        },\n" +
		"        \"securityContext\": {\n" +
		"          \"$ref\": \"#/definitions/v1SecurityContext\"\n" +
		"        },\n" +
		"        \"startupProbe\": {\n" +
		"          \"$ref\": \"#/definitions/v1Probe\"\n" +
		"        },\n" +
		"        \"stdin\": {\n" +
		"          \"description\": \"Whether this container should allocate a buffer for stdin in the container runtime. If this\\nis not set, reads from stdin in the container will always result in EOF.\\nDefault is false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Stdin\"\n" +
		"        },\n" +
		"        \"stdinOnce\": {\n" +
		"          \"description\": \"Whether the container runtime should close the stdin channel after it has been opened by\\na single attach. When stdin is true the stdin stream will remain open across multiple attach\\nsessions. If stdinOnce is set to true, stdin is opened on container start, is empty until the\\nfirst client attaches to stdin, and then remains open and accepts data until the client disconnects,\\nat which time stdin is closed and remains closed until the container is restarted. If this\\nflag is false, a container processes that reads from stdin will never receive an EOF.\\nDefault is false\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"StdinOnce\"\n" +
		"        },\n" +
		"        \"targetContainerName\": {\n" +
		"          \"description\": \"If set, the name of the container from PodSpec that this ephemeral container targets.\\nThe ephemeral container will be run in the namespaces (IPC, PID, etc) of this container.\\nIf not set then the ephemeral container is run in whatever namespaces are shared\\nfor the pod. Note that the container runtime must support this feature.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"TargetContainerName\"\n" +
		"        },\n" +
		"        \"terminationMessagePath\": {\n" +
		"          \"description\": \"Optional: Path at which the file to which the container's termination message\\nwill be written is mounted into the container's filesystem.\\nMessage written is intended to be brief final status, such as an assertion failure message.\\nWill be truncated by the node if greater than 4096 bytes. The total message length across\\nall containers will be limited to 12kb.\\nDefaults to /dev/termination-log.\\nCannot be updated.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"TerminationMessagePath\"\n" +
		"        },\n" +
		"        \"terminationMessagePolicy\": {\n" +
		"          \"$ref\": \"#/definitions/v1TerminationMessagePolicy\"\n" +
		"        },\n" +
		"        \"tty\": {\n" +
		"          \"description\": \"Whether this container should allocate a TTY for itself, also requires 'stdin' to be true.\\nDefault is false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"TTY\"\n" +
		"        },\n" +
		"        \"volumeDevices\": {\n" +
		"          \"description\": \"volumeDevices is the list of block devices to be used by the container.\\n+patchMergeKey=devicePath\\n+patchStrategy=merge\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1VolumeDevice\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"VolumeDevices\"\n" +
		"        },\n" +
		"        \"volumeMounts\": {\n" +
		"          \"description\": \"Pod volumes to mount into the container's filesystem.\\nCannot be updated.\\n+optional\\n+patchMergeKey=mountPath\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1VolumeMount\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"VolumeMounts\"\n" +
		"        },\n" +
		"        \"workingDir\": {\n" +
		"          \"description\": \"Container's working directory.\\nIf not specified, the container runtime's default will be used, which\\nmight be configured in the container image.\\nCannot be updated.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"WorkingDir\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1EphemeralVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents an ephemeral volume that is handled by a normal storage driver.\",\n" +
		"      \"properties\": {\n" +
		"        \"volumeClaimTemplate\": {\n" +
		"          \"$ref\": \"#/definitions/v1PersistentVolumeClaimTemplate\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ExecAction\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"ExecAction describes a \\\"run in container\\\" action.\",\n" +
		"      \"properties\": {\n" +
		"        \"command\": {\n" +
		"          \"description\": \"Command is the command line to execute inside the container, the working directory for the\\ncommand  is root ('/') in the container's filesystem. The command is simply exec'd, it is\\nnot run inside a shell, so traditional shell instructions ('|', etc) won't work. To use\\na shell, you need to explicitly call out to that shell.\\nExit status of 0 is treated as live/healthy and non-zero is unhealthy.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Command\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1FCVolumeSource\": {\n" +
		"      \"description\": \"Fibre Channel volumes can only be mounted as read/write once.\\nFibre Channel volumes support ownership management and SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a Fibre Channel volume.\",\n" +
		"      \"properties\": {\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nTODO: how do we prevent errors in the filesystem from compromising the machine\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"lun\": {\n" +
		"          \"description\": \"Optional: FC target lun number\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"Lun\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Optional: Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"targetWWNs\": {\n" +
		"          \"description\": \"Optional: FC target worldwide names (WWNs)\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"TargetWWNs\"\n" +
		"        },\n" +
		"        \"wwids\": {\n" +
		"          \"description\": \"Optional: FC volume world wide identifiers (wwids)\\nEither wwids or combination of targetWWNs and lun must be set, but not both simultaneously.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"WWIDs\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1FieldsV1\": {\n" +
		"      \"description\": \"Each key is either a '.' representing the field itself, and will always map to an empty set,\\nor a string representing a sub-field or item. The string will follow one of these four formats:\\n'f:\\u003cname\\u003e', where \\u003cname\\u003e is the name of a field in a struct, or key in a map\\n'v:\\u003cvalue\\u003e', where \\u003cvalue\\u003e is the exact json formatted value of a list item\\n'i:\\u003cindex\\u003e', where \\u003cindex\\u003e is position of a item in a list\\n'k:\\u003ckeys\\u003e', where \\u003ckeys\\u003e is a map of  a list item's key fields to their unique values\\nIf a key maps to an empty Fields value, the field that key represents is part of the set.\\n\\nThe exact format is defined in sigs.k8s.io/structured-merge-diff\\n+protobuf.options.(gogoproto.goproto_stringer)=false\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"FieldsV1 stores a set of fields in a data structure like a Trie, in JSON format.\",\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/apis/meta/v1\"\n" +
		"    },\n" +
		"    \"v1FlexVolumeSource\": {\n" +
		"      \"description\": \"FlexVolume represents a generic volume resource that is\\nprovisioned/attached using an exec based plugin.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"driver\": {\n" +
		"          \"description\": \"Driver is the name of the driver to use for this volume.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Driver\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". The default filesystem depends on FlexVolume script.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"options\": {\n" +
		"          \"description\": \"Optional: Extra command options if any.\\n+optional\",\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Options\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Optional: Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1FlockerVolumeSource\": {\n" +
		"      \"description\": \"One and only one of datasetName and datasetUUID should be set.\\nFlocker volumes do not support ownership management or SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a Flocker volume mounted by the Flocker agent.\",\n" +
		"      \"properties\": {\n" +
		"        \"datasetName\": {\n" +
		"          \"description\": \"Name of the dataset stored as metadata -\\u003e name on the dataset for Flocker\\nshould be considered as deprecated\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"DatasetName\"\n" +
		"        },\n" +
		"        \"datasetUUID\": {\n" +
		"          \"description\": \"UUID of the dataset. This is unique identifier of a Flocker dataset\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"DatasetUUID\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1GCEPersistentDiskVolumeSource\": {\n" +
		"      \"description\": \"A GCE PD must exist before mounting to a container. The disk must\\nalso be in the same GCE project and zone as the kubelet. A GCE PD\\ncan only be mounted as read/write once or read-only many times. GCE\\nPDs support ownership management and SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a Persistent Disk resource in Google Compute Engine.\",\n" +
		"      \"properties\": {\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type of the volume that you want to mount.\\nTip: Ensure that the filesystem type is supported by the host operating system.\\nExamples: \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk\\nTODO: how do we prevent errors in the filesystem from compromising the machine\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"partition\": {\n" +
		"          \"description\": \"The partition in the volume that you want to mount.\\nIf omitted, the default is to mount by volume name.\\nExamples: For volume /dev/sda1, you specify the partition as \\\"1\\\".\\nSimilarly, the volume partition for /dev/sda is \\\"0\\\" (or you can leave the property empty).\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"Partition\"\n" +
		"        },\n" +
		"        \"pdName\": {\n" +
		"          \"description\": \"Unique name of the PD resource in GCE. Used to identify the disk in GCE.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"PDName\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"ReadOnly here will force the ReadOnly setting in VolumeMounts.\\nDefaults to false.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1GitRepoVolumeSource\": {\n" +
		"      \"description\": \"DEPRECATED: GitRepo is deprecated. To provision a container with a git repo, mount an\\nEmptyDir into an InitContainer that clones the repo using git, then mount the EmptyDir\\ninto the Pod's container.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a volume that is populated with the contents of a git repository.\\nGit repo volumes do not support ownership management.\\nGit repo volumes support SELinux relabeling.\",\n" +
		"      \"properties\": {\n" +
		"        \"directory\": {\n" +
		"          \"description\": \"Target directory name.\\nMust not contain or start with '..'.  If '.' is supplied, the volume directory will be the\\ngit repository.  Otherwise, if specified, the volume will contain the git repository in\\nthe subdirectory with the given name.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Directory\"\n" +
		"        },\n" +
		"        \"repository\": {\n" +
		"          \"description\": \"Repository URL\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Repository\"\n" +
		"        },\n" +
		"        \"revision\": {\n" +
		"          \"description\": \"Commit hash for the specified revision.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Revision\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1GlusterfsVolumeSource\": {\n" +
		"      \"description\": \"Glusterfs volumes do not support ownership management or SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a Glusterfs mount that lasts the lifetime of a pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"endpoints\": {\n" +
		"          \"description\": \"EndpointsName is the endpoint name that details Glusterfs topology.\\nMore info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"EndpointsName\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"description\": \"Path is the Glusterfs volume path.\\nMore info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Path\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"ReadOnly here will force the Glusterfs volume to be mounted with read-only permissions.\\nDefaults to false.\\nMore info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1HTTPGetAction\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"HTTPGetAction describes an action based on HTTP Get requests.\",\n" +
		"      \"properties\": {\n" +
		"        \"host\": {\n" +
		"          \"description\": \"Host name to connect to, defaults to the pod IP. You probably want to set\\n\\\"Host\\\" in httpHeaders instead.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Host\"\n" +
		"        },\n" +
		"        \"httpHeaders\": {\n" +
		"          \"description\": \"Custom headers to set in the request. HTTP allows repeated headers.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1HTTPHeader\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"HTTPHeaders\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"description\": \"Path to access on the HTTP server.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Path\"\n" +
		"        },\n" +
		"        \"port\": {\n" +
		"          \"$ref\": \"#/definitions/intstrIntOrString\"\n" +
		"        },\n" +
		"        \"scheme\": {\n" +
		"          \"$ref\": \"#/definitions/v1URIScheme\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1HTTPHeader\": {\n" +
		"      \"description\": \"HTTPHeader describes a custom header to be used in HTTP probes\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"description\": \"The header field name\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"description\": \"The header field value\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Value\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1HTTPIngressPath\": {\n" +
		"      \"description\": \"HTTPIngressPath associates a path with a backend. Incoming urls matching the\\npath are forwarded to the backend.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"backend\": {\n" +
		"          \"description\": \"Backend defines the referenced service endpoint to which the traffic\\nwill be forwarded to.\",\n" +
		"          \"$ref\": \"#/definitions/v1IngressBackend\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Path is matched against the path of an incoming request. Currently it can\\ncontain characters disallowed from the conventional \\\"path\\\" part of a URL\\nas defined by RFC 3986. Paths must begin with a '/' and must be present\\nwhen using PathType with value \\\"Exact\\\" or \\\"Prefix\\\".\\n+optional\"\n" +
		"        },\n" +
		"        \"pathType\": {\n" +
		"          \"description\": \"PathType determines the interpretation of the Path matching. PathType can\\nbe one of the following values:\\n* Exact: Matches the URL path exactly.\\n* Prefix: Matches based on a URL path prefix split by '/'. Matching is\\n  done on a path element by element basis. A path element refers is the\\n  list of labels in the path split by the '/' separator. A request is a\\n  match for path p if every p is an element-wise prefix of p of the\\n  request path. Note that if the last element of the path is a substring\\n  of the last element in request path, it is not a match (e.g. /foo/bar\\n  matches /foo/bar/baz, but does not match /foo/barbaz).\\n* ImplementationSpecific: Interpretation of the Path matching is up to\\n  the IngressClass. Implementations can treat this as a separate PathType\\n  or treat it identically to Prefix or Exact path types.\\nImplementations are required to support all path types.\",\n" +
		"          \"type\": \"string\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1HTTPIngressRuleValue\": {\n" +
		"      \"description\": \"HTTPIngressRuleValue is a list of http selectors pointing to backends.\\nIn the example: http://\\u003chost\\u003e/\\u003cpath\\u003e?\\u003csearchpart\\u003e -\\u003e backend where\\nwhere parts of the url correspond to RFC 3986, this resource will be used\\nto match against everything after the last '/' and before the first '?'\\nor '#'.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"paths\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"title\": \"A collection of paths that map requests to backends.\\n+listType=atomic\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1HTTPIngressPath\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1Handler\": {\n" +
		"      \"description\": \"Handler defines a specific action that should be taken\\nTODO: pass structured data to these actions, and document that data here.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"exec\": {\n" +
		"          \"$ref\": \"#/definitions/v1ExecAction\"\n" +
		"        },\n" +
		"        \"httpGet\": {\n" +
		"          \"$ref\": \"#/definitions/v1HTTPGetAction\"\n" +
		"        },\n" +
		"        \"tcpSocket\": {\n" +
		"          \"$ref\": \"#/definitions/v1TCPSocketAction\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1HostAlias\": {\n" +
		"      \"description\": \"HostAlias holds the mapping between IP and hostnames that will be injected as an entry in the\\npod's hosts file.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"hostnames\": {\n" +
		"          \"description\": \"Hostnames for the above IP address.\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Hostnames\"\n" +
		"        },\n" +
		"        \"ip\": {\n" +
		"          \"description\": \"IP address of the host file entry.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"IP\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1HostPathType\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1HostPathVolumeSource\": {\n" +
		"      \"description\": \"Host path volumes do not support ownership management or SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a host path mapped into a pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"path\": {\n" +
		"          \"description\": \"Path of the directory on the host.\\nIf the path is a symlink, it will follow the link to the real path.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#hostpath\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Path\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"$ref\": \"#/definitions/v1HostPathType\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ISCSIVolumeSource\": {\n" +
		"      \"description\": \"ISCSI volumes can only be mounted as read/write once.\\nISCSI volumes support ownership management and SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents an ISCSI disk.\",\n" +
		"      \"properties\": {\n" +
		"        \"chapAuthDiscovery\": {\n" +
		"          \"description\": \"whether support iSCSI Discovery CHAP authentication\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"DiscoveryCHAPAuth\"\n" +
		"        },\n" +
		"        \"chapAuthSession\": {\n" +
		"          \"description\": \"whether support iSCSI Session CHAP authentication\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"SessionCHAPAuth\"\n" +
		"        },\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type of the volume that you want to mount.\\nTip: Ensure that the filesystem type is supported by the host operating system.\\nExamples: \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#iscsi\\nTODO: how do we prevent errors in the filesystem from compromising the machine\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"initiatorName\": {\n" +
		"          \"description\": \"Custom iSCSI Initiator Name.\\nIf initiatorName is specified with iscsiInterface simultaneously, new iSCSI interface\\n\\u003ctarget portal\\u003e:\\u003cvolume name\\u003e will be created for the connection.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"InitiatorName\"\n" +
		"        },\n" +
		"        \"iqn\": {\n" +
		"          \"description\": \"Target iSCSI Qualified Name.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"IQN\"\n" +
		"        },\n" +
		"        \"iscsiInterface\": {\n" +
		"          \"description\": \"iSCSI Interface Name that uses an iSCSI transport.\\nDefaults to 'default' (tcp).\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"ISCSIInterface\"\n" +
		"        },\n" +
		"        \"lun\": {\n" +
		"          \"description\": \"iSCSI Target Lun number.\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"Lun\"\n" +
		"        },\n" +
		"        \"portals\": {\n" +
		"          \"description\": \"iSCSI Target Portal List. The portal is either an IP or ip_addr:port if the port\\nis other than default (typically TCP ports 860 and 3260).\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Portals\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"ReadOnly here will force the ReadOnly setting in VolumeMounts.\\nDefaults to false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"        },\n" +
		"        \"targetPortal\": {\n" +
		"          \"description\": \"iSCSI Target Portal. The Portal is either an IP or ip_addr:port if the port\\nis other than default (typically TCP ports 860 and 3260).\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"TargetPortal\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1Ingress\": {\n" +
		"      \"description\": \"Ingress is a collection of rules that allow inbound connections to reach the\\nendpoints defined by a backend. An Ingress can be configured to give services\\nexternally-reachable urls, load balance traffic, terminate SSL, offer name\\nbased virtual hosting etc.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"metadata\": {\n" +
		"          \"title\": \"Standard object's metadata.\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1ObjectMeta\"\n" +
		"        },\n" +
		"        \"spec\": {\n" +
		"          \"title\": \"Spec is the desired state of the Ingress.\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1IngressSpec\"\n" +
		"        },\n" +
		"        \"status\": {\n" +
		"          \"title\": \"Status is the current state of the Ingress.\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1IngressStatus\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1IngressBackend\": {\n" +
		"      \"description\": \"IngressBackend describes all endpoints for a given service and port.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"resource\": {\n" +
		"          \"title\": \"Resource is an ObjectRef to another Kubernetes resource in the namespace\\nof the Ingress object. If resource is specified, a service.Name and\\nservice.Port must not be specified.\\nThis is a mutually exclusive setting with \\\"Service\\\".\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1TypedLocalObjectReference\"\n" +
		"        },\n" +
		"        \"service\": {\n" +
		"          \"title\": \"Service references a Service as a Backend.\\nThis is a mutually exclusive setting with \\\"Resource\\\".\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1IngressServiceBackend\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1IngressRule\": {\n" +
		"      \"description\": \"IngressRule represents the rules mapping the paths under a specified host to\\nthe related backend services. Incoming requests are first evaluated for a host\\nmatch, then routed to the backend associated with the matching IngressRuleValue.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"host\": {\n" +
		"          \"description\": \"Host is the fully qualified domain name of a network host, as defined by RFC 3986.\\nNote the following deviations from the \\\"host\\\" part of the\\nURI as defined in RFC 3986:\\n1. IPs are not allowed. Currently an IngressRuleValue can only apply to\\n   the IP in the Spec of the parent Ingress.\\n2. The `:` delimiter is not respected because ports are not allowed.\\n\\t  Currently the port of an Ingress is implicitly :80 for http and\\n\\t  :443 for https.\\nBoth these may change in the future.\\nIncoming requests are matched against the host before the\\nIngressRuleValue. If the host is unspecified, the Ingress routes all\\ntraffic based on the specified IngressRuleValue.\\n\\nHost can be \\\"precise\\\" which is a domain name without the terminating dot of\\na network host (e.g. \\\"foo.bar.com\\\") or \\\"wildcard\\\", which is a domain name\\nprefixed with a single wildcard label (e.g. \\\"*.foo.com\\\").\\nThe wildcard character '*' must appear by itself as the first DNS label and\\nmatches only a single label. You cannot have a wildcard label by itself (e.g. Host == \\\"*\\\").\\nRequests will be matched against the Host field in the following way:\\n1. If Host is precise, the request matches this rule if the http host header is equal to Host.\\n2. If Host is a wildcard, then the request matches this rule if the http host header\\nis to equal to the suffix (removing the first label) of the wildcard rule.\\n+optional\",\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"ingressRuleValue\": {\n" +
		"          \"title\": \"IngressRuleValue represents a rule to route requests for this IngressRule.\\nIf unspecified, the rule defaults to a http catch-all. Whether that sends\\njust traffic matching the host to the default backend or all traffic to the\\ndefault backend, is left to the controller fulfilling the Ingress. Http is\\ncurrently the only supported IngressRuleValue.\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1IngressRuleValue\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1IngressRuleValue\": {\n" +
		"      \"description\": \"IngressRuleValue represents a rule to apply against incoming requests. If the\\nrule is satisfied, the request is routed to the specified backend. Currently\\nmixing different types of rules in a single Ingress is disallowed, so exactly\\none of the following must be set.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"http\": {\n" +
		"          \"title\": \"+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1HTTPIngressRuleValue\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1IngressServiceBackend\": {\n" +
		"      \"description\": \"IngressServiceBackend references a Kubernetes Service as a Backend.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name is the referenced service. The service must exist in\\nthe same namespace as the Ingress object.\",\n" +
		"          \"type\": \"string\"\n" +
		"        },\n" +
		"        \"port\": {\n" +
		"          \"description\": \"Port of the referenced service. A port name or port number\\nis required for a IngressServiceBackend.\",\n" +
		"          \"$ref\": \"#/definitions/v1ServiceBackendPort\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1IngressSpec\": {\n" +
		"      \"description\": \"IngressSpec describes the Ingress the user wishes to exist.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"defaultBackend\": {\n" +
		"          \"title\": \"DefaultBackend is the backend that should handle requests that don't\\nmatch any rule. If Rules are not specified, DefaultBackend must be specified.\\nIf DefaultBackend is not set, the handling of requests that do not match any\\nof the rules will be up to the Ingress controller.\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1IngressBackend\"\n" +
		"        },\n" +
		"        \"ingressClassName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"IngressClassName is the name of the IngressClass cluster resource. The\\nassociated IngressClass defines which controller will implement the\\nresource. This replaces the deprecated `kubernetes.io/ingress.class`\\nannotation. For backwards compatibility, when that annotation is set, it\\nmust be given precedence over this field. The controller may emit a\\nwarning if the field and annotation have different values.\\nImplementations of this API should ignore Ingresses without a class\\nspecified. An IngressClass resource may be marked as default, which can\\nbe used to set a default value for this field. For more information,\\nrefer to the IngressClass documentation.\\n+optional\"\n" +
		"        },\n" +
		"        \"rules\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"title\": \"A list of host rules used to configure the Ingress. If unspecified, or\\nno rule matches, all traffic is sent to the default backend.\\n+listType=atomic\\n+optional\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1IngressRule\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"tls\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"title\": \"TLS configuration. Currently the Ingress only supports a single TLS\\nport, 443. If multiple members of this list specify different hosts, they\\nwill be multiplexed on the same port according to the hostname specified\\nthrough the SNI TLS extension, if the ingress controller fulfilling the\\ningress supports SNI.\\n+listType=atomic\\n+optional\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1IngressTLS\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1IngressStatus\": {\n" +
		"      \"description\": \"IngressStatus describe the current state of the Ingress.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"loadBalancer\": {\n" +
		"          \"title\": \"LoadBalancer contains the current status of the load-balancer.\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1LoadBalancerStatus\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1IngressTLS\": {\n" +
		"      \"description\": \"IngressTLS describes the transport layer security associated with an Ingress.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"hosts\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"title\": \"Hosts are a list of hosts included in the TLS certificate. The values in\\nthis list must match the name/s used in the tlsSecret. Defaults to the\\nwildcard host setting for the loadbalancer controller fulfilling this\\nIngress, if left unspecified.\\n+listType=atomic\\n+optional\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"secretName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"SecretName is the name of the secret used to terminate TLS traffic on\\nport 443. Field is left optional to allow TLS routing based on SNI\\nhostname alone. If the SNI host in a listener conflicts with the \\\"Host\\\"\\nheader field used by an IngressRule, the SNI host is used for termination\\nand value of the Host header is used for routing.\\n+optional\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1KeyToPath\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Maps a string key to a path within a volume.\",\n" +
		"      \"properties\": {\n" +
		"        \"key\": {\n" +
		"          \"description\": \"The key to project.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Key\"\n" +
		"        },\n" +
		"        \"mode\": {\n" +
		"          \"description\": \"Optional: mode bits used to set permissions on this file.\\nMust be an octal value between 0000 and 0777 or a decimal value between 0 and 511.\\nYAML accepts both octal and decimal values, JSON requires decimal values for mode bits.\\nIf not specified, the volume defaultMode will be used.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"Mode\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"description\": \"The relative path of the file to map the key to.\\nMay not be an absolute path.\\nMay not contain the path element '..'.\\nMay not start with the string '..'.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Path\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1LabelSelector\": {\n" +
		"      \"description\": \"A label selector is a label query over a set of resources. The result of matchLabels and\\nmatchExpressions are ANDed. An empty label selector matches all objects. A null\\nlabel selector matches no objects.\\n+structType=atomic\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"matchExpressions\": {\n" +
		"          \"description\": \"matchExpressions is a list of label selector requirements. The requirements are ANDed.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1LabelSelectorRequirement\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"MatchExpressions\"\n" +
		"        },\n" +
		"        \"matchLabels\": {\n" +
		"          \"description\": \"matchLabels is a map of {key,value} pairs. A single {key,value} in the matchLabels\\nmap is equivalent to an element of matchExpressions, whose key field is \\\"key\\\", the\\noperator is \\\"In\\\", and the values array contains only \\\"value\\\". The requirements are ANDed.\\n+optional\",\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"MatchLabels\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/apis/meta/v1\"\n" +
		"    },\n" +
		"    \"v1LabelSelectorOperator\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"A label selector operator is the set of operators that can be used in a selector requirement.\",\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/apis/meta/v1\"\n" +
		"    },\n" +
		"    \"v1LabelSelectorRequirement\": {\n" +
		"      \"description\": \"A label selector requirement is a selector that contains values, a key, and an operator that\\nrelates the key and values.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"key\": {\n" +
		"          \"description\": \"key is the label key that the selector applies to.\\n+patchMergeKey=key\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Key\"\n" +
		"        },\n" +
		"        \"operator\": {\n" +
		"          \"$ref\": \"#/definitions/v1LabelSelectorOperator\"\n" +
		"        },\n" +
		"        \"values\": {\n" +
		"          \"description\": \"values is an array of string values. If the operator is In or NotIn,\\nthe values array must be non-empty. If the operator is Exists or DoesNotExist,\\nthe values array must be empty. This array is replaced during a strategic\\nmerge patch.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Values\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/apis/meta/v1\"\n" +
		"    },\n" +
		"    \"v1Lifecycle\": {\n" +
		"      \"description\": \"Lifecycle describes actions that the management system should take in response to container lifecycle\\nevents. For the PostStart and PreStop lifecycle handlers, management of the container blocks\\nuntil the action is complete, unless the container process fails, in which case the handler is aborted.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"postStart\": {\n" +
		"          \"$ref\": \"#/definitions/v1Handler\"\n" +
		"        },\n" +
		"        \"preStop\": {\n" +
		"          \"$ref\": \"#/definitions/v1Handler\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1LoadBalancerIngress\": {\n" +
		"      \"description\": \"LoadBalancerIngress represents the status of a load-balancer ingress point:\\ntraffic intended for the service should be sent to an ingress point.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"hostname\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Hostname is set for load-balancer ingress points that are DNS based\\n(typically AWS load-balancers)\\n+optional\"\n" +
		"        },\n" +
		"        \"ip\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"IP is set for load-balancer ingress points that are IP based\\n(typically GCE or OpenStack load-balancers)\\n+optional\"\n" +
		"        },\n" +
		"        \"ports\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"title\": \"Ports is a list of records of service ports\\nIf used, every port defined in the service should have an entry in it\\n+listType=atomic\\n+optional\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PortStatus\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1LoadBalancerStatus\": {\n" +
		"      \"description\": \"LoadBalancerStatus represents the status of a load-balancer.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"ingress\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"title\": \"Ingress is a list containing ingress points for the load-balancer.\\nTraffic intended for the service should be sent to these ingress points.\\n+optional\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1LoadBalancerIngress\"\n" +
		"          }\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1LocalObjectReference\": {\n" +
		"      \"description\": \"LocalObjectReference contains enough information to let you locate the\\nreferenced object inside the same namespace.\\n+structType=atomic\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the referent.\\nMore info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names\\nTODO: Add other useful fields. apiVersion, kind, uid?\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ManagedFieldsEntry\": {\n" +
		"      \"description\": \"ManagedFieldsEntry is a workflow-id, a FieldSet and the group version of the resource\\nthat the fieldset applies to.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"apiVersion\": {\n" +
		"          \"description\": \"APIVersion defines the version of this resource that this field set\\napplies to. The format is \\\"group/version\\\" just like the top-level\\nAPIVersion field. It is necessary to track the version of a field\\nset because it cannot be automatically converted.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"APIVersion\"\n" +
		"        },\n" +
		"        \"fieldsType\": {\n" +
		"          \"description\": \"FieldsType is the discriminator for the different fields format and version.\\nThere is currently only one possible value: \\\"FieldsV1\\\"\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FieldsType\"\n" +
		"        },\n" +
		"        \"fieldsV1\": {\n" +
		"          \"$ref\": \"#/definitions/v1FieldsV1\"\n" +
		"        },\n" +
		"        \"manager\": {\n" +
		"          \"description\": \"Manager is an identifier of the workflow managing these fields.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Manager\"\n" +
		"        },\n" +
		"        \"operation\": {\n" +
		"          \"$ref\": \"#/definitions/v1ManagedFieldsOperationType\"\n" +
		"        },\n" +
		"        \"subresource\": {\n" +
		"          \"description\": \"Subresource is the name of the subresource used to update that object, or\\nempty string if the object was updated through the main resource. The\\nvalue of this field is used to distinguish between managers, even if they\\nshare the same name. For example, a status update will be distinct from a\\nregular update using the same manager name.\\nNote that the APIVersion field is not related to the Subresource field and\\nit always corresponds to the version of the main resource.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Subresource\"\n" +
		"        },\n" +
		"        \"time\": {\n" +
		"          \"$ref\": \"#/definitions/v1Time\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/apis/meta/v1\"\n" +
		"    },\n" +
		"    \"v1ManagedFieldsOperationType\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"ManagedFieldsOperationType is the type of operation which lead to a ManagedFieldsEntry being created.\",\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/apis/meta/v1\"\n" +
		"    },\n" +
		"    \"v1MountPropagationMode\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"MountPropagationMode describes mount propagation.\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1NFSVolumeSource\": {\n" +
		"      \"description\": \"NFS volumes do not support ownership management or SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents an NFS mount that lasts the lifetime of a pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"path\": {\n" +
		"          \"description\": \"Path that is exported by the NFS server.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#nfs\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Path\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"ReadOnly here will force\\nthe NFS export to be mounted with read-only permissions.\\nDefaults to false.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#nfs\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"server\": {\n" +
		"          \"description\": \"Server is the hostname or IP address of the NFS server.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#nfs\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Server\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1NodeAffinity\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Node affinity is a group of node affinity scheduling rules.\",\n" +
		"      \"properties\": {\n" +
		"        \"preferredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"description\": \"The scheduler will prefer to schedule pods to nodes that satisfy\\nthe affinity expressions specified by this field, but it may choose\\na node that violates one or more of the expressions. The node that is\\nmost preferred is the one with the greatest sum of weights, i.e.\\nfor each node that meets all of the scheduling requirements (resource\\nrequest, requiredDuringScheduling affinity expressions, etc.),\\ncompute a sum by iterating through the elements of this field and adding\\n\\\"weight\\\" to the sum if the node matches the corresponding matchExpressions; the\\nnode(s) with the highest sum are the most preferred.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PreferredSchedulingTerm\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"PreferredDuringSchedulingIgnoredDuringExecution\"\n" +
		"        },\n" +
		"        \"requiredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"$ref\": \"#/definitions/v1NodeSelector\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1NodeSelector\": {\n" +
		"      \"description\": \"A node selector represents the union of the results of one or more label queries\\nover a set of nodes; that is, it represents the OR of the selectors represented\\nby the node selector terms.\\n+structType=atomic\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"nodeSelectorTerms\": {\n" +
		"          \"description\": \"Required. A list of node selector terms. The terms are ORed.\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1NodeSelectorTerm\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"NodeSelectorTerms\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1NodeSelectorOperator\": {\n" +
		"      \"description\": \"A node selector operator is the set of operators that can be used in\\na node selector requirement.\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1NodeSelectorRequirement\": {\n" +
		"      \"description\": \"A node selector requirement is a selector that contains values, a key, and an operator\\nthat relates the key and values.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"key\": {\n" +
		"          \"description\": \"The label key that the selector applies to.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Key\"\n" +
		"        },\n" +
		"        \"operator\": {\n" +
		"          \"$ref\": \"#/definitions/v1NodeSelectorOperator\"\n" +
		"        },\n" +
		"        \"values\": {\n" +
		"          \"description\": \"An array of string values. If the operator is In or NotIn,\\nthe values array must be non-empty. If the operator is Exists or DoesNotExist,\\nthe values array must be empty. If the operator is Gt or Lt, the values\\narray must have a single element, which will be interpreted as an integer.\\nThis array is replaced during a strategic merge patch.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Values\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1NodeSelectorTerm\": {\n" +
		"      \"description\": \"A null or empty node selector term matches no objects. The requirements of\\nthem are ANDed.\\nThe TopologySelectorTerm type implements a subset of the NodeSelectorTerm.\\n+structType=atomic\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"matchExpressions\": {\n" +
		"          \"description\": \"A list of node selector requirements by node's labels.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1NodeSelectorRequirement\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"MatchExpressions\"\n" +
		"        },\n" +
		"        \"matchFields\": {\n" +
		"          \"description\": \"A list of node selector requirements by node's fields.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1NodeSelectorRequirement\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"MatchFields\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ObjectFieldSelector\": {\n" +
		"      \"description\": \"+structType=atomic\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"ObjectFieldSelector selects an APIVersioned field of an object.\",\n" +
		"      \"properties\": {\n" +
		"        \"apiVersion\": {\n" +
		"          \"description\": \"Version of the schema the FieldPath is written in terms of, defaults to \\\"v1\\\".\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"APIVersion\"\n" +
		"        },\n" +
		"        \"fieldPath\": {\n" +
		"          \"description\": \"Path of the field to select in the specified API version.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FieldPath\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ObjectMeta\": {\n" +
		"      \"description\": \"ObjectMeta is metadata that all persisted resources must have, which includes all objects\\nusers must create.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"annotations\": {\n" +
		"          \"description\": \"Annotations is an unstructured key value map stored with a resource that may be\\nset by external tools to store and retrieve arbitrary metadata. They are not\\nqueryable and should be preserved when modifying objects.\\nMore info: http://kubernetes.io/docs/user-guide/annotations\\n+optional\",\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Annotations\"\n" +
		"        },\n" +
		"        \"clusterName\": {\n" +
		"          \"description\": \"The name of the cluster which the object belongs to.\\nThis is used to distinguish resources with same name and namespace in different clusters.\\nThis field is not set anywhere right now and apiserver is going to ignore it if set in create or update request.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"ClusterName\"\n" +
		"        },\n" +
		"        \"creationTimestamp\": {\n" +
		"          \"$ref\": \"#/definitions/v1Time\"\n" +
		"        },\n" +
		"        \"deletionGracePeriodSeconds\": {\n" +
		"          \"description\": \"Number of seconds allowed for this object to gracefully terminate before\\nit will be removed from the system. Only set when deletionTimestamp is also set.\\nMay only be shortened.\\nRead-only.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"DeletionGracePeriodSeconds\"\n" +
		"        },\n" +
		"        \"deletionTimestamp\": {\n" +
		"          \"$ref\": \"#/definitions/v1Time\"\n" +
		"        },\n" +
		"        \"finalizers\": {\n" +
		"          \"description\": \"Must be empty before the object is deleted from the registry. Each entry\\nis an identifier for the responsible component that will remove the entry\\nfrom the list. If the deletionTimestamp of the object is non-nil, entries\\nin this list can only be removed.\\nFinalizers may be processed and removed in any order.  Order is NOT enforced\\nbecause it introduces significant risk of stuck finalizers.\\nfinalizers is a shared field, any actor with permission can reorder it.\\nIf the finalizer list is processed in order, then this can lead to a situation\\nin which the component responsible for the first finalizer in the list is\\nwaiting for a signal (field value, external system, or other) produced by a\\ncomponent responsible for a finalizer later in the list, resulting in a deadlock.\\nWithout enforced ordering finalizers are free to order amongst themselves and\\nare not vulnerable to ordering changes in the list.\\n+optional\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Finalizers\"\n" +
		"        },\n" +
		"        \"generateName\": {\n" +
		"          \"description\": \"GenerateName is an optional prefix, used by the server, to generate a unique\\nname ONLY IF the Name field has not been provided.\\nIf this field is used, the name returned to the client will be different\\nthan the name passed. This value will also be combined with a unique suffix.\\nThe provided value has the same validation rules as the Name field,\\nand may be truncated by the length of the suffix required to make the value\\nunique on the server.\\n\\nIf this field is specified and the generated name exists, the server will\\nNOT return a 409 - instead, it will either return 201 Created or 500 with Reason\\nServerTimeout indicating a unique name could not be found in the time allotted, and the client\\nshould retry (optionally after the time indicated in the Retry-After header).\\n\\nApplied only if Name is not specified.\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#idempotency\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"GenerateName\"\n" +
		"        },\n" +
		"        \"generation\": {\n" +
		"          \"description\": \"A sequence number representing a specific generation of the desired state.\\nPopulated by the system. Read-only.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"Generation\"\n" +
		"        },\n" +
		"        \"labels\": {\n" +
		"          \"description\": \"Map of string keys and values that can be used to organize and categorize\\n(scope and select) objects. May match selectors of replication controllers\\nand services.\\nMore info: http://kubernetes.io/docs/user-guide/labels\\n+optional\",\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Labels\"\n" +
		"        },\n" +
		"        \"managedFields\": {\n" +
		"          \"description\": \"ManagedFields maps workflow-id and version to the set of fields\\nthat are managed by that workflow. This is mostly for internal\\nhousekeeping, and users typically shouldn't need to set or\\nunderstand this field. A workflow can be the user's name, a\\ncontroller's name, or the name of a specific apply path like\\n\\\"ci-cd\\\". The set of fields is always in the version that the\\nworkflow used when modifying the object.\\n\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1ManagedFieldsEntry\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"ManagedFields\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name must be unique within a namespace. Is required when creating resources, although\\nsome resources may allow a client to request the generation of an appropriate name\\nautomatically. Name is primarily intended for creation idempotence and configuration\\ndefinition.\\nCannot be updated.\\nMore info: http://kubernetes.io/docs/user-guide/identifiers#names\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"namespace\": {\n" +
		"          \"description\": \"Namespace defines the space within which each name must be unique. An empty namespace is\\nequivalent to the \\\"default\\\" namespace, but \\\"default\\\" is the canonical representation.\\nNot all objects are required to be scoped to a namespace - the value of this field for\\nthose objects will be empty.\\n\\nMust be a DNS_LABEL.\\nCannot be updated.\\nMore info: http://kubernetes.io/docs/user-guide/namespaces\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Namespace\"\n" +
		"        },\n" +
		"        \"ownerReferences\": {\n" +
		"          \"description\": \"List of objects depended by this object. If ALL objects in the list have\\nbeen deleted, this object will be garbage collected. If this object is managed by a controller,\\nthen an entry in this list will point to this controller, with the controller field set to true.\\nThere cannot be more than one managing controller.\\n+optional\\n+patchMergeKey=uid\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1OwnerReference\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"OwnerReferences\"\n" +
		"        },\n" +
		"        \"resourceVersion\": {\n" +
		"          \"description\": \"An opaque value that represents the internal version of this object that can\\nbe used by clients to determine when objects have changed. May be used for optimistic\\nconcurrency, change detection, and the watch operation on a resource or set of resources.\\nClients must treat these values as opaque and passed unmodified back to the server.\\nThey may only be valid for a particular resource or set of resources.\\n\\nPopulated by the system.\\nRead-only.\\nValue must be treated as opaque by clients and .\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#concurrency-control-and-consistency\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"ResourceVersion\"\n" +
		"        },\n" +
		"        \"selfLink\": {\n" +
		"          \"description\": \"SelfLink is a URL representing this object.\\nPopulated by the system.\\nRead-only.\\n\\nDEPRECATED\\nKubernetes will stop propagating this field in 1.20 release and the field is planned\\nto be removed in 1.21 release.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"SelfLink\"\n" +
		"        },\n" +
		"        \"uid\": {\n" +
		"          \"$ref\": \"#/definitions/typesUID\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/apis/meta/v1\"\n" +
		"    },\n" +
		"    \"v1OwnerReference\": {\n" +
		"      \"description\": \"OwnerReference contains enough information to let you identify an owning\\nobject. An owning object must be in the same namespace as the dependent, or\\nbe cluster-scoped, so there is no namespace field.\\n+structType=atomic\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"apiVersion\": {\n" +
		"          \"description\": \"API version of the referent.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"APIVersion\"\n" +
		"        },\n" +
		"        \"blockOwnerDeletion\": {\n" +
		"          \"description\": \"If true, AND if the owner has the \\\"foregroundDeletion\\\" finalizer, then\\nthe owner cannot be deleted from the key-value store until this\\nreference is removed.\\nDefaults to false.\\nTo set this field, a user needs \\\"delete\\\" permission of the owner,\\notherwise 422 (Unprocessable Entity) will be returned.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"BlockOwnerDeletion\"\n" +
		"        },\n" +
		"        \"controller\": {\n" +
		"          \"description\": \"If true, this reference points to the managing controller.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Controller\"\n" +
		"        },\n" +
		"        \"kind\": {\n" +
		"          \"description\": \"Kind of the referent.\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Kind\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the referent.\\nMore info: http://kubernetes.io/docs/user-guide/identifiers#names\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"uid\": {\n" +
		"          \"$ref\": \"#/definitions/typesUID\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/apis/meta/v1\"\n" +
		"    },\n" +
		"    \"v1PersistentVolumeAccessMode\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PersistentVolumeClaimSpec\": {\n" +
		"      \"description\": \"PersistentVolumeClaimSpec describes the common attributes of storage devices\\nand allows a Source for provider-specific attributes\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"accessModes\": {\n" +
		"          \"description\": \"AccessModes contains the desired access modes the volume should have.\\nMore info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#access-modes-1\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PersistentVolumeAccessMode\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"AccessModes\"\n" +
		"        },\n" +
		"        \"dataSource\": {\n" +
		"          \"$ref\": \"#/definitions/v1TypedLocalObjectReference\"\n" +
		"        },\n" +
		"        \"dataSourceRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1TypedLocalObjectReference\"\n" +
		"        },\n" +
		"        \"resources\": {\n" +
		"          \"$ref\": \"#/definitions/v1ResourceRequirements\"\n" +
		"        },\n" +
		"        \"selector\": {\n" +
		"          \"$ref\": \"#/definitions/v1LabelSelector\"\n" +
		"        },\n" +
		"        \"storageClassName\": {\n" +
		"          \"description\": \"Name of the StorageClass required by the claim.\\nMore info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#class-1\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"StorageClassName\"\n" +
		"        },\n" +
		"        \"volumeMode\": {\n" +
		"          \"$ref\": \"#/definitions/v1PersistentVolumeMode\"\n" +
		"        },\n" +
		"        \"volumeName\": {\n" +
		"          \"description\": \"VolumeName is the binding reference to the PersistentVolume backing this claim.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"VolumeName\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PersistentVolumeClaimTemplate\": {\n" +
		"      \"description\": \"PersistentVolumeClaimTemplate is used to produce\\nPersistentVolumeClaim objects as part of an EphemeralVolumeSource.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"annotations\": {\n" +
		"          \"description\": \"Annotations is an unstructured key value map stored with a resource that may be\\nset by external tools to store and retrieve arbitrary metadata. They are not\\nqueryable and should be preserved when modifying objects.\\nMore info: http://kubernetes.io/docs/user-guide/annotations\\n+optional\",\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Annotations\"\n" +
		"        },\n" +
		"        \"clusterName\": {\n" +
		"          \"description\": \"The name of the cluster which the object belongs to.\\nThis is used to distinguish resources with same name and namespace in different clusters.\\nThis field is not set anywhere right now and apiserver is going to ignore it if set in create or update request.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"ClusterName\"\n" +
		"        },\n" +
		"        \"creationTimestamp\": {\n" +
		"          \"$ref\": \"#/definitions/v1Time\"\n" +
		"        },\n" +
		"        \"deletionGracePeriodSeconds\": {\n" +
		"          \"description\": \"Number of seconds allowed for this object to gracefully terminate before\\nit will be removed from the system. Only set when deletionTimestamp is also set.\\nMay only be shortened.\\nRead-only.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"DeletionGracePeriodSeconds\"\n" +
		"        },\n" +
		"        \"deletionTimestamp\": {\n" +
		"          \"$ref\": \"#/definitions/v1Time\"\n" +
		"        },\n" +
		"        \"finalizers\": {\n" +
		"          \"description\": \"Must be empty before the object is deleted from the registry. Each entry\\nis an identifier for the responsible component that will remove the entry\\nfrom the list. If the deletionTimestamp of the object is non-nil, entries\\nin this list can only be removed.\\nFinalizers may be processed and removed in any order.  Order is NOT enforced\\nbecause it introduces significant risk of stuck finalizers.\\nfinalizers is a shared field, any actor with permission can reorder it.\\nIf the finalizer list is processed in order, then this can lead to a situation\\nin which the component responsible for the first finalizer in the list is\\nwaiting for a signal (field value, external system, or other) produced by a\\ncomponent responsible for a finalizer later in the list, resulting in a deadlock.\\nWithout enforced ordering finalizers are free to order amongst themselves and\\nare not vulnerable to ordering changes in the list.\\n+optional\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Finalizers\"\n" +
		"        },\n" +
		"        \"generateName\": {\n" +
		"          \"description\": \"GenerateName is an optional prefix, used by the server, to generate a unique\\nname ONLY IF the Name field has not been provided.\\nIf this field is used, the name returned to the client will be different\\nthan the name passed. This value will also be combined with a unique suffix.\\nThe provided value has the same validation rules as the Name field,\\nand may be truncated by the length of the suffix required to make the value\\nunique on the server.\\n\\nIf this field is specified and the generated name exists, the server will\\nNOT return a 409 - instead, it will either return 201 Created or 500 with Reason\\nServerTimeout indicating a unique name could not be found in the time allotted, and the client\\nshould retry (optionally after the time indicated in the Retry-After header).\\n\\nApplied only if Name is not specified.\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#idempotency\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"GenerateName\"\n" +
		"        },\n" +
		"        \"generation\": {\n" +
		"          \"description\": \"A sequence number representing a specific generation of the desired state.\\nPopulated by the system. Read-only.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"Generation\"\n" +
		"        },\n" +
		"        \"labels\": {\n" +
		"          \"description\": \"Map of string keys and values that can be used to organize and categorize\\n(scope and select) objects. May match selectors of replication controllers\\nand services.\\nMore info: http://kubernetes.io/docs/user-guide/labels\\n+optional\",\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Labels\"\n" +
		"        },\n" +
		"        \"managedFields\": {\n" +
		"          \"description\": \"ManagedFields maps workflow-id and version to the set of fields\\nthat are managed by that workflow. This is mostly for internal\\nhousekeeping, and users typically shouldn't need to set or\\nunderstand this field. A workflow can be the user's name, a\\ncontroller's name, or the name of a specific apply path like\\n\\\"ci-cd\\\". The set of fields is always in the version that the\\nworkflow used when modifying the object.\\n\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1ManagedFieldsEntry\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"ManagedFields\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name must be unique within a namespace. Is required when creating resources, although\\nsome resources may allow a client to request the generation of an appropriate name\\nautomatically. Name is primarily intended for creation idempotence and configuration\\ndefinition.\\nCannot be updated.\\nMore info: http://kubernetes.io/docs/user-guide/identifiers#names\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"namespace\": {\n" +
		"          \"description\": \"Namespace defines the space within which each name must be unique. An empty namespace is\\nequivalent to the \\\"default\\\" namespace, but \\\"default\\\" is the canonical representation.\\nNot all objects are required to be scoped to a namespace - the value of this field for\\nthose objects will be empty.\\n\\nMust be a DNS_LABEL.\\nCannot be updated.\\nMore info: http://kubernetes.io/docs/user-guide/namespaces\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Namespace\"\n" +
		"        },\n" +
		"        \"ownerReferences\": {\n" +
		"          \"description\": \"List of objects depended by this object. If ALL objects in the list have\\nbeen deleted, this object will be garbage collected. If this object is managed by a controller,\\nthen an entry in this list will point to this controller, with the controller field set to true.\\nThere cannot be more than one managing controller.\\n+optional\\n+patchMergeKey=uid\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1OwnerReference\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"OwnerReferences\"\n" +
		"        },\n" +
		"        \"resourceVersion\": {\n" +
		"          \"description\": \"An opaque value that represents the internal version of this object that can\\nbe used by clients to determine when objects have changed. May be used for optimistic\\nconcurrency, change detection, and the watch operation on a resource or set of resources.\\nClients must treat these values as opaque and passed unmodified back to the server.\\nThey may only be valid for a particular resource or set of resources.\\n\\nPopulated by the system.\\nRead-only.\\nValue must be treated as opaque by clients and .\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#concurrency-control-and-consistency\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"ResourceVersion\"\n" +
		"        },\n" +
		"        \"selfLink\": {\n" +
		"          \"description\": \"SelfLink is a URL representing this object.\\nPopulated by the system.\\nRead-only.\\n\\nDEPRECATED\\nKubernetes will stop propagating this field in 1.20 release and the field is planned\\nto be removed in 1.21 release.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"SelfLink\"\n" +
		"        },\n" +
		"        \"spec\": {\n" +
		"          \"$ref\": \"#/definitions/v1PersistentVolumeClaimSpec\"\n" +
		"        },\n" +
		"        \"uid\": {\n" +
		"          \"$ref\": \"#/definitions/typesUID\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PersistentVolumeClaimVolumeSource\": {\n" +
		"      \"description\": \"This volume finds the bound PV and mounts that volume for the pod. A\\nPersistentVolumeClaimVolumeSource is, essentially, a wrapper around another\\ntype of volume that is owned by someone else (the system).\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"PersistentVolumeClaimVolumeSource references the user's PVC in the same namespace.\",\n" +
		"      \"properties\": {\n" +
		"        \"claimName\": {\n" +
		"          \"description\": \"ClaimName is the name of a PersistentVolumeClaim in the same namespace as the pod using this volume.\\nMore info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"ClaimName\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Will force the ReadOnly setting in VolumeMounts.\\nDefault false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PersistentVolumeMode\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"PersistentVolumeMode describes how a volume is intended to be consumed, either Block or Filesystem.\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PhotonPersistentDiskVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a Photon Controller persistent disk resource.\",\n" +
		"      \"properties\": {\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"pdID\": {\n" +
		"          \"description\": \"ID that identifies Photon Controller persistent disk\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"PdID\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PodAffinity\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Pod affinity is a group of inter pod affinity scheduling rules.\",\n" +
		"      \"properties\": {\n" +
		"        \"preferredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"description\": \"The scheduler will prefer to schedule pods to nodes that satisfy\\nthe affinity expressions specified by this field, but it may choose\\na node that violates one or more of the expressions. The node that is\\nmost preferred is the one with the greatest sum of weights, i.e.\\nfor each node that meets all of the scheduling requirements (resource\\nrequest, requiredDuringScheduling affinity expressions, etc.),\\ncompute a sum by iterating through the elements of this field and adding\\n\\\"weight\\\" to the sum if the node has pods which matches the corresponding podAffinityTerm; the\\nnode(s) with the highest sum are the most preferred.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1WeightedPodAffinityTerm\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"PreferredDuringSchedulingIgnoredDuringExecution\"\n" +
		"        },\n" +
		"        \"requiredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"description\": \"If the affinity requirements specified by this field are not met at\\nscheduling time, the pod will not be scheduled onto the node.\\nIf the affinity requirements specified by this field cease to be met\\nat some point during pod execution (e.g. due to a pod label update), the\\nsystem may or may not try to eventually evict the pod from its node.\\nWhen there are multiple elements, the lists of nodes corresponding to each\\npodAffinityTerm are intersected, i.e. all terms must be satisfied.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PodAffinityTerm\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"RequiredDuringSchedulingIgnoredDuringExecution\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PodAffinityTerm\": {\n" +
		"      \"description\": \"Defines a set of pods (namely those matching the labelSelector\\nrelative to the given namespace(s)) that this pod should be\\nco-located (affinity) or not co-located (anti-affinity) with,\\nwhere co-located is defined as running on a node whose value of\\nthe label with key \\u003ctopologyKey\\u003e matches that of any node on which\\na pod of the set of pods is running\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"labelSelector\": {\n" +
		"          \"$ref\": \"#/definitions/v1LabelSelector\"\n" +
		"        },\n" +
		"        \"namespaceSelector\": {\n" +
		"          \"$ref\": \"#/definitions/v1LabelSelector\"\n" +
		"        },\n" +
		"        \"namespaces\": {\n" +
		"          \"description\": \"namespaces specifies a static list of namespace names that the term applies to.\\nThe term is applied to the union of the namespaces listed in this field\\nand the ones selected by namespaceSelector.\\nnull or empty namespaces list and null namespaceSelector means \\\"this pod's namespace\\\"\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Namespaces\"\n" +
		"        },\n" +
		"        \"topologyKey\": {\n" +
		"          \"description\": \"This pod should be co-located (affinity) or not co-located (anti-affinity) with the pods matching\\nthe labelSelector in the specified namespaces, where co-located is defined as running on a node\\nwhose value of the label with key topologyKey matches that of any node on which any of the\\nselected pods is running.\\nEmpty topologyKey is not allowed.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"TopologyKey\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PodAntiAffinity\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Pod anti affinity is a group of inter pod anti affinity scheduling rules.\",\n" +
		"      \"properties\": {\n" +
		"        \"preferredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"description\": \"The scheduler will prefer to schedule pods to nodes that satisfy\\nthe anti-affinity expressions specified by this field, but it may choose\\na node that violates one or more of the expressions. The node that is\\nmost preferred is the one with the greatest sum of weights, i.e.\\nfor each node that meets all of the scheduling requirements (resource\\nrequest, requiredDuringScheduling anti-affinity expressions, etc.),\\ncompute a sum by iterating through the elements of this field and adding\\n\\\"weight\\\" to the sum if the node has pods which matches the corresponding podAffinityTerm; the\\nnode(s) with the highest sum are the most preferred.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1WeightedPodAffinityTerm\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"PreferredDuringSchedulingIgnoredDuringExecution\"\n" +
		"        },\n" +
		"        \"requiredDuringSchedulingIgnoredDuringExecution\": {\n" +
		"          \"description\": \"If the anti-affinity requirements specified by this field are not met at\\nscheduling time, the pod will not be scheduled onto the node.\\nIf the anti-affinity requirements specified by this field cease to be met\\nat some point during pod execution (e.g. due to a pod label update), the\\nsystem may or may not try to eventually evict the pod from its node.\\nWhen there are multiple elements, the lists of nodes corresponding to each\\npodAffinityTerm are intersected, i.e. all terms must be satisfied.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PodAffinityTerm\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"RequiredDuringSchedulingIgnoredDuringExecution\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PodConditionType\": {\n" +
		"      \"description\": \"PodConditionType is a valid value for PodCondition.Type\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PodDNSConfig\": {\n" +
		"      \"description\": \"PodDNSConfig defines the DNS parameters of a pod in addition to\\nthose generated from DNSPolicy.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"nameservers\": {\n" +
		"          \"description\": \"A list of DNS name server IP addresses.\\nThis will be appended to the base nameservers generated from DNSPolicy.\\nDuplicated nameservers will be removed.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Nameservers\"\n" +
		"        },\n" +
		"        \"options\": {\n" +
		"          \"description\": \"A list of DNS resolver options.\\nThis will be merged with the base options generated from DNSPolicy.\\nDuplicated entries will be removed. Resolution options given in Options\\nwill override those that appear in the base DNSPolicy.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PodDNSConfigOption\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Options\"\n" +
		"        },\n" +
		"        \"searches\": {\n" +
		"          \"description\": \"A list of DNS search domains for host-name lookup.\\nThis will be appended to the base search paths generated from DNSPolicy.\\nDuplicated search paths will be removed.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Searches\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PodDNSConfigOption\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"PodDNSConfigOption defines DNS resolver options of a pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Required.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"description\": \"+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Value\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PodFSGroupChangePolicy\": {\n" +
		"      \"description\": \"PodFSGroupChangePolicy holds policies that will be used for applying fsGroup to a volume\\nwhen volume is mounted.\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PodReadinessGate\": {\n" +
		"      \"description\": \"PodReadinessGate contains the reference to a pod condition\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"conditionType\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodConditionType\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PodSecurityContext\": {\n" +
		"      \"description\": \"Some fields are also present in container.securityContext.  Field values of\\ncontainer.securityContext take precedence over field values of PodSecurityContext.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"PodSecurityContext holds pod-level security attributes and common container settings.\",\n" +
		"      \"properties\": {\n" +
		"        \"fsGroup\": {\n" +
		"          \"description\": \"A special supplemental group that applies to all containers in a pod.\\nSome volume types allow the Kubelet to change the ownership of that volume\\nto be owned by the pod:\\n\\n1. The owning GID will be the FSGroup\\n2. The setgid bit is set (new files created in the volume will be owned by FSGroup)\\n3. The permission bits are OR'd with rw-rw----\\n\\nIf unset, the Kubelet will not modify the ownership and permissions of any volume.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"FSGroup\"\n" +
		"        },\n" +
		"        \"fsGroupChangePolicy\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodFSGroupChangePolicy\"\n" +
		"        },\n" +
		"        \"runAsGroup\": {\n" +
		"          \"description\": \"The GID to run the entrypoint of the container process.\\nUses runtime default if unset.\\nMay also be set in SecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence\\nfor that container.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"RunAsGroup\"\n" +
		"        },\n" +
		"        \"runAsNonRoot\": {\n" +
		"          \"description\": \"Indicates that the container must run as a non-root user.\\nIf true, the Kubelet will validate the image at runtime to ensure that it\\ndoes not run as UID 0 (root) and fail to start the container if it does.\\nIf unset or false, no such validation will be performed.\\nMay also be set in SecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"RunAsNonRoot\"\n" +
		"        },\n" +
		"        \"runAsUser\": {\n" +
		"          \"description\": \"The UID to run the entrypoint of the container process.\\nDefaults to user specified in image metadata if unspecified.\\nMay also be set in SecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence\\nfor that container.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"RunAsUser\"\n" +
		"        },\n" +
		"        \"seLinuxOptions\": {\n" +
		"          \"$ref\": \"#/definitions/v1SELinuxOptions\"\n" +
		"        },\n" +
		"        \"seccompProfile\": {\n" +
		"          \"$ref\": \"#/definitions/v1SeccompProfile\"\n" +
		"        },\n" +
		"        \"supplementalGroups\": {\n" +
		"          \"description\": \"A list of groups applied to the first process run in each container, in addition\\nto the container's primary GID.  If unspecified, no groups will be added to\\nany container.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"integer\",\n" +
		"            \"format\": \"int64\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"SupplementalGroups\"\n" +
		"        },\n" +
		"        \"sysctls\": {\n" +
		"          \"description\": \"Sysctls hold a list of namespaced sysctls used for the pod. Pods with unsupported\\nsysctls (by the container runtime) might fail to launch.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Sysctl\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Sysctls\"\n" +
		"        },\n" +
		"        \"windowsOptions\": {\n" +
		"          \"$ref\": \"#/definitions/v1WindowsSecurityContextOptions\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PodSpec\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"PodSpec is a description of a pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"activeDeadlineSeconds\": {\n" +
		"          \"description\": \"Optional duration in seconds the pod may be active on the node relative to\\nStartTime before the system will actively try to mark it failed and kill associated containers.\\nValue must be a positive integer.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"ActiveDeadlineSeconds\"\n" +
		"        },\n" +
		"        \"affinity\": {\n" +
		"          \"$ref\": \"#/definitions/v1Affinity\"\n" +
		"        },\n" +
		"        \"automountServiceAccountToken\": {\n" +
		"          \"description\": \"AutomountServiceAccountToken indicates whether a service account token should be automatically mounted.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"AutomountServiceAccountToken\"\n" +
		"        },\n" +
		"        \"containers\": {\n" +
		"          \"description\": \"List of containers belonging to the pod.\\nContainers cannot currently be added or removed.\\nThere must be at least one container in a Pod.\\nCannot be updated.\\n+patchMergeKey=name\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Container\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Containers\"\n" +
		"        },\n" +
		"        \"dnsConfig\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodDNSConfig\"\n" +
		"        },\n" +
		"        \"dnsPolicy\": {\n" +
		"          \"$ref\": \"#/definitions/v1DNSPolicy\"\n" +
		"        },\n" +
		"        \"enableServiceLinks\": {\n" +
		"          \"description\": \"EnableServiceLinks indicates whether information about services should be injected into pod's\\nenvironment variables, matching the syntax of Docker links.\\nOptional: Defaults to true.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"EnableServiceLinks\"\n" +
		"        },\n" +
		"        \"ephemeralContainers\": {\n" +
		"          \"description\": \"List of ephemeral containers run in this pod. Ephemeral containers may be run in an existing\\npod to perform user-initiated actions such as debugging. This list cannot be specified when\\ncreating a pod, and it cannot be modified by updating the pod spec. In order to add an\\nephemeral container to an existing pod, use the pod's ephemeralcontainers subresource.\\nThis field is alpha-level and is only honored by servers that enable the EphemeralContainers feature.\\n+optional\\n+patchMergeKey=name\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1EphemeralContainer\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"EphemeralContainers\"\n" +
		"        },\n" +
		"        \"hostAliases\": {\n" +
		"          \"description\": \"HostAliases is an optional list of hosts and IPs that will be injected into the pod's hosts\\nfile if specified. This is only valid for non-hostNetwork pods.\\n+optional\\n+patchMergeKey=ip\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1HostAlias\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"HostAliases\"\n" +
		"        },\n" +
		"        \"hostIPC\": {\n" +
		"          \"description\": \"Use the host's ipc namespace.\\nOptional: Default to false.\\n+k8s:conversion-gen=false\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"HostIPC\"\n" +
		"        },\n" +
		"        \"hostNetwork\": {\n" +
		"          \"description\": \"Host networking requested for this pod. Use the host's network namespace.\\nIf this option is set, the ports that will be used must be specified.\\nDefault to false.\\n+k8s:conversion-gen=false\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"HostNetwork\"\n" +
		"        },\n" +
		"        \"hostPID\": {\n" +
		"          \"description\": \"Use the host's pid namespace.\\nOptional: Default to false.\\n+k8s:conversion-gen=false\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"HostPID\"\n" +
		"        },\n" +
		"        \"hostname\": {\n" +
		"          \"description\": \"Specifies the hostname of the Pod\\nIf not specified, the pod's hostname will be set to a system-defined value.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Hostname\"\n" +
		"        },\n" +
		"        \"imagePullSecrets\": {\n" +
		"          \"description\": \"ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec.\\nIf specified, these secrets will be passed to individual puller implementations for them to use. For example,\\nin the case of docker, only DockerConfig type secrets are honored.\\nMore info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod\\n+optional\\n+patchMergeKey=name\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"ImagePullSecrets\"\n" +
		"        },\n" +
		"        \"initContainers\": {\n" +
		"          \"description\": \"List of initialization containers belonging to the pod.\\nInit containers are executed in order prior to containers being started. If any\\ninit container fails, the pod is considered to have failed and is handled according\\nto its restartPolicy. The name for an init container or normal container must be\\nunique among all containers.\\nInit containers may not have Lifecycle actions, Readiness probes, Liveness probes, or Startup probes.\\nThe resourceRequirements of an init container are taken into account during scheduling\\nby finding the highest request/limit for each resource type, and then using the max of\\nof that value or the sum of the normal containers. Limits are applied to init containers\\nin a similar fashion.\\nInit containers cannot currently be added or removed.\\nCannot be updated.\\nMore info: https://kubernetes.io/docs/concepts/workloads/pods/init-containers/\\n+patchMergeKey=name\\n+patchStrategy=merge\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Container\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"InitContainers\"\n" +
		"        },\n" +
		"        \"nodeName\": {\n" +
		"          \"description\": \"NodeName is a request to schedule this pod onto a specific node. If it is non-empty,\\nthe scheduler simply schedules this pod onto that node, assuming that it fits resource\\nrequirements.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"NodeName\"\n" +
		"        },\n" +
		"        \"nodeSelector\": {\n" +
		"          \"description\": \"NodeSelector is a selector which must be true for the pod to fit on a node.\\nSelector which must match a node's labels for the pod to be scheduled on that node.\\nMore info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/\\n+optional\\n+mapType=atomic\",\n" +
		"          \"type\": \"object\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"NodeSelector\"\n" +
		"        },\n" +
		"        \"overhead\": {\n" +
		"          \"$ref\": \"#/definitions/v1ResourceList\"\n" +
		"        },\n" +
		"        \"preemptionPolicy\": {\n" +
		"          \"$ref\": \"#/definitions/v1PreemptionPolicy\"\n" +
		"        },\n" +
		"        \"priority\": {\n" +
		"          \"description\": \"The priority value. Various system components use this field to find the\\npriority of the pod. When Priority Admission Controller is enabled, it\\nprevents users from setting this field. The admission controller populates\\nthis field from PriorityClassName.\\nThe higher the value, the higher the priority.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"Priority\"\n" +
		"        },\n" +
		"        \"priorityClassName\": {\n" +
		"          \"description\": \"If specified, indicates the pod's priority. \\\"system-node-critical\\\" and\\n\\\"system-cluster-critical\\\" are two special keywords which indicate the\\nhighest priorities with the former being the highest priority. Any other\\nname must be defined by creating a PriorityClass object with that name.\\nIf not specified, the pod priority will be default or zero if there is no\\ndefault.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"PriorityClassName\"\n" +
		"        },\n" +
		"        \"readinessGates\": {\n" +
		"          \"description\": \"If specified, all readiness gates will be evaluated for pod readiness.\\nA pod is ready when all its containers are ready AND\\nall conditions specified in the readiness gates have status equal to \\\"True\\\"\\nMore info: https://git.k8s.io/enhancements/keps/sig-network/580-pod-readiness-gates\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1PodReadinessGate\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"ReadinessGates\"\n" +
		"        },\n" +
		"        \"restartPolicy\": {\n" +
		"          \"$ref\": \"#/definitions/v1RestartPolicy\"\n" +
		"        },\n" +
		"        \"runtimeClassName\": {\n" +
		"          \"description\": \"RuntimeClassName refers to a RuntimeClass object in the node.k8s.io group, which should be used\\nto run this pod.  If no RuntimeClass resource matches the named class, the pod will not be run.\\nIf unset or empty, the \\\"legacy\\\" RuntimeClass will be used, which is an implicit class with an\\nempty definition that uses the default runtime handler.\\nMore info: https://git.k8s.io/enhancements/keps/sig-node/585-runtime-class\\nThis is a beta feature as of Kubernetes v1.14.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"RuntimeClassName\"\n" +
		"        },\n" +
		"        \"schedulerName\": {\n" +
		"          \"description\": \"If specified, the pod will be dispatched by specified scheduler.\\nIf not specified, the pod will be dispatched by default scheduler.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"SchedulerName\"\n" +
		"        },\n" +
		"        \"securityContext\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodSecurityContext\"\n" +
		"        },\n" +
		"        \"serviceAccount\": {\n" +
		"          \"description\": \"DeprecatedServiceAccount is a depreciated alias for ServiceAccountName.\\nDeprecated: Use serviceAccountName instead.\\n+k8s:conversion-gen=false\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"DeprecatedServiceAccount\"\n" +
		"        },\n" +
		"        \"serviceAccountName\": {\n" +
		"          \"description\": \"ServiceAccountName is the name of the ServiceAccount to use to run this pod.\\nMore info: https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"ServiceAccountName\"\n" +
		"        },\n" +
		"        \"setHostnameAsFQDN\": {\n" +
		"          \"description\": \"If true the pod's hostname will be configured as the pod's FQDN, rather than the leaf name (the default).\\nIn Linux containers, this means setting the FQDN in the hostname field of the kernel (the nodename field of struct utsname).\\nIn Windows containers, this means setting the registry value of hostname for the registry key HKEY_LOCAL_MACHINE\\\\\\\\SYSTEM\\\\\\\\CurrentControlSet\\\\\\\\Services\\\\\\\\Tcpip\\\\\\\\Parameters to FQDN.\\nIf a pod does not have FQDN, this has no effect.\\nDefault to false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"SetHostnameAsFQDN\"\n" +
		"        },\n" +
		"        \"shareProcessNamespace\": {\n" +
		"          \"description\": \"Share a single process namespace between all of the containers in a pod.\\nWhen this is set containers will be able to view and signal processes from other containers\\nin the same pod, and the first process in each container will not be assigned PID 1.\\nHostPID and ShareProcessNamespace cannot both be set.\\nOptional: Default to false.\\n+k8s:conversion-gen=false\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ShareProcessNamespace\"\n" +
		"        },\n" +
		"        \"subdomain\": {\n" +
		"          \"description\": \"If specified, the fully qualified Pod hostname will be \\\"\\u003chostname\\u003e.\\u003csubdomain\\u003e.\\u003cpod namespace\\u003e.svc.\\u003ccluster domain\\u003e\\\".\\nIf not specified, the pod will not have a domainname at all.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Subdomain\"\n" +
		"        },\n" +
		"        \"terminationGracePeriodSeconds\": {\n" +
		"          \"description\": \"Optional duration in seconds the pod needs to terminate gracefully. May be decreased in delete request.\\nValue must be non-negative integer. The value zero indicates stop immediately via\\nthe kill signal (no opportunity to shut down).\\nIf this value is nil, the default grace period will be used instead.\\nThe grace period is the duration in seconds after the processes running in the pod are sent\\na termination signal and the time when the processes are forcibly halted with a kill signal.\\nSet this value longer than the expected cleanup time for your process.\\nDefaults to 30 seconds.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"TerminationGracePeriodSeconds\"\n" +
		"        },\n" +
		"        \"tolerations\": {\n" +
		"          \"description\": \"If specified, the pod's tolerations.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Toleration\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Tolerations\"\n" +
		"        },\n" +
		"        \"topologySpreadConstraints\": {\n" +
		"          \"description\": \"TopologySpreadConstraints describes how a group of pods ought to spread across topology\\ndomains. Scheduler will schedule pods in a way which abides by the constraints.\\nAll topologySpreadConstraints are ANDed.\\n+optional\\n+patchMergeKey=topologyKey\\n+patchStrategy=merge\\n+listType=map\\n+listMapKey=topologyKey\\n+listMapKey=whenUnsatisfiable\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1TopologySpreadConstraint\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"TopologySpreadConstraints\"\n" +
		"        },\n" +
		"        \"volumes\": {\n" +
		"          \"description\": \"List of volumes that can be mounted by containers belonging to the pod.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes\\n+optional\\n+patchMergeKey=name\\n+patchStrategy=merge,retainKeys\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Volume\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Volumes\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PortStatus\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"error\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Error is to record the problem with the service port\\nThe format of the error shall comply with the following rules:\\n- built-in error values shall be specified in this file and those shall use\\n  CamelCase names\\n- cloud provider specific error values must have names that comply with the\\n  format foo.example.com/CamelCase.\\n---\\nThe regex it matches is (dns1123SubdomainFmt/)?(qualifiedNameFmt)\\n+optional\\n+kubebuilder:validation:Required\\n+kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\\\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$`\\n+kubebuilder:validation:MaxLength=316\"\n" +
		"        },\n" +
		"        \"port\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Port is the port number of the service port of which status is recorded here\"\n" +
		"        },\n" +
		"        \"protocol\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Protocol is the protocol of the service port of which status is recorded here\\nThe supported values are: \\\"TCP\\\", \\\"UDP\\\", \\\"SCTP\\\"\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1PortworxVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"PortworxVolumeSource represents a Portworx volume resource.\",\n" +
		"      \"properties\": {\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"FSType represents the filesystem type to mount\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"volumeID\": {\n" +
		"          \"description\": \"VolumeID uniquely identifies a Portworx volume\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"VolumeID\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PreemptionPolicy\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"PreemptionPolicy describes a policy for if/when to preempt a pod.\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PreferredSchedulingTerm\": {\n" +
		"      \"description\": \"An empty preferred scheduling term matches all objects with implicit weight 0\\n(i.e. it's a no-op). A null preferred scheduling term matches no objects (i.e. is also a no-op).\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"preference\": {\n" +
		"          \"$ref\": \"#/definitions/v1NodeSelectorTerm\"\n" +
		"        },\n" +
		"        \"weight\": {\n" +
		"          \"description\": \"Weight associated with matching the corresponding nodeSelectorTerm, in the range 1-100.\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"Weight\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1Probe\": {\n" +
		"      \"description\": \"Probe describes a health check to be performed against a container to determine whether it is\\nalive or ready to receive traffic.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"exec\": {\n" +
		"          \"$ref\": \"#/definitions/v1ExecAction\"\n" +
		"        },\n" +
		"        \"failureThreshold\": {\n" +
		"          \"description\": \"Minimum consecutive failures for the probe to be considered failed after having succeeded.\\nDefaults to 3. Minimum value is 1.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"FailureThreshold\"\n" +
		"        },\n" +
		"        \"httpGet\": {\n" +
		"          \"$ref\": \"#/definitions/v1HTTPGetAction\"\n" +
		"        },\n" +
		"        \"initialDelaySeconds\": {\n" +
		"          \"description\": \"Number of seconds after the container has started before liveness probes are initiated.\\nMore info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"InitialDelaySeconds\"\n" +
		"        },\n" +
		"        \"periodSeconds\": {\n" +
		"          \"description\": \"How often (in seconds) to perform the probe.\\nDefault to 10 seconds. Minimum value is 1.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"PeriodSeconds\"\n" +
		"        },\n" +
		"        \"successThreshold\": {\n" +
		"          \"description\": \"Minimum consecutive successes for the probe to be considered successful after having failed.\\nDefaults to 1. Must be 1 for liveness and startup. Minimum value is 1.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"SuccessThreshold\"\n" +
		"        },\n" +
		"        \"tcpSocket\": {\n" +
		"          \"$ref\": \"#/definitions/v1TCPSocketAction\"\n" +
		"        },\n" +
		"        \"terminationGracePeriodSeconds\": {\n" +
		"          \"description\": \"Optional duration in seconds the pod needs to terminate gracefully upon probe failure.\\nThe grace period is the duration in seconds after the processes running in the pod are sent\\na termination signal and the time when the processes are forcibly halted with a kill signal.\\nSet this value longer than the expected cleanup time for your process.\\nIf this value is nil, the pod's terminationGracePeriodSeconds will be used. Otherwise, this\\nvalue overrides the value provided by the pod spec.\\nValue must be non-negative integer. The value zero indicates stop immediately via\\nthe kill signal (no opportunity to shut down).\\nThis is a beta field and requires enabling ProbeTerminationGracePeriod feature gate.\\nMinimum value is 1. spec.terminationGracePeriodSeconds is used if unset.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"TerminationGracePeriodSeconds\"\n" +
		"        },\n" +
		"        \"timeoutSeconds\": {\n" +
		"          \"description\": \"Number of seconds after which the probe times out.\\nDefaults to 1 second. Minimum value is 1.\\nMore info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"TimeoutSeconds\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ProcMountType\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ProjectedVolumeSource\": {\n" +
		"      \"description\": \"Represents a projected volume source\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"defaultMode\": {\n" +
		"          \"description\": \"Mode bits used to set permissions on created files by default.\\nMust be an octal value between 0000 and 0777 or a decimal value between 0 and 511.\\nYAML accepts both octal and decimal values, JSON requires decimal values for mode bits.\\nDirectories within the path are not affected by this setting.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"DefaultMode\"\n" +
		"        },\n" +
		"        \"sources\": {\n" +
		"          \"description\": \"list of volume projections\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1VolumeProjection\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Sources\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1Protocol\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"Protocol defines network protocols supported for things like container ports.\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1PullPolicy\": {\n" +
		"      \"description\": \"PullPolicy describes a policy for if/when to pull a container image\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1QuobyteVolumeSource\": {\n" +
		"      \"description\": \"Quobyte volumes do not support ownership management or SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a Quobyte mount that lasts the lifetime of a pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"group\": {\n" +
		"          \"description\": \"Group to map volume access to\\nDefault is no group\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Group\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"ReadOnly here will force the Quobyte volume to be mounted with read-only permissions.\\nDefaults to false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"registry\": {\n" +
		"          \"description\": \"Registry represents a single or multiple Quobyte Registry services\\nspecified as a string as host:port pair (multiple entries are separated with commas)\\nwhich acts as the central registry for volumes\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Registry\"\n" +
		"        },\n" +
		"        \"tenant\": {\n" +
		"          \"description\": \"Tenant owning the given Quobyte volume in the Backend\\nUsed with dynamically provisioned Quobyte volumes, value is set by the plugin\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Tenant\"\n" +
		"        },\n" +
		"        \"user\": {\n" +
		"          \"description\": \"User to map volume access to\\nDefaults to serivceaccount user\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"User\"\n" +
		"        },\n" +
		"        \"volume\": {\n" +
		"          \"description\": \"Volume is a string that references an already created Quobyte volume by name.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Volume\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1RBDVolumeSource\": {\n" +
		"      \"description\": \"RBD volumes support ownership management and SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a Rados Block Device mount that lasts the lifetime of a pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type of the volume that you want to mount.\\nTip: Ensure that the filesystem type is supported by the host operating system.\\nExamples: \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#rbd\\nTODO: how do we prevent errors in the filesystem from compromising the machine\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"image\": {\n" +
		"          \"description\": \"The rados image name.\\nMore info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"RBDImage\"\n" +
		"        },\n" +
		"        \"keyring\": {\n" +
		"          \"description\": \"Keyring is the path to key ring for RBDUser.\\nDefault is /etc/ceph/keyring.\\nMore info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Keyring\"\n" +
		"        },\n" +
		"        \"monitors\": {\n" +
		"          \"description\": \"A collection of Ceph monitors.\\nMore info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"CephMonitors\"\n" +
		"        },\n" +
		"        \"pool\": {\n" +
		"          \"description\": \"The rados pool name.\\nDefault is rbd.\\nMore info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"RBDPool\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"ReadOnly here will force the ReadOnly setting in VolumeMounts.\\nDefaults to false.\\nMore info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"        },\n" +
		"        \"user\": {\n" +
		"          \"description\": \"The rados user name.\\nDefault is admin.\\nMore info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"RadosUser\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ResourceFieldSelector\": {\n" +
		"      \"description\": \"ResourceFieldSelector represents container resources (cpu, memory) and their output format\\n+structType=atomic\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"containerName\": {\n" +
		"          \"description\": \"Container name: required for volumes, optional for env vars\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"ContainerName\"\n" +
		"        },\n" +
		"        \"divisor\": {\n" +
		"          \"$ref\": \"#/definitions/resourceQuantity\"\n" +
		"        },\n" +
		"        \"resource\": {\n" +
		"          \"description\": \"Required: resource to select\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Resource\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ResourceList\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"ResourceList is a set of (resource name, quantity) pairs.\",\n" +
		"      \"additionalProperties\": {\n" +
		"        \"$ref\": \"#/definitions/resourceQuantity\"\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ResourceRequirements\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"ResourceRequirements describes the compute resource requirements.\",\n" +
		"      \"properties\": {\n" +
		"        \"limits\": {\n" +
		"          \"$ref\": \"#/definitions/v1ResourceList\"\n" +
		"        },\n" +
		"        \"requests\": {\n" +
		"          \"$ref\": \"#/definitions/v1ResourceList\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1RestartPolicy\": {\n" +
		"      \"description\": \"Only one of the following restart policies may be specified.\\nIf none of the following policies is specified, the default one\\nis RestartPolicyAlways.\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"RestartPolicy describes how the container should be restarted.\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1SELinuxOptions\": {\n" +
		"      \"description\": \"SELinuxOptions are the labels to be applied to the container\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"level\": {\n" +
		"          \"description\": \"Level is SELinux level label that applies to the container.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Level\"\n" +
		"        },\n" +
		"        \"role\": {\n" +
		"          \"description\": \"Role is a SELinux role label that applies to the container.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Role\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"description\": \"Type is a SELinux type label that applies to the container.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Type\"\n" +
		"        },\n" +
		"        \"user\": {\n" +
		"          \"description\": \"User is a SELinux user label that applies to the container.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"User\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ScaleIOVolumeSource\": {\n" +
		"      \"description\": \"ScaleIOVolumeSource represents a persistent ScaleIO volume\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\".\\nDefault is \\\"xfs\\\".\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"gateway\": {\n" +
		"          \"description\": \"The host address of the ScaleIO API Gateway.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Gateway\"\n" +
		"        },\n" +
		"        \"protectionDomain\": {\n" +
		"          \"description\": \"The name of the ScaleIO Protection Domain for the configured storage.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"ProtectionDomain\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"        },\n" +
		"        \"sslEnabled\": {\n" +
		"          \"description\": \"Flag to enable/disable SSL communication with Gateway, default false\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"SSLEnabled\"\n" +
		"        },\n" +
		"        \"storageMode\": {\n" +
		"          \"description\": \"Indicates whether the storage for a volume should be ThickProvisioned or ThinProvisioned.\\nDefault is ThinProvisioned.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"StorageMode\"\n" +
		"        },\n" +
		"        \"storagePool\": {\n" +
		"          \"description\": \"The ScaleIO Storage Pool associated with the protection domain.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"StoragePool\"\n" +
		"        },\n" +
		"        \"system\": {\n" +
		"          \"description\": \"The name of the storage system as configured in ScaleIO.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"System\"\n" +
		"        },\n" +
		"        \"volumeName\": {\n" +
		"          \"description\": \"The name of a volume already created in the ScaleIO system\\nthat is associated with this volume source.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"VolumeName\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1SeccompProfile\": {\n" +
		"      \"description\": \"Only one profile source may be set.\\n+union\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"SeccompProfile defines a pod/container's seccomp profile settings.\",\n" +
		"      \"properties\": {\n" +
		"        \"localhostProfile\": {\n" +
		"          \"description\": \"localhostProfile indicates a profile defined in a file on the node should be used.\\nThe profile must be preconfigured on the node to work.\\nMust be a descending path, relative to the kubelet's configured seccomp profile location.\\nMust only be set if type is \\\"Localhost\\\".\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"LocalhostProfile\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"$ref\": \"#/definitions/v1SeccompProfileType\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1SeccompProfileType\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"SeccompProfileType defines the supported seccomp profile types.\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1SecretEnvSource\": {\n" +
		"      \"description\": \"The contents of the target Secret's Data field will represent the\\nkey-value pairs as environment variables.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"SecretEnvSource selects a Secret to populate the environment\\nvariables with.\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the referent.\\nMore info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names\\nTODO: Add other useful fields. apiVersion, kind, uid?\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"description\": \"Specify whether the Secret must be defined\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1SecretKeySelector\": {\n" +
		"      \"description\": \"+structType=atomic\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"SecretKeySelector selects a key of a Secret.\",\n" +
		"      \"properties\": {\n" +
		"        \"key\": {\n" +
		"          \"description\": \"The key of the secret to select from.  Must be a valid secret key.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Key\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the referent.\\nMore info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names\\nTODO: Add other useful fields. apiVersion, kind, uid?\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"description\": \"Specify whether the Secret or its key must be defined\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1SecretProjection\": {\n" +
		"      \"description\": \"The contents of the target Secret's Data field will be presented in a\\nprojected volume as files using the keys in the Data field as the file names.\\nNote that this is identical to a secret volume source without the default\\nmode.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Adapts a secret into a projected volume.\",\n" +
		"      \"properties\": {\n" +
		"        \"items\": {\n" +
		"          \"description\": \"If unspecified, each key-value pair in the Data field of the referenced\\nSecret will be projected into the volume as a file whose name is the\\nkey and content is the value. If specified, the listed keys will be\\nprojected into the specified paths, and unlisted keys will not be\\npresent. If a key is specified which is not present in the Secret,\\nthe volume setup will error unless it is marked optional. Paths must be\\nrelative and may not contain the '..' path or start with '..'.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1KeyToPath\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Items\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of the referent.\\nMore info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names\\nTODO: Add other useful fields. apiVersion, kind, uid?\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"description\": \"Specify whether the Secret or its key must be defined\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Optional\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1SecretVolumeSource\": {\n" +
		"      \"description\": \"The contents of the target Secret's Data field will be presented in a volume\\nas files using the keys in the Data field as the file names.\\nSecret volumes support ownership management and SELinux relabeling.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Adapts a Secret into a volume.\",\n" +
		"      \"properties\": {\n" +
		"        \"defaultMode\": {\n" +
		"          \"description\": \"Optional: mode bits used to set permissions on created files by default.\\nMust be an octal value between 0000 and 0777 or a decimal value between 0 and 511.\\nYAML accepts both octal and decimal values, JSON requires decimal values\\nfor mode bits. Defaults to 0644.\\nDirectories within the path are not affected by this setting.\\nThis might be in conflict with other options that affect the file\\nmode, like fsGroup, and the result can be other mode bits set.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"DefaultMode\"\n" +
		"        },\n" +
		"        \"items\": {\n" +
		"          \"description\": \"If unspecified, each key-value pair in the Data field of the referenced\\nSecret will be projected into the volume as a file whose name is the\\nkey and content is the value. If specified, the listed keys will be\\nprojected into the specified paths, and unlisted keys will not be\\npresent. If a key is specified which is not present in the Secret,\\nthe volume setup will error unless it is marked optional. Paths must be\\nrelative and may not contain the '..' path or start with '..'.\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1KeyToPath\"\n" +
		"          },\n" +
		"          \"x-go-name\": \"Items\"\n" +
		"        },\n" +
		"        \"optional\": {\n" +
		"          \"description\": \"Specify whether the Secret or its keys must be defined\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Optional\"\n" +
		"        },\n" +
		"        \"secretName\": {\n" +
		"          \"description\": \"Name of the secret in the pod's namespace to use.\\nMore info: https://kubernetes.io/docs/concepts/storage/volumes#secret\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"SecretName\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1SecurityContext\": {\n" +
		"      \"description\": \"Some fields are present in both SecurityContext and PodSecurityContext.  When both\\nare set, the values in SecurityContext take precedence.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"SecurityContext holds security configuration that will be applied to a container.\",\n" +
		"      \"properties\": {\n" +
		"        \"allowPrivilegeEscalation\": {\n" +
		"          \"description\": \"AllowPrivilegeEscalation controls whether a process can gain more\\nprivileges than its parent process. This bool directly controls if\\nthe no_new_privs flag will be set on the container process.\\nAllowPrivilegeEscalation is true always when the container is:\\n1) run as Privileged\\n2) has CAP_SYS_ADMIN\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"AllowPrivilegeEscalation\"\n" +
		"        },\n" +
		"        \"capabilities\": {\n" +
		"          \"$ref\": \"#/definitions/v1Capabilities\"\n" +
		"        },\n" +
		"        \"privileged\": {\n" +
		"          \"description\": \"Run container in privileged mode.\\nProcesses in privileged containers are essentially equivalent to root on the host.\\nDefaults to false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"Privileged\"\n" +
		"        },\n" +
		"        \"procMount\": {\n" +
		"          \"$ref\": \"#/definitions/v1ProcMountType\"\n" +
		"        },\n" +
		"        \"readOnlyRootFilesystem\": {\n" +
		"          \"description\": \"Whether this container has a read-only root filesystem.\\nDefault is false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnlyRootFilesystem\"\n" +
		"        },\n" +
		"        \"runAsGroup\": {\n" +
		"          \"description\": \"The GID to run the entrypoint of the container process.\\nUses runtime default if unset.\\nMay also be set in PodSecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"RunAsGroup\"\n" +
		"        },\n" +
		"        \"runAsNonRoot\": {\n" +
		"          \"description\": \"Indicates that the container must run as a non-root user.\\nIf true, the Kubelet will validate the image at runtime to ensure that it\\ndoes not run as UID 0 (root) and fail to start the container if it does.\\nIf unset or false, no such validation will be performed.\\nMay also be set in PodSecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"RunAsNonRoot\"\n" +
		"        },\n" +
		"        \"runAsUser\": {\n" +
		"          \"description\": \"The UID to run the entrypoint of the container process.\\nDefaults to user specified in image metadata if unspecified.\\nMay also be set in PodSecurityContext.  If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"RunAsUser\"\n" +
		"        },\n" +
		"        \"seLinuxOptions\": {\n" +
		"          \"$ref\": \"#/definitions/v1SELinuxOptions\"\n" +
		"        },\n" +
		"        \"seccompProfile\": {\n" +
		"          \"$ref\": \"#/definitions/v1SeccompProfile\"\n" +
		"        },\n" +
		"        \"windowsOptions\": {\n" +
		"          \"$ref\": \"#/definitions/v1WindowsSecurityContextOptions\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1Service\": {\n" +
		"      \"description\": \"Service is a named abstraction of software service (for example, mysql) consisting of local port\\n(for example 3306) that the proxy listens on, and the selector that determines which pods\\nwill answer requests sent through the proxy.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"metadata\": {\n" +
		"          \"title\": \"Standard object's metadata.\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1ObjectMeta\"\n" +
		"        },\n" +
		"        \"spec\": {\n" +
		"          \"title\": \"Spec defines the behavior of a service.\\nhttps://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1ServiceSpec\"\n" +
		"        },\n" +
		"        \"status\": {\n" +
		"          \"title\": \"Most recently observed status of the service.\\nPopulated by the system.\\nRead-only.\\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1ServiceStatus\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1ServiceAccountTokenProjection\": {\n" +
		"      \"description\": \"ServiceAccountTokenProjection represents a projected service account token\\nvolume. This projection can be used to insert a service account token into\\nthe pods runtime filesystem for use against APIs (Kubernetes API Server or\\notherwise).\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"audience\": {\n" +
		"          \"description\": \"Audience is the intended audience of the token. A recipient of a token\\nmust identify itself with an identifier specified in the audience of the\\ntoken, and otherwise should reject the token. The audience defaults to the\\nidentifier of the apiserver.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Audience\"\n" +
		"        },\n" +
		"        \"expirationSeconds\": {\n" +
		"          \"description\": \"ExpirationSeconds is the requested duration of validity of the service\\naccount token. As the token approaches expiration, the kubelet volume\\nplugin will proactively rotate the service account token. The kubelet will\\nstart trying to rotate the token if the token is older than 80 percent of\\nits time to live or if the token is older than 24 hours.Defaults to 1 hour\\nand must be at least 10 minutes.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"ExpirationSeconds\"\n" +
		"        },\n" +
		"        \"path\": {\n" +
		"          \"description\": \"Path is the path relative to the mount point of the file to project the\\ntoken into.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Path\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1ServiceBackendPort\": {\n" +
		"      \"description\": \"ServiceBackendPort is the service port being referenced.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Name is the name of the port on the Service.\\nThis is a mutually exclusive setting with \\\"Number\\\".\\n+optional\"\n" +
		"        },\n" +
		"        \"number\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"Number is the numerical port number (e.g. 80) on the Service.\\nThis is a mutually exclusive setting with \\\"Name\\\".\\n+optional\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1ServicePort\": {\n" +
		"      \"description\": \"ServicePort contains information on service's port.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"appProtocol\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The application protocol for this port.\\nThis field follows standard Kubernetes label syntax.\\nUn-prefixed names are reserved for IANA standard service names (as per\\nRFC-6335 and http://www.iana.org/assignments/service-names).\\nNon-standard protocols should use prefixed names such as\\nmycompany.com/my-custom-protocol.\\n+optional\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The name of this port within the service. This must be a DNS_LABEL.\\nAll ports within a ServiceSpec must have unique names. When considering\\nthe endpoints for a Service, this must match the 'name' field in the\\nEndpointPort.\\nOptional if only one ServicePort is defined on this service.\\n+optional\"\n" +
		"        },\n" +
		"        \"nodePort\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"The port on each node on which this service is exposed when type is\\nNodePort or LoadBalancer.  Usually assigned by the system. If a value is\\nspecified, in-range, and not in use it will be used, otherwise the\\noperation will fail.  If not specified, a port will be allocated if this\\nService requires one.  If this field is specified when creating a\\nService which does not need it, creation will fail. This field will be\\nwiped when updating a Service to no longer need it (e.g. changing type\\nfrom NodePort to ClusterIP).\\nMore info: https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport\\n+optional\"\n" +
		"        },\n" +
		"        \"port\": {\n" +
		"          \"description\": \"The port that will be exposed by this service.\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\"\n" +
		"        },\n" +
		"        \"protocol\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"The IP protocol for this port. Supports \\\"TCP\\\", \\\"UDP\\\", and \\\"SCTP\\\".\\nDefault is TCP.\\n+default=\\\"TCP\\\"\\n+optional\"\n" +
		"        },\n" +
		"        \"targetPort\": {\n" +
		"          \"title\": \"Number or name of the port to access on the pods targeted by the service.\\nNumber must be in the range 1 to 65535. Name must be an IANA_SVC_NAME.\\nIf this is a string, it will be looked up as a named port in the\\ntarget Pod's container ports. If this is not specified, the value\\nof the 'port' field is used (an identity map).\\nThis field is ignored for services with clusterIP=None, and should be\\nomitted or set equal to the 'port' field.\\nMore info: https://kubernetes.io/docs/concepts/services-networking/service/#defining-a-service\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/intstrIntOrString\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1ServiceSpec\": {\n" +
		"      \"description\": \"ServiceSpec describes the attributes that a user creates on a service.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"allocateLoadBalancerNodePorts\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"title\": \"allocateLoadBalancerNodePorts defines if NodePorts will be automatically\\nallocated for services with type LoadBalancer.  Default is \\\"true\\\". It\\nmay be set to \\\"false\\\" if the cluster load-balancer does not rely on\\nNodePorts.  If the caller requests specific NodePorts (by specifying a\\nvalue), those requests will be respected, regardless of this field.\\nThis field may only be set for services with type LoadBalancer and will\\nbe cleared if the type is changed to any other type.\\nThis field is beta-level and is only honored by servers that enable the ServiceLBNodePortControl feature.\\n+featureGate=ServiceLBNodePortControl\\n+optional\"\n" +
		"        },\n" +
		"        \"clusterIP\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"clusterIP is the IP address of the service and is usually assigned\\nrandomly. If an address is specified manually, is in-range (as per\\nsystem configuration), and is not in use, it will be allocated to the\\nservice; otherwise creation of the service will fail. This field may not\\nbe changed through updates unless the type field is also being changed\\nto ExternalName (which requires this field to be blank) or the type\\nfield is being changed from ExternalName (in which case this field may\\noptionally be specified, as describe above).  Valid values are \\\"None\\\",\\nempty string (\\\"\\\"), or a valid IP address. Setting this to \\\"None\\\" makes a\\n\\\"headless service\\\" (no virtual IP), which is useful when direct endpoint\\nconnections are preferred and proxying is not required.  Only applies to\\ntypes ClusterIP, NodePort, and LoadBalancer. If this field is specified\\nwhen creating a Service of type ExternalName, creation will fail. This\\nfield will be wiped when updating a Service to type ExternalName.\\nMore info: https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies\\n+optional\"\n" +
		"        },\n" +
		"        \"clusterIPs\": {\n" +
		"          \"description\": \"ClusterIPs is a list of IP addresses assigned to this service, and are\\nusually assigned randomly.  If an address is specified manually, is\\nin-range (as per system configuration), and is not in use, it will be\\nallocated to the service; otherwise creation of the service will fail.\\nThis field may not be changed through updates unless the type field is\\nalso being changed to ExternalName (which requires this field to be\\nempty) or the type field is being changed from ExternalName (in which\\ncase this field may optionally be specified, as describe above).  Valid\\nvalues are \\\"None\\\", empty string (\\\"\\\"), or a valid IP address.  Setting\\nthis to \\\"None\\\" makes a \\\"headless service\\\" (no virtual IP), which is\\nuseful when direct endpoint connections are preferred and proxying is\\nnot required.  Only applies to types ClusterIP, NodePort, and\\nLoadBalancer. If this field is specified when creating a Service of type\\nExternalName, creation will fail. This field will be wiped when updating\\na Service to type ExternalName.  If this field is not specified, it will\\nbe initialized from the clusterIP field.  If this field is specified,\\nclients must ensure that clusterIPs[0] and clusterIP have the same\\nvalue.\\n\\nUnless the \\\"IPv6DualStack\\\" feature gate is enabled, this field is\\nlimited to one value, which must be the same as the clusterIP field.  If\\nthe feature gate is enabled, this field may hold a maximum of two\\nentries (dual-stack IPs, in either order).  These IPs must correspond to\\nthe values of the ipFamilies field. Both clusterIPs and ipFamilies are\\ngoverned by the ipFamilyPolicy field.\\nMore info: https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies\\n+listType=atomic\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"externalIPs\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"title\": \"externalIPs is a list of IP addresses for which nodes in the cluster\\nwill also accept traffic for this service.  These IPs are not managed by\\nKubernetes.  The user is responsible for ensuring that traffic arrives\\nat a node with this IP.  A common example is external load-balancers\\nthat are not part of the Kubernetes system.\\n+optional\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"externalName\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"externalName is the external reference that discovery mechanisms will\\nreturn as an alias for this service (e.g. a DNS CNAME record). No\\nproxying will be involved.  Must be a lowercase RFC-1123 hostname\\n(https://tools.ietf.org/html/rfc1123) and requires `type` to be \\\"ExternalName\\\".\\n+optional\"\n" +
		"        },\n" +
		"        \"externalTrafficPolicy\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"externalTrafficPolicy denotes if this Service desires to route external\\ntraffic to node-local or cluster-wide endpoints. \\\"Local\\\" preserves the\\nclient source IP and avoids a second hop for LoadBalancer and Nodeport\\ntype services, but risks potentially imbalanced traffic spreading.\\n\\\"Cluster\\\" obscures the client source IP and may cause a second hop to\\nanother node, but should have good overall load-spreading.\\n+optional\"\n" +
		"        },\n" +
		"        \"healthCheckNodePort\": {\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"title\": \"healthCheckNodePort specifies the healthcheck nodePort for the service.\\nThis only applies when type is set to LoadBalancer and\\nexternalTrafficPolicy is set to Local. If a value is specified, is\\nin-range, and is not in use, it will be used.  If not specified, a value\\nwill be automatically allocated.  External systems (e.g. load-balancers)\\ncan use this port to determine if a given node holds endpoints for this\\nservice or not.  If this field is specified when creating a Service\\nwhich does not need it, creation will fail. This field will be wiped\\nwhen updating a Service to no longer need it (e.g. changing type).\\n+optional\"\n" +
		"        },\n" +
		"        \"internalTrafficPolicy\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"InternalTrafficPolicy specifies if the cluster internal traffic\\nshould be routed to all endpoints or node-local endpoints only.\\n\\\"Cluster\\\" routes internal traffic to a Service to all endpoints.\\n\\\"Local\\\" routes traffic to node-local endpoints only, traffic is\\ndropped if no node-local endpoints are ready.\\nThe default value is \\\"Cluster\\\".\\n+featureGate=ServiceInternalTrafficPolicy\\n+optional\"\n" +
		"        },\n" +
		"        \"ipFamilies\": {\n" +
		"          \"description\": \"IPFamilies is a list of IP families (e.g. IPv4, IPv6) assigned to this\\nservice, and is gated by the \\\"IPv6DualStack\\\" feature gate.  This field\\nis usually assigned automatically based on cluster configuration and the\\nipFamilyPolicy field. If this field is specified manually, the requested\\nfamily is available in the cluster, and ipFamilyPolicy allows it, it\\nwill be used; otherwise creation of the service will fail.  This field\\nis conditionally mutable: it allows for adding or removing a secondary\\nIP family, but it does not allow changing the primary IP family of the\\nService.  Valid values are \\\"IPv4\\\" and \\\"IPv6\\\".  This field only applies\\nto Services of types ClusterIP, NodePort, and LoadBalancer, and does\\napply to \\\"headless\\\" services.  This field will be wiped when updating a\\nService to type ExternalName.\\n\\nThis field may hold a maximum of two entries (dual-stack families, in\\neither order).  These families must correspond to the values of the\\nclusterIPs field, if specified. Both clusterIPs and ipFamilies are\\ngoverned by the ipFamilyPolicy field.\\n+listType=atomic\\n+optional\",\n" +
		"          \"type\": \"array\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"ipFamilyPolicy\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"IPFamilyPolicy represents the dual-stack-ness requested or required by\\nthis Service, and is gated by the \\\"IPv6DualStack\\\" feature gate.  If\\nthere is no value provided, then this field will be set to SingleStack.\\nServices can be \\\"SingleStack\\\" (a single IP family), \\\"PreferDualStack\\\"\\n(two IP families on dual-stack configured clusters or a single IP family\\non single-stack clusters), or \\\"RequireDualStack\\\" (two IP families on\\ndual-stack configured clusters, otherwise fail). The ipFamilies and\\nclusterIPs fields depend on the value of this field.  This field will be\\nwiped when updating a service to type ExternalName.\\n+optional\"\n" +
		"        },\n" +
		"        \"loadBalancerClass\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"loadBalancerClass is the class of the load balancer implementation this Service belongs to.\\nIf specified, the value of this field must be a label-style identifier, with an optional prefix,\\ne.g. \\\"internal-vip\\\" or \\\"example.com/internal-vip\\\". Unprefixed names are reserved for end-users.\\nThis field can only be set when the Service type is 'LoadBalancer'. If not set, the default load\\nbalancer implementation is used, today this is typically done through the cloud provider integration,\\nbut should apply for any default implementation. If set, it is assumed that a load balancer\\nimplementation is watching for Services with a matching class. Any default load balancer\\nimplementation (e.g. cloud providers) should ignore Services that set this field.\\nThis field can only be set when creating or updating a Service to type 'LoadBalancer'.\\nOnce set, it can not be changed. This field will be wiped when a service is updated to a non 'LoadBalancer' type.\\n+featureGate=LoadBalancerClass\\n+optional\"\n" +
		"        },\n" +
		"        \"loadBalancerIP\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Only applies to Service Type: LoadBalancer\\nLoadBalancer will get created with the IP specified in this field.\\nThis feature depends on whether the underlying cloud-provider supports specifying\\nthe loadBalancerIP when a load balancer is created.\\nThis field will be ignored if the cloud-provider does not support the feature.\\n+optional\"\n" +
		"        },\n" +
		"        \"loadBalancerSourceRanges\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"title\": \"If specified and supported by the platform, this will restrict traffic through the cloud-provider\\nload-balancer will be restricted to the specified client IPs. This field will be ignored if the\\ncloud-provider does not support the feature.\\\"\\nMore info: https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/\\n+optional\",\n" +
		"          \"items\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"ports\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"title\": \"The list of ports that are exposed by this service.\\nMore info: https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies\\n+patchMergeKey=port\\n+patchStrategy=merge\\n+listType=map\\n+listMapKey=port\\n+listMapKey=protocol\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1ServicePort\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"publishNotReadyAddresses\": {\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"title\": \"publishNotReadyAddresses indicates that any agent which deals with endpoints for this\\nService should disregard any indications of ready/not-ready.\\nThe primary use case for setting this field is for a StatefulSet's Headless Service to\\npropagate SRV DNS records for its Pods for the purpose of peer discovery.\\nThe Kubernetes controllers that generate Endpoints and EndpointSlice resources for\\nServices interpret this to mean that all endpoints are considered \\\"ready\\\" even if the\\nPods themselves are not. Agents which consume only Kubernetes generated endpoints\\nthrough the Endpoints or EndpointSlice resources can safely assume this behavior.\\n+optional\"\n" +
		"        },\n" +
		"        \"selector\": {\n" +
		"          \"type\": \"object\",\n" +
		"          \"title\": \"Route service traffic to pods with label keys and values matching this\\nselector. If empty or not present, the service is assumed to have an\\nexternal process managing its endpoints, which Kubernetes will not\\nmodify. Only applies to types ClusterIP, NodePort, and LoadBalancer.\\nIgnored if type is ExternalName.\\nMore info: https://kubernetes.io/docs/concepts/services-networking/service/\\n+optional\\n+mapType=atomic\",\n" +
		"          \"additionalProperties\": {\n" +
		"            \"type\": \"string\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"sessionAffinity\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"Supports \\\"ClientIP\\\" and \\\"None\\\". Used to maintain session affinity.\\nEnable client IP based session affinity.\\nMust be ClientIP or None.\\nDefaults to None.\\nMore info: https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies\\n+optional\"\n" +
		"        },\n" +
		"        \"sessionAffinityConfig\": {\n" +
		"          \"title\": \"sessionAffinityConfig contains the configurations of session affinity.\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1SessionAffinityConfig\"\n" +
		"        },\n" +
		"        \"type\": {\n" +
		"          \"type\": \"string\",\n" +
		"          \"title\": \"type determines how the Service is exposed. Defaults to ClusterIP. Valid\\noptions are ExternalName, ClusterIP, NodePort, and LoadBalancer.\\n\\\"ClusterIP\\\" allocates a cluster-internal IP address for load-balancing\\nto endpoints. Endpoints are determined by the selector or if that is not\\nspecified, by manual construction of an Endpoints object or\\nEndpointSlice objects. If clusterIP is \\\"None\\\", no virtual IP is\\nallocated and the endpoints are published as a set of endpoints rather\\nthan a virtual IP.\\n\\\"NodePort\\\" builds on ClusterIP and allocates a port on every node which\\nroutes to the same endpoints as the clusterIP.\\n\\\"LoadBalancer\\\" builds on NodePort and creates an external load-balancer\\n(if supported in the current cloud) which routes to the same endpoints\\nas the clusterIP.\\n\\\"ExternalName\\\" aliases this service to the specified externalName.\\nSeveral other fields do not apply to ExternalName services.\\nMore info: https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types\\n+optional\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1ServiceStatus\": {\n" +
		"      \"description\": \"ServiceStatus represents the current status of a service.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"conditions\": {\n" +
		"          \"type\": \"array\",\n" +
		"          \"title\": \"Current service state\\n+optional\\n+patchMergeKey=type\\n+patchStrategy=merge\\n+listType=map\\n+listMapKey=type\",\n" +
		"          \"items\": {\n" +
		"            \"$ref\": \"#/definitions/v1Condition\"\n" +
		"          }\n" +
		"        },\n" +
		"        \"loadBalancer\": {\n" +
		"          \"title\": \"LoadBalancer contains the current status of the load-balancer,\\nif one is present.\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1LoadBalancerStatus\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1SessionAffinityConfig\": {\n" +
		"      \"description\": \"SessionAffinityConfig represents the configurations of session affinity.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"clientIP\": {\n" +
		"          \"title\": \"clientIP contains the configurations of Client IP based session affinity.\\n+optional\",\n" +
		"          \"$ref\": \"#/definitions/v1ClientIPConfig\"\n" +
		"        }\n" +
		"      }\n" +
		"    },\n" +
		"    \"v1StorageMedium\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"StorageMedium defines ways that storage can be allocated to a volume.\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1StorageOSVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a StorageOS persistent volume resource.\",\n" +
		"      \"properties\": {\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Defaults to false (read/write). ReadOnly here will force\\nthe ReadOnly setting in VolumeMounts.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"secretRef\": {\n" +
		"          \"$ref\": \"#/definitions/v1LocalObjectReference\"\n" +
		"        },\n" +
		"        \"volumeName\": {\n" +
		"          \"description\": \"VolumeName is the human-readable name of the StorageOS volume.  Volume\\nnames are only unique within a namespace.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"VolumeName\"\n" +
		"        },\n" +
		"        \"volumeNamespace\": {\n" +
		"          \"description\": \"VolumeNamespace specifies the scope of the volume within StorageOS.  If no\\nnamespace is specified then the Pod's namespace will be used.  This allows the\\nKubernetes name scoping to be mirrored within StorageOS for tighter integration.\\nSet VolumeName to any name to override the default behaviour.\\nSet to \\\"default\\\" if you are not using namespaces within StorageOS.\\nNamespaces that do not pre-exist within StorageOS will be created.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"VolumeNamespace\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1Sysctl\": {\n" +
		"      \"description\": \"Sysctl defines a kernel parameter to be set\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name of a property to set\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"description\": \"Value of a property to set\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Value\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1TCPSocketAction\": {\n" +
		"      \"description\": \"TCPSocketAction describes an action based on opening a socket\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"host\": {\n" +
		"          \"description\": \"Optional: Host name to connect to, defaults to the pod IP.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Host\"\n" +
		"        },\n" +
		"        \"port\": {\n" +
		"          \"$ref\": \"#/definitions/intstrIntOrString\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1TaintEffect\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1TerminationMessagePolicy\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"TerminationMessagePolicy describes how termination messages are retrieved from a container.\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1Time\": {\n" +
		"      \"description\": \"Programs using times should typically store and pass them as values,\\nnot pointers. That is, time variables and struct fields should be of\\ntype time.Time, not *time.Time.\\n\\nA Time value can be used by multiple goroutines simultaneously except\\nthat the methods GobDecode, UnmarshalBinary, UnmarshalJSON and\\nUnmarshalText are not concurrency-safe.\\n\\nTime instants can be compared using the Before, After, and Equal methods.\\nThe Sub method subtracts two instants, producing a Duration.\\nThe Add method adds a Time and a Duration, producing a Time.\\n\\nThe zero value of type Time is January 1, year 1, 00:00:00.000000000 UTC.\\nAs this time is unlikely to come up in practice, the IsZero method gives\\na simple way of detecting a time that has not been initialized explicitly.\\n\\nEach Time has associated with it a Location, consulted when computing the\\npresentation form of the time, such as in the Format, Hour, and Year methods.\\nThe methods Local, UTC, and In return a Time with a specific location.\\nChanging the location in this way changes only the presentation; it does not\\nchange the instant in time being denoted and therefore does not affect the\\ncomputations described in earlier paragraphs.\\n\\nRepresentations of a Time value saved by the GobEncode, MarshalBinary,\\nMarshalJSON, and MarshalText methods store the Time.Location's offset, but not\\nthe location name. They therefore lose information about Daylight Saving Time.\\n\\nIn addition to the required “wall clock” reading, a Time may contain an optional\\nreading of the current process's monotonic clock, to provide additional precision\\nfor comparison or subtraction.\\nSee the “Monotonic Clocks” section in the package documentation for details.\\n\\nNote that the Go == operator compares not just the time instant but also the\\nLocation and the monotonic clock reading. Therefore, Time values should not\\nbe used as map or database keys without first guaranteeing that the\\nidentical Location has been set for all values, which can be achieved\\nthrough use of the UTC or Local method, and that the monotonic clock reading\\nhas been stripped by setting t = t.Round(0). In general, prefer t.Equal(u)\\nto t == u, since t.Equal uses the most accurate comparison available and\\ncorrectly handles the case when only one of its arguments has a monotonic\\nclock reading.\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"format\": \"date-time\",\n" +
		"      \"title\": \"A Time represents an instant in time with nanosecond precision.\",\n" +
		"      \"x-go-package\": \"k8s.io/apimachinery/pkg/apis/meta/v1\"\n" +
		"    },\n" +
		"    \"v1Toleration\": {\n" +
		"      \"description\": \"The pod this Toleration is attached to tolerates any taint that matches\\nthe triple \\u003ckey,value,effect\\u003e using the matching operator \\u003coperator\\u003e.\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"effect\": {\n" +
		"          \"$ref\": \"#/definitions/v1TaintEffect\"\n" +
		"        },\n" +
		"        \"key\": {\n" +
		"          \"description\": \"Key is the taint key that the toleration applies to. Empty means match all taint keys.\\nIf the key is empty, operator must be Exists; this combination means to match all values and all keys.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Key\"\n" +
		"        },\n" +
		"        \"operator\": {\n" +
		"          \"$ref\": \"#/definitions/v1TolerationOperator\"\n" +
		"        },\n" +
		"        \"tolerationSeconds\": {\n" +
		"          \"description\": \"TolerationSeconds represents the period of time the toleration (which must be\\nof effect NoExecute, otherwise this field is ignored) tolerates the taint. By default,\\nit is not set, which means tolerate the taint forever (do not evict). Zero and\\nnegative values will be treated as 0 (evict immediately) by the system.\\n+optional\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int64\",\n" +
		"          \"x-go-name\": \"TolerationSeconds\"\n" +
		"        },\n" +
		"        \"value\": {\n" +
		"          \"description\": \"Value is the taint value the toleration matches to.\\nIf the operator is Exists, the value should be empty, otherwise just a regular string.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Value\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1TolerationOperator\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"title\": \"A toleration operator is the set of operators that can be used in a toleration.\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1TopologySpreadConstraint\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"TopologySpreadConstraint specifies how to spread matching pods among the given topology.\",\n" +
		"      \"properties\": {\n" +
		"        \"labelSelector\": {\n" +
		"          \"$ref\": \"#/definitions/v1LabelSelector\"\n" +
		"        },\n" +
		"        \"maxSkew\": {\n" +
		"          \"description\": \"MaxSkew describes the degree to which pods may be unevenly distributed.\\nWhen `whenUnsatisfiable=DoNotSchedule`, it is the maximum permitted difference\\nbetween the number of matching pods in the target topology and the global minimum.\\nFor example, in a 3-zone cluster, MaxSkew is set to 1, and pods with the same\\nlabelSelector spread as 1/1/0:\\n+-------+-------+-------+\\n zone1 | zone2 | zone3 |\\n+-------+-------+-------+\\n   P   |   P   |       |\\n+-------+-------+-------+\\nif MaxSkew is 1, incoming pod can only be scheduled to zone3 to become 1/1/1;\\nscheduling it onto zone1(zone2) would make the ActualSkew(2-0) on zone1(zone2)\\nviolate MaxSkew(1).\\nif MaxSkew is 2, incoming pod can be scheduled onto any zone.\\nWhen `whenUnsatisfiable=ScheduleAnyway`, it is used to give higher precedence\\nto topologies that satisfy it.\\nIt's a required field. Default value is 1 and 0 is not allowed.\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"MaxSkew\"\n" +
		"        },\n" +
		"        \"topologyKey\": {\n" +
		"          \"description\": \"TopologyKey is the key of node labels. Nodes that have a label with this key\\nand identical values are considered to be in the same topology.\\nWe consider each \\u003ckey, value\\u003e as a \\\"bucket\\\", and try to put balanced number\\nof pods into each bucket.\\nIt's a required field.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"TopologyKey\"\n" +
		"        },\n" +
		"        \"whenUnsatisfiable\": {\n" +
		"          \"$ref\": \"#/definitions/v1UnsatisfiableConstraintAction\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1TypedLocalObjectReference\": {\n" +
		"      \"description\": \"TypedLocalObjectReference contains enough information to let you locate the\\ntyped referenced object inside the same namespace.\\n+structType=atomic\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"apiGroup\": {\n" +
		"          \"description\": \"APIGroup is the group for the resource being referenced.\\nIf APIGroup is not specified, the specified Kind must be in the core API group.\\nFor any other third-party types, APIGroup is required.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"APIGroup\"\n" +
		"        },\n" +
		"        \"kind\": {\n" +
		"          \"description\": \"Kind is the type of resource being referenced\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Kind\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Name is the name of resource being referenced\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1URIScheme\": {\n" +
		"      \"description\": \"URIScheme identifies the scheme used for connection to a host for Get actions\",\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1UnsatisfiableConstraintAction\": {\n" +
		"      \"type\": \"string\",\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1Volume\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Volume represents a named volume in a pod that may be accessed by any container in the pod.\",\n" +
		"      \"properties\": {\n" +
		"        \"awsElasticBlockStore\": {\n" +
		"          \"$ref\": \"#/definitions/v1AWSElasticBlockStoreVolumeSource\"\n" +
		"        },\n" +
		"        \"azureDisk\": {\n" +
		"          \"$ref\": \"#/definitions/v1AzureDiskVolumeSource\"\n" +
		"        },\n" +
		"        \"azureFile\": {\n" +
		"          \"$ref\": \"#/definitions/v1AzureFileVolumeSource\"\n" +
		"        },\n" +
		"        \"cephfs\": {\n" +
		"          \"$ref\": \"#/definitions/v1CephFSVolumeSource\"\n" +
		"        },\n" +
		"        \"cinder\": {\n" +
		"          \"$ref\": \"#/definitions/v1CinderVolumeSource\"\n" +
		"        },\n" +
		"        \"configMap\": {\n" +
		"          \"$ref\": \"#/definitions/v1ConfigMapVolumeSource\"\n" +
		"        },\n" +
		"        \"csi\": {\n" +
		"          \"$ref\": \"#/definitions/v1CSIVolumeSource\"\n" +
		"        },\n" +
		"        \"downwardAPI\": {\n" +
		"          \"$ref\": \"#/definitions/v1DownwardAPIVolumeSource\"\n" +
		"        },\n" +
		"        \"emptyDir\": {\n" +
		"          \"$ref\": \"#/definitions/v1EmptyDirVolumeSource\"\n" +
		"        },\n" +
		"        \"ephemeral\": {\n" +
		"          \"$ref\": \"#/definitions/v1EphemeralVolumeSource\"\n" +
		"        },\n" +
		"        \"fc\": {\n" +
		"          \"$ref\": \"#/definitions/v1FCVolumeSource\"\n" +
		"        },\n" +
		"        \"flexVolume\": {\n" +
		"          \"$ref\": \"#/definitions/v1FlexVolumeSource\"\n" +
		"        },\n" +
		"        \"flocker\": {\n" +
		"          \"$ref\": \"#/definitions/v1FlockerVolumeSource\"\n" +
		"        },\n" +
		"        \"gcePersistentDisk\": {\n" +
		"          \"$ref\": \"#/definitions/v1GCEPersistentDiskVolumeSource\"\n" +
		"        },\n" +
		"        \"gitRepo\": {\n" +
		"          \"$ref\": \"#/definitions/v1GitRepoVolumeSource\"\n" +
		"        },\n" +
		"        \"glusterfs\": {\n" +
		"          \"$ref\": \"#/definitions/v1GlusterfsVolumeSource\"\n" +
		"        },\n" +
		"        \"hostPath\": {\n" +
		"          \"$ref\": \"#/definitions/v1HostPathVolumeSource\"\n" +
		"        },\n" +
		"        \"iscsi\": {\n" +
		"          \"$ref\": \"#/definitions/v1ISCSIVolumeSource\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"Volume's name.\\nMust be a DNS_LABEL and unique within the pod.\\nMore info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"nfs\": {\n" +
		"          \"$ref\": \"#/definitions/v1NFSVolumeSource\"\n" +
		"        },\n" +
		"        \"persistentVolumeClaim\": {\n" +
		"          \"$ref\": \"#/definitions/v1PersistentVolumeClaimVolumeSource\"\n" +
		"        },\n" +
		"        \"photonPersistentDisk\": {\n" +
		"          \"$ref\": \"#/definitions/v1PhotonPersistentDiskVolumeSource\"\n" +
		"        },\n" +
		"        \"portworxVolume\": {\n" +
		"          \"$ref\": \"#/definitions/v1PortworxVolumeSource\"\n" +
		"        },\n" +
		"        \"projected\": {\n" +
		"          \"$ref\": \"#/definitions/v1ProjectedVolumeSource\"\n" +
		"        },\n" +
		"        \"quobyte\": {\n" +
		"          \"$ref\": \"#/definitions/v1QuobyteVolumeSource\"\n" +
		"        },\n" +
		"        \"rbd\": {\n" +
		"          \"$ref\": \"#/definitions/v1RBDVolumeSource\"\n" +
		"        },\n" +
		"        \"scaleIO\": {\n" +
		"          \"$ref\": \"#/definitions/v1ScaleIOVolumeSource\"\n" +
		"        },\n" +
		"        \"secret\": {\n" +
		"          \"$ref\": \"#/definitions/v1SecretVolumeSource\"\n" +
		"        },\n" +
		"        \"storageos\": {\n" +
		"          \"$ref\": \"#/definitions/v1StorageOSVolumeSource\"\n" +
		"        },\n" +
		"        \"vsphereVolume\": {\n" +
		"          \"$ref\": \"#/definitions/v1VsphereVirtualDiskVolumeSource\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1VolumeDevice\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"volumeDevice describes a mapping of a raw block device within a container.\",\n" +
		"      \"properties\": {\n" +
		"        \"devicePath\": {\n" +
		"          \"description\": \"devicePath is the path inside of the container that the device will be mapped to.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"DevicePath\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"name must match the name of a persistentVolumeClaim in the pod\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1VolumeMount\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"VolumeMount describes a mounting of a Volume within a container.\",\n" +
		"      \"properties\": {\n" +
		"        \"mountPath\": {\n" +
		"          \"description\": \"Path within the container at which the volume should be mounted.  Must\\nnot contain ':'.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"MountPath\"\n" +
		"        },\n" +
		"        \"mountPropagation\": {\n" +
		"          \"$ref\": \"#/definitions/v1MountPropagationMode\"\n" +
		"        },\n" +
		"        \"name\": {\n" +
		"          \"description\": \"This must match the Name of a Volume.\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"Name\"\n" +
		"        },\n" +
		"        \"readOnly\": {\n" +
		"          \"description\": \"Mounted read-only if true, read-write otherwise (false or unspecified).\\nDefaults to false.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"ReadOnly\"\n" +
		"        },\n" +
		"        \"subPath\": {\n" +
		"          \"description\": \"Path within the volume from which the container's volume should be mounted.\\nDefaults to \\\"\\\" (volume's root).\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"SubPath\"\n" +
		"        },\n" +
		"        \"subPathExpr\": {\n" +
		"          \"description\": \"Expanded path within the volume from which the container's volume should be mounted.\\nBehaves similarly to SubPath but environment variable references $(VAR_NAME) are expanded using the container's environment.\\nDefaults to \\\"\\\" (volume's root).\\nSubPathExpr and SubPath are mutually exclusive.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"SubPathExpr\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1VolumeProjection\": {\n" +
		"      \"description\": \"Projection that may be projected along with other supported volume types\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"configMap\": {\n" +
		"          \"$ref\": \"#/definitions/v1ConfigMapProjection\"\n" +
		"        },\n" +
		"        \"downwardAPI\": {\n" +
		"          \"$ref\": \"#/definitions/v1DownwardAPIProjection\"\n" +
		"        },\n" +
		"        \"secret\": {\n" +
		"          \"$ref\": \"#/definitions/v1SecretProjection\"\n" +
		"        },\n" +
		"        \"serviceAccountToken\": {\n" +
		"          \"$ref\": \"#/definitions/v1ServiceAccountTokenProjection\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1VsphereVirtualDiskVolumeSource\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"Represents a vSphere volume resource.\",\n" +
		"      \"properties\": {\n" +
		"        \"fsType\": {\n" +
		"          \"description\": \"Filesystem type to mount.\\nMust be a filesystem type supported by the host operating system.\\nEx. \\\"ext4\\\", \\\"xfs\\\", \\\"ntfs\\\". Implicitly inferred to be \\\"ext4\\\" if unspecified.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"FSType\"\n" +
		"        },\n" +
		"        \"storagePolicyID\": {\n" +
		"          \"description\": \"Storage Policy Based Management (SPBM) profile ID associated with the StoragePolicyName.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"StoragePolicyID\"\n" +
		"        },\n" +
		"        \"storagePolicyName\": {\n" +
		"          \"description\": \"Storage Policy Based Management (SPBM) profile name.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"StoragePolicyName\"\n" +
		"        },\n" +
		"        \"volumePath\": {\n" +
		"          \"description\": \"Path that identifies vSphere volume vmdk\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"VolumePath\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1WeightedPodAffinityTerm\": {\n" +
		"      \"description\": \"The weights of all of the matched WeightedPodAffinityTerm fields are added per-node to find the most preferred node(s)\",\n" +
		"      \"type\": \"object\",\n" +
		"      \"properties\": {\n" +
		"        \"podAffinityTerm\": {\n" +
		"          \"$ref\": \"#/definitions/v1PodAffinityTerm\"\n" +
		"        },\n" +
		"        \"weight\": {\n" +
		"          \"description\": \"weight associated with matching the corresponding podAffinityTerm,\\nin the range 1-100.\",\n" +
		"          \"type\": \"integer\",\n" +
		"          \"format\": \"int32\",\n" +
		"          \"x-go-name\": \"Weight\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    },\n" +
		"    \"v1WindowsSecurityContextOptions\": {\n" +
		"      \"type\": \"object\",\n" +
		"      \"title\": \"WindowsSecurityContextOptions contain Windows-specific options and credentials.\",\n" +
		"      \"properties\": {\n" +
		"        \"gmsaCredentialSpec\": {\n" +
		"          \"description\": \"GMSACredentialSpec is where the GMSA admission webhook\\n(https://github.com/kubernetes-sigs/windows-gmsa) inlines the contents of the\\nGMSA credential spec named by the GMSACredentialSpecName field.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"GMSACredentialSpec\"\n" +
		"        },\n" +
		"        \"gmsaCredentialSpecName\": {\n" +
		"          \"description\": \"GMSACredentialSpecName is the name of the GMSA credential spec to use.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"GMSACredentialSpecName\"\n" +
		"        },\n" +
		"        \"hostProcess\": {\n" +
		"          \"description\": \"HostProcess determines if a container should be run as a 'Host Process' container.\\nThis field is alpha-level and will only be honored by components that enable the\\nWindowsHostProcessContainers feature flag. Setting this field without the feature\\nflag will result in errors when validating the Pod. All of a Pod's containers must\\nhave the same effective HostProcess value (it is not allowed to have a mix of HostProcess\\ncontainers and non-HostProcess containers).  In addition, if HostProcess is true\\nthen HostNetwork must also be set to true.\\n+optional\",\n" +
		"          \"type\": \"boolean\",\n" +
		"          \"x-go-name\": \"HostProcess\"\n" +
		"        },\n" +
		"        \"runAsUserName\": {\n" +
		"          \"description\": \"The UserName in Windows to run the entrypoint of the container process.\\nDefaults to the user specified in image metadata if unspecified.\\nMay also be set in PodSecurityContext. If set in both SecurityContext and\\nPodSecurityContext, the value specified in SecurityContext takes precedence.\\n+optional\",\n" +
		"          \"type\": \"string\",\n" +
		"          \"x-go-name\": \"RunAsUserName\"\n" +
		"        }\n" +
		"      },\n" +
		"      \"x-go-package\": \"k8s.io/api/core/v1\"\n" +
		"    }\n" +
		"  }\n" +
		"}"
	return tmpl
}
