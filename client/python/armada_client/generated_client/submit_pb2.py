# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: armada_client/generated_client/submit.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from k8s.io.api.core.v1 import generated_pb2 as k8s_dot_io_dot_api_dot_core_dot_v1_dot_generated__pb2
from google.api import annotations_pb2 as google_dot_api_dot_annotations__pb2
from github.com.gogo.protobuf.gogoproto import gogo_pb2 as github_dot_com_dot_gogo_dot_protobuf_dot_gogoproto_dot_gogo__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n+armada_client/generated_client/submit.proto\x12\x03\x61pi\x1a\x1bgoogle/protobuf/empty.proto\x1a\"k8s.io/api/core/v1/generated.proto\x1a\x1cgoogle/api/annotations.proto\x1a-github.com/gogo/protobuf/gogoproto/gogo.proto\"\xe7\x04\n\x14JobSubmitRequestItem\x12\x10\n\x08priority\x18\x01 \x01(\x01\x12\x11\n\tnamespace\x18\x03 \x01(\t\x12\x11\n\tclient_id\x18\x08 \x01(\t\x12\x35\n\x06labels\x18\x04 \x03(\x0b\x32%.api.JobSubmitRequestItem.LabelsEntry\x12?\n\x0b\x61nnotations\x18\x05 \x03(\x0b\x32*.api.JobSubmitRequestItem.AnnotationsEntry\x12S\n\x14required_node_labels\x18\x06 \x03(\x0b\x32\x31.api.JobSubmitRequestItem.RequiredNodeLabelsEntryB\x02\x18\x01\x12\x31\n\x08pod_spec\x18\x02 \x01(\x0b\x32\x1b.k8s.io.api.core.v1.PodSpecB\x02\x18\x01\x12.\n\tpod_specs\x18\x07 \x03(\x0b\x32\x1b.k8s.io.api.core.v1.PodSpec\x12#\n\x07ingress\x18\t \x03(\x0b\x32\x12.api.IngressConfig\x12$\n\x08services\x18\n \x03(\x0b\x32\x12.api.ServiceConfig\x1a-\n\x0bLabelsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a\x32\n\x10\x41nnotationsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a\x39\n\x17RequiredNodeLabelsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\xef\x01\n\rIngressConfig\x12\"\n\x04type\x18\x01 \x01(\x0e\x32\x10.api.IngressTypeB\x02\x18\x01\x12\r\n\x05ports\x18\x02 \x03(\r\x12\x38\n\x0b\x61nnotations\x18\x03 \x03(\x0b\x32#.api.IngressConfig.AnnotationsEntry\x12\x13\n\x0btls_enabled\x18\x04 \x01(\x08\x12\x11\n\tcert_name\x18\x05 \x01(\t\x12\x15\n\ruse_clusterIP\x18\x06 \x01(\x08\x1a\x32\n\x10\x41nnotationsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\">\n\rServiceConfig\x12\x1e\n\x04type\x18\x01 \x01(\x0e\x32\x10.api.ServiceType\x12\r\n\x05ports\x18\x02 \x03(\r\"k\n\x10JobSubmitRequest\x12\r\n\x05queue\x18\x01 \x01(\t\x12\x12\n\njob_set_id\x18\x02 \x01(\t\x12\x34\n\x11job_request_items\x18\x03 \x03(\x0b\x32\x19.api.JobSubmitRequestItem\"E\n\x10JobCancelRequest\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x12\n\njob_set_id\x18\x02 \x01(\t\x12\r\n\x05queue\x18\x03 \x01(\t\"b\n\x16JobReprioritizeRequest\x12\x0f\n\x07job_ids\x18\x01 \x03(\t\x12\x12\n\njob_set_id\x18\x02 \x01(\t\x12\r\n\x05queue\x18\x03 \x01(\t\x12\x14\n\x0cnew_priority\x18\x04 \x01(\x01\"\xb6\x01\n\x17JobReprioritizeResponse\x12[\n\x18reprioritization_results\x18\x01 \x03(\x0b\x32\x39.api.JobReprioritizeResponse.ReprioritizationResultsEntry\x1a>\n\x1cReprioritizationResultsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"6\n\x15JobSubmitResponseItem\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\r\n\x05\x65rror\x18\x02 \x01(\t\"K\n\x11JobSubmitResponse\x12\x36\n\x12job_response_items\x18\x01 \x03(\x0b\x32\x1a.api.JobSubmitResponseItem\"\xed\x02\n\x05Queue\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x17\n\x0fpriority_factor\x18\x02 \x01(\x01\x12\x13\n\x0buser_owners\x18\x03 \x03(\t\x12\x14\n\x0cgroup_owners\x18\x04 \x03(\t\x12\x37\n\x0fresource_limits\x18\x05 \x03(\x0b\x32\x1e.api.Queue.ResourceLimitsEntry\x12+\n\x0bpermissions\x18\x06 \x03(\x0b\x32\x16.api.Queue.Permissions\x1au\n\x0bPermissions\x12\x30\n\x08subjects\x18\x01 \x03(\x0b\x32\x1e.api.Queue.Permissions.Subject\x12\r\n\x05verbs\x18\x02 \x03(\t\x1a%\n\x07Subject\x12\x0c\n\x04kind\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x1a\x35\n\x13ResourceLimitsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x01:\x02\x38\x01\"=\n\x12\x43\x61ncellationResult\x12\'\n\rcancelled_ids\x18\x01 \x03(\tB\x10\xea\xde\x1f\x0c\x63\x61ncelledIds\"\x1f\n\x0fQueueGetRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\" \n\x10QueueInfoRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\"\"\n\x12QueueDeleteRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\"C\n\tQueueInfo\x12\x0c\n\x04name\x18\x01 \x01(\t\x12(\n\x0f\x61\x63tive_job_sets\x18\x02 \x03(\x0b\x32\x0f.api.JobSetInfo\"D\n\nJobSetInfo\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x13\n\x0bqueued_jobs\x18\x02 \x01(\x05\x12\x13\n\x0bleased_jobs\x18\x03 \x01(\x05*\x1a\n\x0bIngressType\x12\x0b\n\x07Ingress\x10\x00*)\n\x0bServiceType\x12\x0c\n\x08NodePort\x10\x00\x12\x0c\n\x08Headless\x10\x01\x32\xba\x05\n\x06Submit\x12V\n\nSubmitJobs\x12\x15.api.JobSubmitRequest\x1a\x16.api.JobSubmitResponse\"\x19\x82\xd3\xe4\x93\x02\x13\"\x0e/v1/job/submit:\x01*\x12W\n\nCancelJobs\x12\x15.api.JobCancelRequest\x1a\x17.api.CancellationResult\"\x19\x82\xd3\xe4\x93\x02\x13\"\x0e/v1/job/cancel:\x01*\x12n\n\x10ReprioritizeJobs\x12\x1b.api.JobReprioritizeRequest\x1a\x1c.api.JobReprioritizeResponse\"\x1f\x82\xd3\xe4\x93\x02\x19\"\x14/v1/job/reprioritize:\x01*\x12G\n\x0b\x43reateQueue\x12\n.api.Queue\x1a\x16.google.protobuf.Empty\"\x14\x82\xd3\xe4\x93\x02\x0e\"\t/v1/queue:\x01*\x12N\n\x0bUpdateQueue\x12\n.api.Queue\x1a\x16.google.protobuf.Empty\"\x1b\x82\xd3\xe4\x93\x02\x15\x1a\x10/v1/queue/{name}:\x01*\x12X\n\x0b\x44\x65leteQueue\x12\x17.api.QueueDeleteRequest\x1a\x16.google.protobuf.Empty\"\x18\x82\xd3\xe4\x93\x02\x12*\x10/v1/queue/{name}\x12\x46\n\x08GetQueue\x12\x14.api.QueueGetRequest\x1a\n.api.Queue\"\x18\x82\xd3\xe4\x93\x02\x12\x12\x10/v1/queue/{name}\x12T\n\x0cGetQueueInfo\x12\x15.api.QueueInfoRequest\x1a\x0e.api.QueueInfo\"\x1d\x82\xd3\xe4\x93\x02\x17\x12\x15/v1/queue/{name}/infoB\x08\xd8\xe1\x1e\x00\x80\xe2\x1e\x01\x62\x06proto3')

_INGRESSTYPE = DESCRIPTOR.enum_types_by_name['IngressType']
IngressType = enum_type_wrapper.EnumTypeWrapper(_INGRESSTYPE)
_SERVICETYPE = DESCRIPTOR.enum_types_by_name['ServiceType']
ServiceType = enum_type_wrapper.EnumTypeWrapper(_SERVICETYPE)
Ingress = 0
NodePort = 0
Headless = 1


_JOBSUBMITREQUESTITEM = DESCRIPTOR.message_types_by_name['JobSubmitRequestItem']
_JOBSUBMITREQUESTITEM_LABELSENTRY = _JOBSUBMITREQUESTITEM.nested_types_by_name['LabelsEntry']
_JOBSUBMITREQUESTITEM_ANNOTATIONSENTRY = _JOBSUBMITREQUESTITEM.nested_types_by_name['AnnotationsEntry']
_JOBSUBMITREQUESTITEM_REQUIREDNODELABELSENTRY = _JOBSUBMITREQUESTITEM.nested_types_by_name['RequiredNodeLabelsEntry']
_INGRESSCONFIG = DESCRIPTOR.message_types_by_name['IngressConfig']
_INGRESSCONFIG_ANNOTATIONSENTRY = _INGRESSCONFIG.nested_types_by_name['AnnotationsEntry']
_SERVICECONFIG = DESCRIPTOR.message_types_by_name['ServiceConfig']
_JOBSUBMITREQUEST = DESCRIPTOR.message_types_by_name['JobSubmitRequest']
_JOBCANCELREQUEST = DESCRIPTOR.message_types_by_name['JobCancelRequest']
_JOBREPRIORITIZEREQUEST = DESCRIPTOR.message_types_by_name['JobReprioritizeRequest']
_JOBREPRIORITIZERESPONSE = DESCRIPTOR.message_types_by_name['JobReprioritizeResponse']
_JOBREPRIORITIZERESPONSE_REPRIORITIZATIONRESULTSENTRY = _JOBREPRIORITIZERESPONSE.nested_types_by_name['ReprioritizationResultsEntry']
_JOBSUBMITRESPONSEITEM = DESCRIPTOR.message_types_by_name['JobSubmitResponseItem']
_JOBSUBMITRESPONSE = DESCRIPTOR.message_types_by_name['JobSubmitResponse']
_QUEUE = DESCRIPTOR.message_types_by_name['Queue']
_QUEUE_PERMISSIONS = _QUEUE.nested_types_by_name['Permissions']
_QUEUE_PERMISSIONS_SUBJECT = _QUEUE_PERMISSIONS.nested_types_by_name['Subject']
_QUEUE_RESOURCELIMITSENTRY = _QUEUE.nested_types_by_name['ResourceLimitsEntry']
_CANCELLATIONRESULT = DESCRIPTOR.message_types_by_name['CancellationResult']
_QUEUEGETREQUEST = DESCRIPTOR.message_types_by_name['QueueGetRequest']
_QUEUEINFOREQUEST = DESCRIPTOR.message_types_by_name['QueueInfoRequest']
_QUEUEDELETEREQUEST = DESCRIPTOR.message_types_by_name['QueueDeleteRequest']
_QUEUEINFO = DESCRIPTOR.message_types_by_name['QueueInfo']
_JOBSETINFO = DESCRIPTOR.message_types_by_name['JobSetInfo']
JobSubmitRequestItem = _reflection.GeneratedProtocolMessageType('JobSubmitRequestItem', (_message.Message,), {

  'LabelsEntry' : _reflection.GeneratedProtocolMessageType('LabelsEntry', (_message.Message,), {
    'DESCRIPTOR' : _JOBSUBMITREQUESTITEM_LABELSENTRY,
    '__module__' : 'armada_client.generated_client.submit_pb2'
    # @@protoc_insertion_point(class_scope:api.JobSubmitRequestItem.LabelsEntry)
    })
  ,

  'AnnotationsEntry' : _reflection.GeneratedProtocolMessageType('AnnotationsEntry', (_message.Message,), {
    'DESCRIPTOR' : _JOBSUBMITREQUESTITEM_ANNOTATIONSENTRY,
    '__module__' : 'armada_client.generated_client.submit_pb2'
    # @@protoc_insertion_point(class_scope:api.JobSubmitRequestItem.AnnotationsEntry)
    })
  ,

  'RequiredNodeLabelsEntry' : _reflection.GeneratedProtocolMessageType('RequiredNodeLabelsEntry', (_message.Message,), {
    'DESCRIPTOR' : _JOBSUBMITREQUESTITEM_REQUIREDNODELABELSENTRY,
    '__module__' : 'armada_client.generated_client.submit_pb2'
    # @@protoc_insertion_point(class_scope:api.JobSubmitRequestItem.RequiredNodeLabelsEntry)
    })
  ,
  'DESCRIPTOR' : _JOBSUBMITREQUESTITEM,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.JobSubmitRequestItem)
  })
_sym_db.RegisterMessage(JobSubmitRequestItem)
_sym_db.RegisterMessage(JobSubmitRequestItem.LabelsEntry)
_sym_db.RegisterMessage(JobSubmitRequestItem.AnnotationsEntry)
_sym_db.RegisterMessage(JobSubmitRequestItem.RequiredNodeLabelsEntry)

IngressConfig = _reflection.GeneratedProtocolMessageType('IngressConfig', (_message.Message,), {

  'AnnotationsEntry' : _reflection.GeneratedProtocolMessageType('AnnotationsEntry', (_message.Message,), {
    'DESCRIPTOR' : _INGRESSCONFIG_ANNOTATIONSENTRY,
    '__module__' : 'armada_client.generated_client.submit_pb2'
    # @@protoc_insertion_point(class_scope:api.IngressConfig.AnnotationsEntry)
    })
  ,
  'DESCRIPTOR' : _INGRESSCONFIG,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.IngressConfig)
  })
_sym_db.RegisterMessage(IngressConfig)
_sym_db.RegisterMessage(IngressConfig.AnnotationsEntry)

ServiceConfig = _reflection.GeneratedProtocolMessageType('ServiceConfig', (_message.Message,), {
  'DESCRIPTOR' : _SERVICECONFIG,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.ServiceConfig)
  })
_sym_db.RegisterMessage(ServiceConfig)

JobSubmitRequest = _reflection.GeneratedProtocolMessageType('JobSubmitRequest', (_message.Message,), {
  'DESCRIPTOR' : _JOBSUBMITREQUEST,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.JobSubmitRequest)
  })
_sym_db.RegisterMessage(JobSubmitRequest)

JobCancelRequest = _reflection.GeneratedProtocolMessageType('JobCancelRequest', (_message.Message,), {
  'DESCRIPTOR' : _JOBCANCELREQUEST,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.JobCancelRequest)
  })
_sym_db.RegisterMessage(JobCancelRequest)

JobReprioritizeRequest = _reflection.GeneratedProtocolMessageType('JobReprioritizeRequest', (_message.Message,), {
  'DESCRIPTOR' : _JOBREPRIORITIZEREQUEST,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.JobReprioritizeRequest)
  })
_sym_db.RegisterMessage(JobReprioritizeRequest)

JobReprioritizeResponse = _reflection.GeneratedProtocolMessageType('JobReprioritizeResponse', (_message.Message,), {

  'ReprioritizationResultsEntry' : _reflection.GeneratedProtocolMessageType('ReprioritizationResultsEntry', (_message.Message,), {
    'DESCRIPTOR' : _JOBREPRIORITIZERESPONSE_REPRIORITIZATIONRESULTSENTRY,
    '__module__' : 'armada_client.generated_client.submit_pb2'
    # @@protoc_insertion_point(class_scope:api.JobReprioritizeResponse.ReprioritizationResultsEntry)
    })
  ,
  'DESCRIPTOR' : _JOBREPRIORITIZERESPONSE,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.JobReprioritizeResponse)
  })
_sym_db.RegisterMessage(JobReprioritizeResponse)
_sym_db.RegisterMessage(JobReprioritizeResponse.ReprioritizationResultsEntry)

JobSubmitResponseItem = _reflection.GeneratedProtocolMessageType('JobSubmitResponseItem', (_message.Message,), {
  'DESCRIPTOR' : _JOBSUBMITRESPONSEITEM,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.JobSubmitResponseItem)
  })
_sym_db.RegisterMessage(JobSubmitResponseItem)

JobSubmitResponse = _reflection.GeneratedProtocolMessageType('JobSubmitResponse', (_message.Message,), {
  'DESCRIPTOR' : _JOBSUBMITRESPONSE,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.JobSubmitResponse)
  })
_sym_db.RegisterMessage(JobSubmitResponse)

Queue = _reflection.GeneratedProtocolMessageType('Queue', (_message.Message,), {

  'Permissions' : _reflection.GeneratedProtocolMessageType('Permissions', (_message.Message,), {

    'Subject' : _reflection.GeneratedProtocolMessageType('Subject', (_message.Message,), {
      'DESCRIPTOR' : _QUEUE_PERMISSIONS_SUBJECT,
      '__module__' : 'armada_client.generated_client.submit_pb2'
      # @@protoc_insertion_point(class_scope:api.Queue.Permissions.Subject)
      })
    ,
    'DESCRIPTOR' : _QUEUE_PERMISSIONS,
    '__module__' : 'armada_client.generated_client.submit_pb2'
    # @@protoc_insertion_point(class_scope:api.Queue.Permissions)
    })
  ,

  'ResourceLimitsEntry' : _reflection.GeneratedProtocolMessageType('ResourceLimitsEntry', (_message.Message,), {
    'DESCRIPTOR' : _QUEUE_RESOURCELIMITSENTRY,
    '__module__' : 'armada_client.generated_client.submit_pb2'
    # @@protoc_insertion_point(class_scope:api.Queue.ResourceLimitsEntry)
    })
  ,
  'DESCRIPTOR' : _QUEUE,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.Queue)
  })
_sym_db.RegisterMessage(Queue)
_sym_db.RegisterMessage(Queue.Permissions)
_sym_db.RegisterMessage(Queue.Permissions.Subject)
_sym_db.RegisterMessage(Queue.ResourceLimitsEntry)

CancellationResult = _reflection.GeneratedProtocolMessageType('CancellationResult', (_message.Message,), {
  'DESCRIPTOR' : _CANCELLATIONRESULT,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.CancellationResult)
  })
_sym_db.RegisterMessage(CancellationResult)

QueueGetRequest = _reflection.GeneratedProtocolMessageType('QueueGetRequest', (_message.Message,), {
  'DESCRIPTOR' : _QUEUEGETREQUEST,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.QueueGetRequest)
  })
_sym_db.RegisterMessage(QueueGetRequest)

QueueInfoRequest = _reflection.GeneratedProtocolMessageType('QueueInfoRequest', (_message.Message,), {
  'DESCRIPTOR' : _QUEUEINFOREQUEST,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.QueueInfoRequest)
  })
_sym_db.RegisterMessage(QueueInfoRequest)

QueueDeleteRequest = _reflection.GeneratedProtocolMessageType('QueueDeleteRequest', (_message.Message,), {
  'DESCRIPTOR' : _QUEUEDELETEREQUEST,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.QueueDeleteRequest)
  })
_sym_db.RegisterMessage(QueueDeleteRequest)

QueueInfo = _reflection.GeneratedProtocolMessageType('QueueInfo', (_message.Message,), {
  'DESCRIPTOR' : _QUEUEINFO,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.QueueInfo)
  })
_sym_db.RegisterMessage(QueueInfo)

JobSetInfo = _reflection.GeneratedProtocolMessageType('JobSetInfo', (_message.Message,), {
  'DESCRIPTOR' : _JOBSETINFO,
  '__module__' : 'armada_client.generated_client.submit_pb2'
  # @@protoc_insertion_point(class_scope:api.JobSetInfo)
  })
_sym_db.RegisterMessage(JobSetInfo)

_SUBMIT = DESCRIPTOR.services_by_name['Submit']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\330\341\036\000\200\342\036\001'
  _JOBSUBMITREQUESTITEM_LABELSENTRY._options = None
  _JOBSUBMITREQUESTITEM_LABELSENTRY._serialized_options = b'8\001'
  _JOBSUBMITREQUESTITEM_ANNOTATIONSENTRY._options = None
  _JOBSUBMITREQUESTITEM_ANNOTATIONSENTRY._serialized_options = b'8\001'
  _JOBSUBMITREQUESTITEM_REQUIREDNODELABELSENTRY._options = None
  _JOBSUBMITREQUESTITEM_REQUIREDNODELABELSENTRY._serialized_options = b'8\001'
  _JOBSUBMITREQUESTITEM.fields_by_name['required_node_labels']._options = None
  _JOBSUBMITREQUESTITEM.fields_by_name['required_node_labels']._serialized_options = b'\030\001'
  _JOBSUBMITREQUESTITEM.fields_by_name['pod_spec']._options = None
  _JOBSUBMITREQUESTITEM.fields_by_name['pod_spec']._serialized_options = b'\030\001'
  _INGRESSCONFIG_ANNOTATIONSENTRY._options = None
  _INGRESSCONFIG_ANNOTATIONSENTRY._serialized_options = b'8\001'
  _INGRESSCONFIG.fields_by_name['type']._options = None
  _INGRESSCONFIG.fields_by_name['type']._serialized_options = b'\030\001'
  _JOBREPRIORITIZERESPONSE_REPRIORITIZATIONRESULTSENTRY._options = None
  _JOBREPRIORITIZERESPONSE_REPRIORITIZATIONRESULTSENTRY._serialized_options = b'8\001'
  _QUEUE_RESOURCELIMITSENTRY._options = None
  _QUEUE_RESOURCELIMITSENTRY._serialized_options = b'8\001'
  _CANCELLATIONRESULT.fields_by_name['cancelled_ids']._options = None
  _CANCELLATIONRESULT.fields_by_name['cancelled_ids']._serialized_options = b'\352\336\037\014cancelledIds'
  _SUBMIT.methods_by_name['SubmitJobs']._options = None
  _SUBMIT.methods_by_name['SubmitJobs']._serialized_options = b'\202\323\344\223\002\023\"\016/v1/job/submit:\001*'
  _SUBMIT.methods_by_name['CancelJobs']._options = None
  _SUBMIT.methods_by_name['CancelJobs']._serialized_options = b'\202\323\344\223\002\023\"\016/v1/job/cancel:\001*'
  _SUBMIT.methods_by_name['ReprioritizeJobs']._options = None
  _SUBMIT.methods_by_name['ReprioritizeJobs']._serialized_options = b'\202\323\344\223\002\031\"\024/v1/job/reprioritize:\001*'
  _SUBMIT.methods_by_name['CreateQueue']._options = None
  _SUBMIT.methods_by_name['CreateQueue']._serialized_options = b'\202\323\344\223\002\016\"\t/v1/queue:\001*'
  _SUBMIT.methods_by_name['UpdateQueue']._options = None
  _SUBMIT.methods_by_name['UpdateQueue']._serialized_options = b'\202\323\344\223\002\025\032\020/v1/queue/{name}:\001*'
  _SUBMIT.methods_by_name['DeleteQueue']._options = None
  _SUBMIT.methods_by_name['DeleteQueue']._serialized_options = b'\202\323\344\223\002\022*\020/v1/queue/{name}'
  _SUBMIT.methods_by_name['GetQueue']._options = None
  _SUBMIT.methods_by_name['GetQueue']._serialized_options = b'\202\323\344\223\002\022\022\020/v1/queue/{name}'
  _SUBMIT.methods_by_name['GetQueueInfo']._options = None
  _SUBMIT.methods_by_name['GetQueueInfo']._serialized_options = b'\202\323\344\223\002\027\022\025/v1/queue/{name}/info'
  _INGRESSTYPE._serialized_start=2389
  _INGRESSTYPE._serialized_end=2415
  _SERVICETYPE._serialized_start=2417
  _SERVICETYPE._serialized_end=2458
  _JOBSUBMITREQUESTITEM._serialized_start=195
  _JOBSUBMITREQUESTITEM._serialized_end=810
  _JOBSUBMITREQUESTITEM_LABELSENTRY._serialized_start=654
  _JOBSUBMITREQUESTITEM_LABELSENTRY._serialized_end=699
  _JOBSUBMITREQUESTITEM_ANNOTATIONSENTRY._serialized_start=701
  _JOBSUBMITREQUESTITEM_ANNOTATIONSENTRY._serialized_end=751
  _JOBSUBMITREQUESTITEM_REQUIREDNODELABELSENTRY._serialized_start=753
  _JOBSUBMITREQUESTITEM_REQUIREDNODELABELSENTRY._serialized_end=810
  _INGRESSCONFIG._serialized_start=813
  _INGRESSCONFIG._serialized_end=1052
  _INGRESSCONFIG_ANNOTATIONSENTRY._serialized_start=701
  _INGRESSCONFIG_ANNOTATIONSENTRY._serialized_end=751
  _SERVICECONFIG._serialized_start=1054
  _SERVICECONFIG._serialized_end=1116
  _JOBSUBMITREQUEST._serialized_start=1118
  _JOBSUBMITREQUEST._serialized_end=1225
  _JOBCANCELREQUEST._serialized_start=1227
  _JOBCANCELREQUEST._serialized_end=1296
  _JOBREPRIORITIZEREQUEST._serialized_start=1298
  _JOBREPRIORITIZEREQUEST._serialized_end=1396
  _JOBREPRIORITIZERESPONSE._serialized_start=1399
  _JOBREPRIORITIZERESPONSE._serialized_end=1581
  _JOBREPRIORITIZERESPONSE_REPRIORITIZATIONRESULTSENTRY._serialized_start=1519
  _JOBREPRIORITIZERESPONSE_REPRIORITIZATIONRESULTSENTRY._serialized_end=1581
  _JOBSUBMITRESPONSEITEM._serialized_start=1583
  _JOBSUBMITRESPONSEITEM._serialized_end=1637
  _JOBSUBMITRESPONSE._serialized_start=1639
  _JOBSUBMITRESPONSE._serialized_end=1714
  _QUEUE._serialized_start=1717
  _QUEUE._serialized_end=2082
  _QUEUE_PERMISSIONS._serialized_start=1910
  _QUEUE_PERMISSIONS._serialized_end=2027
  _QUEUE_PERMISSIONS_SUBJECT._serialized_start=1990
  _QUEUE_PERMISSIONS_SUBJECT._serialized_end=2027
  _QUEUE_RESOURCELIMITSENTRY._serialized_start=2029
  _QUEUE_RESOURCELIMITSENTRY._serialized_end=2082
  _CANCELLATIONRESULT._serialized_start=2084
  _CANCELLATIONRESULT._serialized_end=2145
  _QUEUEGETREQUEST._serialized_start=2147
  _QUEUEGETREQUEST._serialized_end=2178
  _QUEUEINFOREQUEST._serialized_start=2180
  _QUEUEINFOREQUEST._serialized_end=2212
  _QUEUEDELETEREQUEST._serialized_start=2214
  _QUEUEDELETEREQUEST._serialized_end=2248
  _QUEUEINFO._serialized_start=2250
  _QUEUEINFO._serialized_end=2317
  _JOBSETINFO._serialized_start=2319
  _JOBSETINFO._serialized_end=2387
  _SUBMIT._serialized_start=2461
  _SUBMIT._serialized_end=3159
# @@protoc_insertion_point(module_scope)
