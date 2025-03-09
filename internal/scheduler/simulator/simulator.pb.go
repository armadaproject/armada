// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        v5.29.3
// source: internal/scheduler/simulator/simulator.proto

package simulator

import (
	schedulerobjects "github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	v1 "k8s.io/api/core/v1"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ClusterSpec struct {
	state                            protoimpl.MessageState `protogen:"open.v1"`
	Name                             string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Clusters                         []*Cluster             `protobuf:"bytes,2,rep,name=clusters,proto3" json:"clusters,omitempty"`
	WorkflowManagerDelayDistribution *ShiftedExponential    `protobuf:"bytes,3,opt,name=workflow_manager_delay_distribution,json=workflowManagerDelayDistribution,proto3" json:"workflow_manager_delay_distribution,omitempty"`
	PendingDelayDistribution         *ShiftedExponential    `protobuf:"bytes,4,opt,name=pending_delay_distribution,json=pendingDelayDistribution,proto3" json:"pending_delay_distribution,omitempty"`
	unknownFields                    protoimpl.UnknownFields
	sizeCache                        protoimpl.SizeCache
}

func (x *ClusterSpec) Reset() {
	*x = ClusterSpec{}
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ClusterSpec) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ClusterSpec) ProtoMessage() {}

func (x *ClusterSpec) ProtoReflect() protoreflect.Message {
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ClusterSpec.ProtoReflect.Descriptor instead.
func (*ClusterSpec) Descriptor() ([]byte, []int) {
	return file_internal_scheduler_simulator_simulator_proto_rawDescGZIP(), []int{0}
}

func (x *ClusterSpec) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *ClusterSpec) GetClusters() []*Cluster {
	if x != nil {
		return x.Clusters
	}
	return nil
}

func (x *ClusterSpec) GetWorkflowManagerDelayDistribution() *ShiftedExponential {
	if x != nil {
		return x.WorkflowManagerDelayDistribution
	}
	return nil
}

func (x *ClusterSpec) GetPendingDelayDistribution() *ShiftedExponential {
	if x != nil {
		return x.PendingDelayDistribution
	}
	return nil
}

type Cluster struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Name          string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Pool          string                 `protobuf:"bytes,3,opt,name=pool,proto3" json:"pool,omitempty"`
	NodeTemplates []*NodeTemplate        `protobuf:"bytes,2,rep,name=node_templates,json=nodeTemplates,proto3" json:"node_templates,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Cluster) Reset() {
	*x = Cluster{}
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Cluster) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Cluster) ProtoMessage() {}

func (x *Cluster) ProtoReflect() protoreflect.Message {
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Cluster.ProtoReflect.Descriptor instead.
func (*Cluster) Descriptor() ([]byte, []int) {
	return file_internal_scheduler_simulator_simulator_proto_rawDescGZIP(), []int{1}
}

func (x *Cluster) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Cluster) GetPool() string {
	if x != nil {
		return x.Pool
	}
	return ""
}

func (x *Cluster) GetNodeTemplates() []*NodeTemplate {
	if x != nil {
		return x.NodeTemplates
	}
	return nil
}

type WorkloadSpec struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	Name  string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Random seed used in simulations; use to ensure simulations are reproducible.
	// If not provided, or explicitly set to 0, the current time is used.
	RandomSeed    int64    `protobuf:"varint,2,opt,name=random_seed,json=randomSeed,proto3" json:"random_seed,omitempty"`
	Queues        []*Queue `protobuf:"bytes,3,rep,name=queues,proto3" json:"queues,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *WorkloadSpec) Reset() {
	*x = WorkloadSpec{}
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WorkloadSpec) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WorkloadSpec) ProtoMessage() {}

func (x *WorkloadSpec) ProtoReflect() protoreflect.Message {
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WorkloadSpec.ProtoReflect.Descriptor instead.
func (*WorkloadSpec) Descriptor() ([]byte, []int) {
	return file_internal_scheduler_simulator_simulator_proto_rawDescGZIP(), []int{2}
}

func (x *WorkloadSpec) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *WorkloadSpec) GetRandomSeed() int64 {
	if x != nil {
		return x.RandomSeed
	}
	return 0
}

func (x *WorkloadSpec) GetQueues() []*Queue {
	if x != nil {
		return x.Queues
	}
	return nil
}

type NodeTemplate struct {
	state          protoimpl.MessageState         `protogen:"open.v1"`
	Number         int64                          `protobuf:"varint,1,opt,name=number,proto3" json:"number,omitempty"`
	Taints         []*v1.Taint                    `protobuf:"bytes,2,rep,name=taints,proto3" json:"taints,omitempty"`
	Labels         map[string]string              `protobuf:"bytes,3,rep,name=labels,proto3" json:"labels,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	TotalResources *schedulerobjects.ResourceList `protobuf:"bytes,4,opt,name=total_resources,json=totalResources,proto3" json:"total_resources,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *NodeTemplate) Reset() {
	*x = NodeTemplate{}
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *NodeTemplate) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NodeTemplate) ProtoMessage() {}

func (x *NodeTemplate) ProtoReflect() protoreflect.Message {
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NodeTemplate.ProtoReflect.Descriptor instead.
func (*NodeTemplate) Descriptor() ([]byte, []int) {
	return file_internal_scheduler_simulator_simulator_proto_rawDescGZIP(), []int{3}
}

func (x *NodeTemplate) GetNumber() int64 {
	if x != nil {
		return x.Number
	}
	return 0
}

func (x *NodeTemplate) GetTaints() []*v1.Taint {
	if x != nil {
		return x.Taints
	}
	return nil
}

func (x *NodeTemplate) GetLabels() map[string]string {
	if x != nil {
		return x.Labels
	}
	return nil
}

func (x *NodeTemplate) GetTotalResources() *schedulerobjects.ResourceList {
	if x != nil {
		return x.TotalResources
	}
	return nil
}

type Queue struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Name          string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Weight        float64                `protobuf:"fixed64,2,opt,name=weight,proto3" json:"weight,omitempty"`
	JobTemplates  []*JobTemplate         `protobuf:"bytes,3,rep,name=job_templates,json=jobTemplates,proto3" json:"job_templates,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Queue) Reset() {
	*x = Queue{}
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Queue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Queue) ProtoMessage() {}

func (x *Queue) ProtoReflect() protoreflect.Message {
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Queue.ProtoReflect.Descriptor instead.
func (*Queue) Descriptor() ([]byte, []int) {
	return file_internal_scheduler_simulator_simulator_proto_rawDescGZIP(), []int{4}
}

func (x *Queue) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Queue) GetWeight() float64 {
	if x != nil {
		return x.Weight
	}
	return 0
}

func (x *Queue) GetJobTemplates() []*JobTemplate {
	if x != nil {
		return x.JobTemplates
	}
	return nil
}

type JobTemplate struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Number of jobs to create from this template.
	Number int64 `protobuf:"varint,1,opt,name=number,proto3" json:"number,omitempty"`
	// Number of jobs created from this template that have succeeded.
	// Maintained by the simulator.
	NumberSuccessful int64 `protobuf:"varint,2,opt,name=numberSuccessful,proto3" json:"numberSuccessful,omitempty"`
	// Queue to which this template belongs. Populated automatically.
	Queue string `protobuf:"bytes,3,opt,name=queue,proto3" json:"queue,omitempty"`
	// Unique id for this template. An id is generated if empty.
	Id                string `protobuf:"bytes,4,opt,name=id,proto3" json:"id,omitempty"`
	JobSet            string `protobuf:"bytes,5,opt,name=job_set,json=jobSet,proto3" json:"job_set,omitempty"`
	QueuePriority     uint32 `protobuf:"varint,6,opt,name=queue_priority,json=queuePriority,proto3" json:"queue_priority,omitempty"`
	PriorityClassName string `protobuf:"bytes,7,opt,name=priority_class_name,json=priorityClassName,proto3" json:"priority_class_name,omitempty"`
	// Scheduling requirements for the pod embedded in the job.
	Requirements *schedulerobjects.PodRequirements `protobuf:"bytes,8,opt,name=requirements,proto3" json:"requirements,omitempty"`
	// List of template ids that must be completed before this template is submitted.
	Dependencies []string `protobuf:"bytes,9,rep,name=dependencies,proto3" json:"dependencies,omitempty"`
	// Earliest time at which jobs from this template are submitted.
	// Measured from the start of the simulation.
	EarliestSubmitTime *durationpb.Duration `protobuf:"bytes,10,opt,name=earliest_submit_time,json=earliestSubmitTime,proto3" json:"earliest_submit_time,omitempty"`
	// Earliest time job can be submitted from when all its dependencies have completed.
	// This option is meant to model thinking or processing time, where some fixed amount of time
	// needs to be spent between dependencies completing and the next batch of jobs being ready to submit.
	EarliestSubmitTimeFromDependencyCompletion *durationpb.Duration `protobuf:"bytes,11,opt,name=earliest_submit_time_from_dependency_completion,json=earliestSubmitTimeFromDependencyCompletion,proto3" json:"earliest_submit_time_from_dependency_completion,omitempty"`
	// Job runtimes are assumed to follow a shifted exponential distribution
	// i.e., to be a fixed constant (runtime_minimum) plus a random amount of time
	// drawn from an exponential distribution with known mean (runtime_tail_mean).
	//
	// The shifted-exponential distribution strikes a good balance between simplicity and accuracy;
	// see https://bora.uib.no/bora-xmlui/bitstream/handle/11250/3014726/drthesis_2022_severinson.pdf?sequence=2
	// for a discussion on the topic.
	RuntimeDistribution *ShiftedExponential `protobuf:"bytes,12,opt,name=runtime_distribution,json=runtimeDistribution,proto3" json:"runtime_distribution,omitempty"`
	// If set, jobs will be assigned to gangs with the given size. In this case `number` must be exactly divisible by the gang size
	GangCardinality uint32 `protobuf:"varint,13,opt,name=gang_cardinality,json=gangCardinality,proto3" json:"gang_cardinality,omitempty"`
	// Node Uniformity label when scheduling gangs.  Only applies if gang_cardinality is non-zero.  If unset it defaults to armadaproject.io/clusterName
	GangNodeUniformityLabel string `protobuf:"bytes,14,opt,name=gang_node_uniformity_label,json=gangNodeUniformityLabel,proto3" json:"gang_node_uniformity_label,omitempty"`
	// If set then the template will be repeated at some frequency. If null then the template will be submitted a single time.
	Repeat        *RepeatDetails `protobuf:"bytes,15,opt,name=repeat,proto3" json:"repeat,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *JobTemplate) Reset() {
	*x = JobTemplate{}
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *JobTemplate) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JobTemplate) ProtoMessage() {}

func (x *JobTemplate) ProtoReflect() protoreflect.Message {
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JobTemplate.ProtoReflect.Descriptor instead.
func (*JobTemplate) Descriptor() ([]byte, []int) {
	return file_internal_scheduler_simulator_simulator_proto_rawDescGZIP(), []int{5}
}

func (x *JobTemplate) GetNumber() int64 {
	if x != nil {
		return x.Number
	}
	return 0
}

func (x *JobTemplate) GetNumberSuccessful() int64 {
	if x != nil {
		return x.NumberSuccessful
	}
	return 0
}

func (x *JobTemplate) GetQueue() string {
	if x != nil {
		return x.Queue
	}
	return ""
}

func (x *JobTemplate) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *JobTemplate) GetJobSet() string {
	if x != nil {
		return x.JobSet
	}
	return ""
}

func (x *JobTemplate) GetQueuePriority() uint32 {
	if x != nil {
		return x.QueuePriority
	}
	return 0
}

func (x *JobTemplate) GetPriorityClassName() string {
	if x != nil {
		return x.PriorityClassName
	}
	return ""
}

func (x *JobTemplate) GetRequirements() *schedulerobjects.PodRequirements {
	if x != nil {
		return x.Requirements
	}
	return nil
}

func (x *JobTemplate) GetDependencies() []string {
	if x != nil {
		return x.Dependencies
	}
	return nil
}

func (x *JobTemplate) GetEarliestSubmitTime() *durationpb.Duration {
	if x != nil {
		return x.EarliestSubmitTime
	}
	return nil
}

func (x *JobTemplate) GetEarliestSubmitTimeFromDependencyCompletion() *durationpb.Duration {
	if x != nil {
		return x.EarliestSubmitTimeFromDependencyCompletion
	}
	return nil
}

func (x *JobTemplate) GetRuntimeDistribution() *ShiftedExponential {
	if x != nil {
		return x.RuntimeDistribution
	}
	return nil
}

func (x *JobTemplate) GetGangCardinality() uint32 {
	if x != nil {
		return x.GangCardinality
	}
	return 0
}

func (x *JobTemplate) GetGangNodeUniformityLabel() string {
	if x != nil {
		return x.GangNodeUniformityLabel
	}
	return ""
}

func (x *JobTemplate) GetRepeat() *RepeatDetails {
	if x != nil {
		return x.Repeat
	}
	return nil
}

type RepeatDetails struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The number of times that template should be repeated. Must be > 0
	NumTimes uint32 `protobuf:"varint,1,opt,name=num_times,json=numTimes,proto3" json:"num_times,omitempty"`
	// The period between template submissions.  May not be null
	Period        *durationpb.Duration `protobuf:"bytes,2,opt,name=period,proto3" json:"period,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RepeatDetails) Reset() {
	*x = RepeatDetails{}
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RepeatDetails) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RepeatDetails) ProtoMessage() {}

func (x *RepeatDetails) ProtoReflect() protoreflect.Message {
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RepeatDetails.ProtoReflect.Descriptor instead.
func (*RepeatDetails) Descriptor() ([]byte, []int) {
	return file_internal_scheduler_simulator_simulator_proto_rawDescGZIP(), []int{6}
}

func (x *RepeatDetails) GetNumTimes() uint32 {
	if x != nil {
		return x.NumTimes
	}
	return 0
}

func (x *RepeatDetails) GetPeriod() *durationpb.Duration {
	if x != nil {
		return x.Period
	}
	return nil
}

type ShiftedExponential struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Minimum       *durationpb.Duration   `protobuf:"bytes,1,opt,name=minimum,proto3" json:"minimum,omitempty"`
	TailMean      *durationpb.Duration   `protobuf:"bytes,2,opt,name=tail_mean,json=tailMean,proto3" json:"tail_mean,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ShiftedExponential) Reset() {
	*x = ShiftedExponential{}
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ShiftedExponential) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ShiftedExponential) ProtoMessage() {}

func (x *ShiftedExponential) ProtoReflect() protoreflect.Message {
	mi := &file_internal_scheduler_simulator_simulator_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ShiftedExponential.ProtoReflect.Descriptor instead.
func (*ShiftedExponential) Descriptor() ([]byte, []int) {
	return file_internal_scheduler_simulator_simulator_proto_rawDescGZIP(), []int{7}
}

func (x *ShiftedExponential) GetMinimum() *durationpb.Duration {
	if x != nil {
		return x.Minimum
	}
	return nil
}

func (x *ShiftedExponential) GetTailMean() *durationpb.Duration {
	if x != nil {
		return x.TailMean
	}
	return nil
}

var File_internal_scheduler_simulator_simulator_proto protoreflect.FileDescriptor

var file_internal_scheduler_simulator_simulator_proto_rawDesc = string([]byte{
	0x0a, 0x2c, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x73, 0x63, 0x68, 0x65, 0x64,
	0x75, 0x6c, 0x65, 0x72, 0x2f, 0x73, 0x69, 0x6d, 0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72, 0x2f, 0x73,
	0x69, 0x6d, 0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x09,
	0x73, 0x69, 0x6d, 0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72, 0x1a, 0x1e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x64, 0x75, 0x72, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x22, 0x6b, 0x38, 0x73, 0x2e, 0x69,
	0x6f, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x72, 0x65, 0x2f, 0x76, 0x31, 0x2f, 0x67, 0x65,
	0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x3a, 0x69,
	0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65,
	0x72, 0x2f, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x6f, 0x62, 0x6a, 0x65, 0x63,
	0x74, 0x73, 0x2f, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x6f, 0x62, 0x6a, 0x65,
	0x63, 0x74, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x9c, 0x02, 0x0a, 0x0b, 0x43, 0x6c,
	0x75, 0x73, 0x74, 0x65, 0x72, 0x53, 0x70, 0x65, 0x63, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x2e, 0x0a,
	0x08, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x12, 0x2e, 0x73, 0x69, 0x6d, 0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x43, 0x6c, 0x75, 0x73,
	0x74, 0x65, 0x72, 0x52, 0x08, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x73, 0x12, 0x6c, 0x0a,
	0x23, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x5f, 0x6d, 0x61, 0x6e, 0x61, 0x67, 0x65,
	0x72, 0x5f, 0x64, 0x65, 0x6c, 0x61, 0x79, 0x5f, 0x64, 0x69, 0x73, 0x74, 0x72, 0x69, 0x62, 0x75,
	0x74, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x73, 0x69, 0x6d,
	0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x53, 0x68, 0x69, 0x66, 0x74, 0x65, 0x64, 0x45, 0x78,
	0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c, 0x52, 0x20, 0x77, 0x6f, 0x72, 0x6b, 0x66,
	0x6c, 0x6f, 0x77, 0x4d, 0x61, 0x6e, 0x61, 0x67, 0x65, 0x72, 0x44, 0x65, 0x6c, 0x61, 0x79, 0x44,
	0x69, 0x73, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x5b, 0x0a, 0x1a, 0x70,
	0x65, 0x6e, 0x64, 0x69, 0x6e, 0x67, 0x5f, 0x64, 0x65, 0x6c, 0x61, 0x79, 0x5f, 0x64, 0x69, 0x73,
	0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1d, 0x2e, 0x73, 0x69, 0x6d, 0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x53, 0x68, 0x69, 0x66,
	0x74, 0x65, 0x64, 0x45, 0x78, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c, 0x52, 0x18,
	0x70, 0x65, 0x6e, 0x64, 0x69, 0x6e, 0x67, 0x44, 0x65, 0x6c, 0x61, 0x79, 0x44, 0x69, 0x73, 0x74,
	0x72, 0x69, 0x62, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x71, 0x0a, 0x07, 0x43, 0x6c, 0x75, 0x73,
	0x74, 0x65, 0x72, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x6f, 0x6f, 0x6c, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x70, 0x6f, 0x6f, 0x6c, 0x12, 0x3e, 0x0a, 0x0e, 0x6e,
	0x6f, 0x64, 0x65, 0x5f, 0x74, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x73, 0x18, 0x02, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x73, 0x69, 0x6d, 0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72, 0x2e,
	0x4e, 0x6f, 0x64, 0x65, 0x54, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x52, 0x0d, 0x6e, 0x6f,
	0x64, 0x65, 0x54, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x73, 0x22, 0x6d, 0x0a, 0x0c, 0x57,
	0x6f, 0x72, 0x6b, 0x6c, 0x6f, 0x61, 0x64, 0x53, 0x70, 0x65, 0x63, 0x12, 0x12, 0x0a, 0x04, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12,
	0x1f, 0x0a, 0x0b, 0x72, 0x61, 0x6e, 0x64, 0x6f, 0x6d, 0x5f, 0x73, 0x65, 0x65, 0x64, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x0a, 0x72, 0x61, 0x6e, 0x64, 0x6f, 0x6d, 0x53, 0x65, 0x65, 0x64,
	0x12, 0x28, 0x0a, 0x06, 0x71, 0x75, 0x65, 0x75, 0x65, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x10, 0x2e, 0x73, 0x69, 0x6d, 0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x52, 0x06, 0x71, 0x75, 0x65, 0x75, 0x65, 0x73, 0x22, 0x9a, 0x02, 0x0a, 0x0c, 0x4e,
	0x6f, 0x64, 0x65, 0x54, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x6e,
	0x75, 0x6d, 0x62, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x6e, 0x75, 0x6d,
	0x62, 0x65, 0x72, 0x12, 0x31, 0x0a, 0x06, 0x74, 0x61, 0x69, 0x6e, 0x74, 0x73, 0x18, 0x02, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x6b, 0x38, 0x73, 0x2e, 0x69, 0x6f, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x69, 0x6e, 0x74, 0x52, 0x06,
	0x74, 0x61, 0x69, 0x6e, 0x74, 0x73, 0x12, 0x3b, 0x0a, 0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73,
	0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x23, 0x2e, 0x73, 0x69, 0x6d, 0x75, 0x6c, 0x61, 0x74,
	0x6f, 0x72, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x54, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x2e,
	0x4c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x06, 0x6c, 0x61, 0x62,
	0x65, 0x6c, 0x73, 0x12, 0x47, 0x0a, 0x0f, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x5f, 0x72, 0x65, 0x73,
	0x6f, 0x75, 0x72, 0x63, 0x65, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1e, 0x2e, 0x73,
	0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x2e,
	0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x0e, 0x74, 0x6f,
	0x74, 0x61, 0x6c, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x73, 0x1a, 0x39, 0x0a, 0x0b,
	0x4c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b,
	0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x70, 0x0a, 0x05, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x77, 0x65, 0x69, 0x67, 0x68, 0x74, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x01, 0x52, 0x06, 0x77, 0x65, 0x69, 0x67, 0x68, 0x74, 0x12, 0x3b, 0x0a, 0x0d,
	0x6a, 0x6f, 0x62, 0x5f, 0x74, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x73, 0x18, 0x03, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x73, 0x69, 0x6d, 0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72, 0x2e,
	0x4a, 0x6f, 0x62, 0x54, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x52, 0x0c, 0x6a, 0x6f, 0x62,
	0x54, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x73, 0x22, 0x8b, 0x06, 0x0a, 0x0b, 0x4a, 0x6f,
	0x62, 0x54, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x6e, 0x75, 0x6d,
	0x62, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x6e, 0x75, 0x6d, 0x62, 0x65,
	0x72, 0x12, 0x2a, 0x0a, 0x10, 0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x53, 0x75, 0x63, 0x63, 0x65,
	0x73, 0x73, 0x66, 0x75, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x10, 0x6e, 0x75, 0x6d,
	0x62, 0x65, 0x72, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x66, 0x75, 0x6c, 0x12, 0x14, 0x0a,
	0x05, 0x71, 0x75, 0x65, 0x75, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x71, 0x75,
	0x65, 0x75, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x02, 0x69, 0x64, 0x12, 0x17, 0x0a, 0x07, 0x6a, 0x6f, 0x62, 0x5f, 0x73, 0x65, 0x74, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x6a, 0x6f, 0x62, 0x53, 0x65, 0x74, 0x12, 0x25, 0x0a, 0x0e,
	0x71, 0x75, 0x65, 0x75, 0x65, 0x5f, 0x70, 0x72, 0x69, 0x6f, 0x72, 0x69, 0x74, 0x79, 0x18, 0x06,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x0d, 0x71, 0x75, 0x65, 0x75, 0x65, 0x50, 0x72, 0x69, 0x6f, 0x72,
	0x69, 0x74, 0x79, 0x12, 0x2e, 0x0a, 0x13, 0x70, 0x72, 0x69, 0x6f, 0x72, 0x69, 0x74, 0x79, 0x5f,
	0x63, 0x6c, 0x61, 0x73, 0x73, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x11, 0x70, 0x72, 0x69, 0x6f, 0x72, 0x69, 0x74, 0x79, 0x43, 0x6c, 0x61, 0x73, 0x73, 0x4e,
	0x61, 0x6d, 0x65, 0x12, 0x45, 0x0a, 0x0c, 0x72, 0x65, 0x71, 0x75, 0x69, 0x72, 0x65, 0x6d, 0x65,
	0x6e, 0x74, 0x73, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x21, 0x2e, 0x73, 0x63, 0x68, 0x65,
	0x64, 0x75, 0x6c, 0x65, 0x72, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x2e, 0x50, 0x6f, 0x64,
	0x52, 0x65, 0x71, 0x75, 0x69, 0x72, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x52, 0x0c, 0x72, 0x65,
	0x71, 0x75, 0x69, 0x72, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x12, 0x22, 0x0a, 0x0c, 0x64, 0x65,
	0x70, 0x65, 0x6e, 0x64, 0x65, 0x6e, 0x63, 0x69, 0x65, 0x73, 0x18, 0x09, 0x20, 0x03, 0x28, 0x09,
	0x52, 0x0c, 0x64, 0x65, 0x70, 0x65, 0x6e, 0x64, 0x65, 0x6e, 0x63, 0x69, 0x65, 0x73, 0x12, 0x4b,
	0x0a, 0x14, 0x65, 0x61, 0x72, 0x6c, 0x69, 0x65, 0x73, 0x74, 0x5f, 0x73, 0x75, 0x62, 0x6d, 0x69,
	0x74, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44,
	0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x12, 0x65, 0x61, 0x72, 0x6c, 0x69, 0x65, 0x73,
	0x74, 0x53, 0x75, 0x62, 0x6d, 0x69, 0x74, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x7e, 0x0a, 0x2f, 0x65,
	0x61, 0x72, 0x6c, 0x69, 0x65, 0x73, 0x74, 0x5f, 0x73, 0x75, 0x62, 0x6d, 0x69, 0x74, 0x5f, 0x74,
	0x69, 0x6d, 0x65, 0x5f, 0x66, 0x72, 0x6f, 0x6d, 0x5f, 0x64, 0x65, 0x70, 0x65, 0x6e, 0x64, 0x65,
	0x6e, 0x63, 0x79, 0x5f, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x0b,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52,
	0x2a, 0x65, 0x61, 0x72, 0x6c, 0x69, 0x65, 0x73, 0x74, 0x53, 0x75, 0x62, 0x6d, 0x69, 0x74, 0x54,
	0x69, 0x6d, 0x65, 0x46, 0x72, 0x6f, 0x6d, 0x44, 0x65, 0x70, 0x65, 0x6e, 0x64, 0x65, 0x6e, 0x63,
	0x79, 0x43, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x50, 0x0a, 0x14, 0x72,
	0x75, 0x6e, 0x74, 0x69, 0x6d, 0x65, 0x5f, 0x64, 0x69, 0x73, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74,
	0x69, 0x6f, 0x6e, 0x18, 0x0c, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x73, 0x69, 0x6d, 0x75,
	0x6c, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x53, 0x68, 0x69, 0x66, 0x74, 0x65, 0x64, 0x45, 0x78, 0x70,
	0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c, 0x52, 0x13, 0x72, 0x75, 0x6e, 0x74, 0x69, 0x6d,
	0x65, 0x44, 0x69, 0x73, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x29, 0x0a,
	0x10, 0x67, 0x61, 0x6e, 0x67, 0x5f, 0x63, 0x61, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x6c, 0x69, 0x74,
	0x79, 0x18, 0x0d, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0f, 0x67, 0x61, 0x6e, 0x67, 0x43, 0x61, 0x72,
	0x64, 0x69, 0x6e, 0x61, 0x6c, 0x69, 0x74, 0x79, 0x12, 0x3b, 0x0a, 0x1a, 0x67, 0x61, 0x6e, 0x67,
	0x5f, 0x6e, 0x6f, 0x64, 0x65, 0x5f, 0x75, 0x6e, 0x69, 0x66, 0x6f, 0x72, 0x6d, 0x69, 0x74, 0x79,
	0x5f, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x18, 0x0e, 0x20, 0x01, 0x28, 0x09, 0x52, 0x17, 0x67, 0x61,
	0x6e, 0x67, 0x4e, 0x6f, 0x64, 0x65, 0x55, 0x6e, 0x69, 0x66, 0x6f, 0x72, 0x6d, 0x69, 0x74, 0x79,
	0x4c, 0x61, 0x62, 0x65, 0x6c, 0x12, 0x30, 0x0a, 0x06, 0x72, 0x65, 0x70, 0x65, 0x61, 0x74, 0x18,
	0x0f, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x73, 0x69, 0x6d, 0x75, 0x6c, 0x61, 0x74, 0x6f,
	0x72, 0x2e, 0x52, 0x65, 0x70, 0x65, 0x61, 0x74, 0x44, 0x65, 0x74, 0x61, 0x69, 0x6c, 0x73, 0x52,
	0x06, 0x72, 0x65, 0x70, 0x65, 0x61, 0x74, 0x22, 0x5f, 0x0a, 0x0d, 0x52, 0x65, 0x70, 0x65, 0x61,
	0x74, 0x44, 0x65, 0x74, 0x61, 0x69, 0x6c, 0x73, 0x12, 0x1b, 0x0a, 0x09, 0x6e, 0x75, 0x6d, 0x5f,
	0x74, 0x69, 0x6d, 0x65, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x6e, 0x75, 0x6d,
	0x54, 0x69, 0x6d, 0x65, 0x73, 0x12, 0x31, 0x0a, 0x06, 0x70, 0x65, 0x72, 0x69, 0x6f, 0x64, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x52, 0x06, 0x70, 0x65, 0x72, 0x69, 0x6f, 0x64, 0x22, 0x81, 0x01, 0x0a, 0x12, 0x53, 0x68, 0x69,
	0x66, 0x74, 0x65, 0x64, 0x45, 0x78, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c, 0x12,
	0x33, 0x0a, 0x07, 0x6d, 0x69, 0x6e, 0x69, 0x6d, 0x75, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x07, 0x6d, 0x69, 0x6e,
	0x69, 0x6d, 0x75, 0x6d, 0x12, 0x36, 0x0a, 0x09, 0x74, 0x61, 0x69, 0x6c, 0x5f, 0x6d, 0x65, 0x61,
	0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x52, 0x08, 0x74, 0x61, 0x69, 0x6c, 0x4d, 0x65, 0x61, 0x6e, 0x42, 0x1e, 0x5a, 0x1c,
	0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c,
	0x65, 0x72, 0x2f, 0x73, 0x69, 0x6d, 0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_internal_scheduler_simulator_simulator_proto_rawDescOnce sync.Once
	file_internal_scheduler_simulator_simulator_proto_rawDescData []byte
)

func file_internal_scheduler_simulator_simulator_proto_rawDescGZIP() []byte {
	file_internal_scheduler_simulator_simulator_proto_rawDescOnce.Do(func() {
		file_internal_scheduler_simulator_simulator_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_internal_scheduler_simulator_simulator_proto_rawDesc), len(file_internal_scheduler_simulator_simulator_proto_rawDesc)))
	})
	return file_internal_scheduler_simulator_simulator_proto_rawDescData
}

var file_internal_scheduler_simulator_simulator_proto_msgTypes = make([]protoimpl.MessageInfo, 9)
var file_internal_scheduler_simulator_simulator_proto_goTypes = []any{
	(*ClusterSpec)(nil),                      // 0: simulator.ClusterSpec
	(*Cluster)(nil),                          // 1: simulator.Cluster
	(*WorkloadSpec)(nil),                     // 2: simulator.WorkloadSpec
	(*NodeTemplate)(nil),                     // 3: simulator.NodeTemplate
	(*Queue)(nil),                            // 4: simulator.Queue
	(*JobTemplate)(nil),                      // 5: simulator.JobTemplate
	(*RepeatDetails)(nil),                    // 6: simulator.RepeatDetails
	(*ShiftedExponential)(nil),               // 7: simulator.ShiftedExponential
	nil,                                      // 8: simulator.NodeTemplate.LabelsEntry
	(*v1.Taint)(nil),                         // 9: k8s.io.api.core.v1.Taint
	(*schedulerobjects.ResourceList)(nil),    // 10: schedulerobjects.ResourceList
	(*schedulerobjects.PodRequirements)(nil), // 11: schedulerobjects.PodRequirements
	(*durationpb.Duration)(nil),              // 12: google.protobuf.Duration
}
var file_internal_scheduler_simulator_simulator_proto_depIdxs = []int32{
	1,  // 0: simulator.ClusterSpec.clusters:type_name -> simulator.Cluster
	7,  // 1: simulator.ClusterSpec.workflow_manager_delay_distribution:type_name -> simulator.ShiftedExponential
	7,  // 2: simulator.ClusterSpec.pending_delay_distribution:type_name -> simulator.ShiftedExponential
	3,  // 3: simulator.Cluster.node_templates:type_name -> simulator.NodeTemplate
	4,  // 4: simulator.WorkloadSpec.queues:type_name -> simulator.Queue
	9,  // 5: simulator.NodeTemplate.taints:type_name -> k8s.io.api.core.v1.Taint
	8,  // 6: simulator.NodeTemplate.labels:type_name -> simulator.NodeTemplate.LabelsEntry
	10, // 7: simulator.NodeTemplate.total_resources:type_name -> schedulerobjects.ResourceList
	5,  // 8: simulator.Queue.job_templates:type_name -> simulator.JobTemplate
	11, // 9: simulator.JobTemplate.requirements:type_name -> schedulerobjects.PodRequirements
	12, // 10: simulator.JobTemplate.earliest_submit_time:type_name -> google.protobuf.Duration
	12, // 11: simulator.JobTemplate.earliest_submit_time_from_dependency_completion:type_name -> google.protobuf.Duration
	7,  // 12: simulator.JobTemplate.runtime_distribution:type_name -> simulator.ShiftedExponential
	6,  // 13: simulator.JobTemplate.repeat:type_name -> simulator.RepeatDetails
	12, // 14: simulator.RepeatDetails.period:type_name -> google.protobuf.Duration
	12, // 15: simulator.ShiftedExponential.minimum:type_name -> google.protobuf.Duration
	12, // 16: simulator.ShiftedExponential.tail_mean:type_name -> google.protobuf.Duration
	17, // [17:17] is the sub-list for method output_type
	17, // [17:17] is the sub-list for method input_type
	17, // [17:17] is the sub-list for extension type_name
	17, // [17:17] is the sub-list for extension extendee
	0,  // [0:17] is the sub-list for field type_name
}

func init() { file_internal_scheduler_simulator_simulator_proto_init() }
func file_internal_scheduler_simulator_simulator_proto_init() {
	if File_internal_scheduler_simulator_simulator_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_internal_scheduler_simulator_simulator_proto_rawDesc), len(file_internal_scheduler_simulator_simulator_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   9,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_internal_scheduler_simulator_simulator_proto_goTypes,
		DependencyIndexes: file_internal_scheduler_simulator_simulator_proto_depIdxs,
		MessageInfos:      file_internal_scheduler_simulator_simulator_proto_msgTypes,
	}.Build()
	File_internal_scheduler_simulator_simulator_proto = out.File
	file_internal_scheduler_simulator_simulator_proto_goTypes = nil
	file_internal_scheduler_simulator_simulator_proto_depIdxs = nil
}
