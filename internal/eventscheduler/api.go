package eventscheduler

import (
	"context"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

type ExecutorApi struct {
	db *pgxpool.Pool
}

func (srv *ExecutorApi) LeaseJobs(ctx context.Context, req *api.LeaseRequest) (*api.JobLease, error) {
	// Write usage reports into postgres.
	// Get all jobs assigned to this executor it hasn't already received.
	return nil, nil
}

func (srv *ExecutorApi) StreamingLeaseJobs(stream api.AggregatedQueue_StreamingLeaseJobsServer) error {
	return nil
}

// writeNodeInfoToPostgres writes the NodeInfo messages received from an executor into postgres
// with the name of the node set as the primary key, i.e., the node name must be unique across all clusters.
func (srv *ExecutorApi) writeNodeInfoToPostgres(ctx context.Context, nodeInfos []api.NodeInfo) error {
	records := make([]interface{}, 0)
	for _, nodeInfo := range nodeInfos {
		message, err := proto.Marshal(&nodeInfo)
		if err != nil {
			return errors.WithStack(err)
		}
		records = append(records, Nodeinfo{
			NodeName: nodeInfo.GetName(),
			Message:  message,
		})
	}
	return Upsert(ctx, srv.db, "nodeinfo", NodeInfoSchema(), records)
}

func (srv *ExecutorApi) RenewLease(ctx context.Context, req *api.RenewLeaseRequest) (*api.IdList, error) {
	// Update the time in postgres when this executor last touched this lease.
	log := ctxlogrus.Extract(ctx)
	log.Infof("executor %s renewed jobs %v", req.ClusterId, req.Ids)
	return &api.IdList{
		Ids: req.Ids,
	}, nil
}

func (srv *ExecutorApi) ReturnLease(ctx context.Context, req *api.ReturnLeaseRequest) (*types.Empty, error) {
	log := ctxlogrus.Extract(ctx)
	log.Infof("executor %s returned %s", req.ClusterId, req.JobId)

	// jobSetName, ok := srv.jobSetByJobId[req.JobId]
	// if !ok {
	// 	return &types.Empty{}, errors.Errorf("unknown job id %s: could not find job set name: %v", req.JobId, srv.jobSetByJobId)
	// }
	// queue, ok := srv.queueByJobId[req.JobId]
	// if !ok {
	// 	return &types.Empty{}, errors.Errorf("unknown job id %s: could not find queue", req.JobId)
	// }
	// runId, ok := srv.runIdByJobId[req.JobId]
	// if !ok {
	// 	return &types.Empty{}, errors.Errorf("unknown job id %s: could not find run id", req.JobId)
	// }
	// sequence := &armadaevents.EventSequence{
	// 	Queue:      queue,
	// 	JobSetName: jobSetName,
	// }

	// jobId, err := armadaevents.ProtoUuidFromUuidString(req.JobId)
	// if err != nil {
	// 	return &types.Empty{}, err
	// }

	// sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
	// 	Event: &armadaevents.EventSequence_Event_JobRunErrors{
	// 		JobRunErrors: &armadaevents.JobRunErrors{
	// 			RunId: runId,
	// 			JobId: jobId,
	// 			Errors: []*armadaevents.Error{
	// 				{
	// 					Terminal: true, // EventMessage_LeaseReturned indicates a pod could not be scheduled.
	// 					Reason: &armadaevents.Error_PodLeaseReturned{
	// 						PodLeaseReturned: &armadaevents.PodLeaseReturned{
	// 							ObjectMeta: &armadaevents.ObjectMeta{
	// 								ExecutorId:   req.ClusterId,
	// 								KubernetesId: "", // TODO: The fields explicitly set empty here should be set, but are not available in req.
	// 							},
	// 							PodNumber: 0,
	// 							Message:   "",
	// 						},
	// 					},
	// 				},
	// 			},
	// 		},
	// 	},
	// })

	// err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{sequence})
	// if err != nil {
	// 	return nil, err
	// }

	return &types.Empty{}, nil
}

func (srv *ExecutorApi) ReportDone(ctx context.Context, req *api.IdList) (*api.IdList, error) {
	log := ctxlogrus.Extract(ctx)
	log.Infof("jobs %v reported done", req.Ids)

	// sequences := make([]*armadaevents.EventSequence, 0)
	// for _, jobId := range req.GetIds() {
	// 	jobSetName, ok := srv.jobSetByJobId[jobId]
	// 	if !ok {
	// 		return nil, errors.Errorf("unknown job id %s: could not find job set name: %v", jobId, srv.jobSetByJobId)
	// 	}
	// 	queue, ok := srv.queueByJobId[jobId]
	// 	if !ok {
	// 		return nil, errors.Errorf("unknown job id %s: could not find queue", jobId)
	// 	}
	// 	runId, ok := srv.runIdByJobId[jobId]
	// 	if !ok {
	// 		return nil, errors.Errorf("unknown job id %s: could not find run id", jobId)
	// 	}
	// 	sequence := &armadaevents.EventSequence{
	// 		Queue:      queue,
	// 		JobSetName: jobSetName,
	// 	}

	// 	jobId, err := armadaevents.ProtoUuidFromUuidString(jobId)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
	// 		Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
	// 			JobRunSucceeded: &armadaevents.JobRunSucceeded{
	// 				RunId: runId,
	// 				JobId: jobId,
	// 			},
	// 		},
	// 	})
	// }

	// err := srv.publishToPulsar(ctx, sequences)
	// if err != nil {
	// 	return nil, err
	// }

	// return &api.IdList{
	// 	Ids: req.Ids,
	// }, nil

	return &api.IdList{}, nil
}

// TODO: Does nothing for now.
func (srv *ExecutorApi) ReportUsage(ctx context.Context, req *api.ClusterUsageReport) (*types.Empty, error) {
	return &types.Empty{}, nil
}

func (srv *ExecutorApi) ReportMultiple(ctx context.Context, apiEvents *api.EventList) (*types.Empty, error) {
	// Because (queue, userId, jobSetId) may differ between events,
	// several sequences may be necessary.
	sequences, err := eventutil.EventSequencesFromApiEvents(apiEvents.Events)
	if err != nil {
		return &types.Empty{}, err
	}
	if len(sequences) == 0 {
		return &types.Empty{}, nil
	}
	return &types.Empty{}, srv.publishToPulsar(ctx, sequences)
}

func (srv *ExecutorApi) Report(ctx context.Context, apiEvent *api.EventMessage) (*types.Empty, error) {
	// Because (queue, userId, jobSetId) may differ between events,
	// several sequences may be necessary.
	sequences, err := eventutil.EventSequencesFromApiEvents([]*api.EventMessage{apiEvent})
	if err != nil {
		return &types.Empty{}, err
	}
	if len(sequences) == 0 {
		return &types.Empty{}, nil
	}

	return &types.Empty{}, srv.publishToPulsar(ctx, sequences)
}

// PublishToPulsar sends pulsar messages async
func (srv *ExecutorApi) publishToPulsar(ctx context.Context, sequences []*armadaevents.EventSequence) error {

	// Reduce the number of sequences to send to the minimum possible,
	// and then break up any sequences larger than srv.MaxAllowedMessageSize.
	sequences = eventutil.CompactEventSequences(sequences)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, 4194304) // 4MB
	if err != nil {
		return err
	}
	// return pulsarutils.PublishSequences(ctx, srv.Producer, sequences)

	return pulsarutils.PublishSequences(ctx, nil, sequences) // TODO: Replace nil
}
