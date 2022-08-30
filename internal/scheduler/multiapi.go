package scheduler

import (
	"context"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/pkg/api"
)

// MultiQueueServer implements AggregatedQueueServer
// and dispatches all calls to several AggregatedQueueServer.
// Results from all component implementations are aggregated before returning to the user.
//
// This allows running several AggregatedQueueServer simultaneously,
// to help simplify migrating to a new implementation of AggregatedQueueServer.
type MultiQueueServer struct {
	api.UnimplementedAggregatedQueueServer
	Servers []api.AggregatedQueueServer
}

func WithChildStreams(ctx context.Context, parent api.AggregatedQueue_StreamingLeaseJobsServer, numChildStreams int, action func(context.Context, []*LeaseJobsChildStream) error) error {
	// ctx is cancelled on error receiving from the parent stream,
	// or if action returns an error.
	g, ctx := errgroup.WithContext(ctx)

	// Setup child streams.
	// Incoming messages are forwarded to all children.
	// Outgoing messages are sent on the parent stream.
	childStreams := make([]*LeaseJobsChildStream, numChildStreams)
	for i := 0; i < numChildStreams; i++ {
		C := make(chan *api.StreamingLeaseRequest)
		defer close(C)
		childStreams[i] = &LeaseJobsChildStream{
			AggregatedQueue_StreamingLeaseJobsServer: parent,
			ctx:                                      ctx,
			C:                                        C,
		}
	}

	// Forward incoming messages to all children
	// until Recv() returns an error.
	g.Go(func() error {
		for {
			m, err := parent.Recv()
			if err != nil {
				return err
			}
			for _, child := range childStreams {
				child.C <- m
			}
		}
	})

	// Run the provided action.
	g.Go(func() error {
		return action(ctx, childStreams)
	})

	return g.Wait()
}

type LeaseJobsChildStream struct {
	// Parent stream. Outgoing messages are sent directly on this.
	api.AggregatedQueue_StreamingLeaseJobsServer
	// Context shared by all child streams.
	// Should be cancelled on error receiving from the parent stream.
	ctx context.Context
	// Queue of incoming messages.
	C chan *api.StreamingLeaseRequest
}

func (s *LeaseJobsChildStream) Recv() (*api.StreamingLeaseRequest, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case msg := <-s.C:
		return msg, nil
	}
}

func (s *LeaseJobsChildStream) RecvMsg(m interface{}) error {
	return errors.New("not implemented")
}

func (s *MultiQueueServer) StreamingLeaseJobs(stream api.AggregatedQueue_StreamingLeaseJobsServer) error {
	return WithChildStreams(stream.Context(), stream, len(s.Servers), func(ctx context.Context, childStreams []*LeaseJobsChildStream) error {
		g, _ := errgroup.WithContext(ctx)
		for i := range s.Servers {
			srv := s.Servers[i]
			stream := childStreams[i]
			g.Go(func() error {
				return srv.StreamingLeaseJobs(stream)
			})
		}
		return g.Wait()
	})
}

func (s *MultiQueueServer) RenewLease(ctx context.Context, req *api.RenewLeaseRequest) (*api.IdList, error) {
	g, ctx := errgroup.WithContext(ctx)
	vs := make([]*api.IdList, len(s.Servers))
	for i := range s.Servers {
		i := i
		g.Go(func() error {
			v, err := s.Servers[i].RenewLease(ctx, req)
			if err != nil {
				return err
			}
			vs[i] = v
			return nil
		})
	}
	err := g.Wait()
	if err != nil {
		return nil, err
	}

	rv := &api.IdList{}
	for _, v := range vs {
		rv.Ids = append(rv.Ids, v.Ids...)
	}

	return rv, nil
}

func (s *MultiQueueServer) ReturnLease(ctx context.Context, req *api.ReturnLeaseRequest) (*types.Empty, error) {
	g, ctx := errgroup.WithContext(ctx)
	for i := range s.Servers {
		i := i
		g.Go(func() error {
			_, err := s.Servers[i].ReturnLease(ctx, req)
			if err != nil {
				return err
			}
			return nil
		})
	}
	err := g.Wait()
	if err != nil {
		return nil, err
	}

	return &types.Empty{}, nil
}

func (s *MultiQueueServer) ReportDone(ctx context.Context, req *api.IdList) (*api.IdList, error) {
	g, ctx := errgroup.WithContext(ctx)
	vs := make([]*api.IdList, len(s.Servers))
	for i := range s.Servers {
		i := i
		g.Go(func() error {
			v, err := s.Servers[i].ReportDone(ctx, req)
			if err != nil {
				return err
			}
			vs[i] = v
			return nil
		})
	}
	err := g.Wait()
	if err != nil {
		return nil, err
	}

	rv := &api.IdList{}
	for _, v := range vs {
		rv.Ids = append(rv.Ids, v.Ids...)
	}

	return rv, nil
}
