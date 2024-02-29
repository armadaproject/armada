package api

import (
	"context"

	"github.com/gogo/status"
	"google.golang.org/grpc"
)

type CustomSubmitClient struct {
	Inner SubmitClient
}

func (c *CustomSubmitClient) SubmitJobs(ctx context.Context, in *JobSubmitRequest, opts ...grpc.CallOption) (*JobSubmitResponse, error) {
	out, err := c.Inner.SubmitJobs(ctx, in, opts...)
	if err != nil {
		st := status.Convert(err)
		for _, detail := range st.Details() {
			switch t := detail.(type) {
			case *JobSubmitResponse:
				return t, err
			}
		}
		return nil, err
	}
	return out, nil
}
