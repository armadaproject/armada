package armadaerrors

import (
	"context"
	goerrors "errors"
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-redis/redis"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/requestid"
)

func TestCodeFromError(t *testing.T) {
	tests := map[string]struct {
		err  error
		want codes.Code
	}{
		"ErrAlreadyExists":                {&ErrAlreadyExists{}, codes.AlreadyExists},
		"ErrNotFound":                     {&ErrNotFound{}, codes.NotFound},
		"ErrInvalidArgument":              {&ErrInvalidArgument{}, codes.InvalidArgument},
		"pkg.Error => ErrAlreadyExists":   {errors.WithMessage(&ErrAlreadyExists{}, "foo"), codes.AlreadyExists},
		"pkg.Error => ErrNotFound":        {errors.WithMessage(&ErrNotFound{}, "foo"), codes.NotFound},
		"pkg.Error => ErrInvalidArgument": {errors.WithMessage(&ErrInvalidArgument{}, "foo"), codes.InvalidArgument},
		"pkg.Error":                       {errors.New("foo"), codes.Unknown},
		"nil":                             {nil, codes.OK},
		"gRPC status":                     {status.New(codes.Internal, "foo").Err(), codes.Internal},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := CodeFromError(tc.err)
			if got != tc.want {
				assert.Equal(t, tc.want, got, "expected %v but got %v", tc.want, got)
			}
		})
	}
}

func TestUnaryServerInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	var handlerErr error
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, handlerErr
	}
	f := UnaryServerInterceptor(1000)

	// nils should be passed through as-is
	handlerErr = nil
	_, err := f(ctx, nil, nil, handler)
	assert.NoError(t, err)

	// gRPC-style errors should be passed through as-is
	handlerErr = status.Error(codes.Aborted, "foo")
	_, err = f(ctx, nil, nil, handler)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Aborted, st.Code())

	// a chain of errors should result in the entire chain being returned
	innerErr := &ErrAlreadyExists{}
	handlerErr = errors.WithMessage(innerErr, "foo")
	_, err = f(ctx, nil, nil, handler)
	st, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.AlreadyExists, st.Code())
	assert.Equal(t, handlerErr.Error(), st.Message())

	// a chain of errors with a stack trace should omit the stack trace
	innerErr = &ErrAlreadyExists{}
	handlerErr = errors.WithMessage(errors.WithStack(innerErr), "foo")
	_, err = f(ctx, nil, nil, handler)
	st, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.AlreadyExists, st.Code())
	assert.Equal(t, errors.WithMessage(innerErr, "foo").Error(), st.Message())

	// if the context contains a request id, it should be included in the error message
	id := "123"
	ctx, ok = requestid.AddToIncomingContext(ctx, id)
	assert.True(t, ok)
	_, err = f(ctx, nil, nil, handler)
	st, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Contains(t, st.Message(), id)

	// the action or method should get added to errors that support it.
	unauthErr := &ErrUnauthenticated{
		Message: "invalid username/password combo for testMethod",
	}
	handlerErr = errors.WithMessage(errors.WithStack(unauthErr), "foo")
	ctx = context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	_, err = f(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "testMethod"}, handler)
	st, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())
	assert.Equal(t, errors.WithMessage(unauthErr, "foo").Error(), st.Message())
	assert.Contains(t, st.Message(), "testMethod")
}

func TestStreamServerInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	stream := &grpc_middleware.WrappedServerStream{}
	stream.WrappedContext = ctx
	var handlerErr error
	handler := func(srv interface{}, stream grpc.ServerStream) error {
		return handlerErr
	}
	f := StreamServerInterceptor(1000)

	// nils should be passed through as-is
	handlerErr = nil
	err := f(nil, stream, nil, handler)
	assert.NoError(t, err)

	// gRPC-style errors should be passed through as-is
	handlerErr = status.Error(codes.Aborted, "foo")
	err = f(nil, stream, nil, handler)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Aborted, st.Code())

	// a chain of errors should result in the message of the cause error being returned
	innerErr := &ErrAlreadyExists{}
	handlerErr = errors.WithMessage(innerErr, "foo")
	err = f(nil, stream, nil, handler)
	st, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.AlreadyExists, st.Code())
	assert.Equal(t, handlerErr.Error(), st.Message())

	// a chain of errors with a stack trace should omit the stack trace
	innerErr = &ErrAlreadyExists{}
	handlerErr = errors.WithMessage(errors.WithStack(innerErr), "foo")
	err = f(nil, stream, nil, handler)
	st, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.AlreadyExists, st.Code())
	assert.Equal(t, errors.WithMessage(innerErr, "foo").Error(), st.Message())

	// if the context contains a request id, it should be included in the error message
	id := "123"
	ctx, ok = requestid.AddToIncomingContext(ctx, id)
	assert.True(t, ok)
	stream.WrappedContext = ctx
	err = f(nil, stream, nil, handler)
	st, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Contains(t, st.Message(), id)

	// the action or method should get added to errors that support it.
	unauthErr := &ErrUnauthenticated{
		Message: "invalid username/password combo for testMethod",
	}
	handlerErr = errors.WithMessage(errors.WithStack(unauthErr), "foo")
	ctx = context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	stream.WrappedContext = ctx
	err = f(ctx, stream, &grpc.StreamServerInfo{FullMethod: "testMethod"}, handler)
	st, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())
	assert.Equal(t, errors.WithMessage(unauthErr, "foo").Error(), st.Message())
	assert.Contains(t, st.Message(), "testMethod")
}

func TestIsNetworkErrorRedis(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:637", // Assume nothing is listening on this port
	})
	cmd := client.Ping()

	err := errors.Wrap(cmd.Err(), "foo")
	assert.True(t, IsNetworkError(err))

	err = fmt.Errorf("%w", cmd.Err())
	assert.True(t, IsNetworkError(err))
}

func TestIsNetworkErrorPulsar(t *testing.T) {
	// Set the timeout really short to immediately get a network error.
	// Setting the timeout to a nanosecond results in a panic in the Pulsar client.
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://pulsar:665", // Assume nothing is listening on this port
		ConnectionTimeout: time.Millisecond,
		OperationTimeout:  time.Millisecond,
	})
	if ok := assert.NoError(t, err); !ok {
		t.FailNow()
	}
	_, err = client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "foo",
		SubscriptionName: "foo",
	})

	assert.True(t, IsNetworkError(errors.Wrap(err, "foo")))
	assert.True(t, IsNetworkError(fmt.Errorf("%w", err)))
}

func TestIsNotNetworkError(t *testing.T) {
	assert.False(t, IsNetworkError(errors.New("foo")))
	assert.False(t, IsNetworkError(goerrors.New("foo")))
	assert.False(t, IsNetworkError(&ErrNotFound{}))
	assert.False(t, IsNetworkError(&ErrAlreadyExists{}))
	assert.False(t, IsNetworkError(&ErrInvalidArgument{}))
	assert.False(t, IsNetworkError(errors.Wrap(&ErrNotFound{}, "foo")))
	assert.False(t, IsNetworkError(fmt.Errorf("%w", &ErrNotFound{})))
}
