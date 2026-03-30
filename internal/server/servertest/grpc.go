package servertest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RequireGrpcCode asserts that err is a gRPC status error with the given code.
func RequireGrpcCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error")
	require.Equal(t, code, st.Code())
}
