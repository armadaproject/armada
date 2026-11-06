package startup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePulledMessage(t *testing.T) {
	tests := map[string]struct {
		message          string
		expectRecognised bool
		expectDuration   time.Duration
		expectWaiting    time.Duration
		expectSizeBytes  *uint64
		expectCached     bool
	}{
		"successful pull": {
			message:          `Successfully pulled image "repo/img:1.0" in 58.204s (1m1.902s including waiting). Image size: 2415919104 bytes.`,
			expectRecognised: true,
			expectDuration:   58204 * time.Millisecond,
			// waiting_time is the difference of the two nested intervals, not the second figure.
			expectWaiting:   3698 * time.Millisecond,
			expectSizeBytes: new(uint64(2415919104)),
		},
		// Go renders durations over a minute as 1m2.5s, so the common case for a large image is not
		// a plain seconds value. This is what breaks a naive regex.
		"durations over a minute": {
			message:          `Successfully pulled image "repo/img:1.0" in 1m2s (2m5s including waiting). Image size: 100 bytes.`,
			expectRecognised: true,
			expectDuration:   62 * time.Second,
			expectWaiting:    63 * time.Second,
			expectSizeBytes:  new(uint64(100)),
		},
		"no waiting": {
			message:          `Successfully pulled image "repo/img:1.0" in 4.102s (4.102s including waiting). Image size: 183500800 bytes.`,
			expectRecognised: true,
			expectDuration:   4102 * time.Millisecond,
			expectWaiting:    0,
			expectSizeBytes:  new(uint64(183500800)),
		},
		// Kubelet uses 0 to indicate unknown, so we only set if non-zero
		"zero size is unknown": {
			message:          `Successfully pulled image "repo/img:1.0" in 1s (1s including waiting). Image size: 0 bytes.`,
			expectRecognised: true,
			expectDuration:   time.Second,
			expectSizeBytes:  nil,
		},
		"cached": {
			message:          `Container image "repo/img:1.0" already present on machine`,
			expectRecognised: true,
			expectCached:     true,
		},
		// Added in Kubernetes 1.33 on the credential verification path.
		"cached with credential verification": {
			message:          `Container image "repo/img:1.0" already present on machine and can be accessed by the pod`,
			expectRecognised: true,
			expectCached:     true,
		},
		"unrecognised": {
			message:          `Successfully pulled image "repo/img:1.0" in some new format we do not know`,
			expectRecognised: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pull := parsePulledMessage(tc.message)

			if !tc.expectRecognised {
				assert.Nil(t, pull)
				return
			}
			require.NotNil(t, pull)
			assert.Equal(t, tc.expectCached, pull.Cached)

			if tc.expectCached {
				assert.Nil(t, pull.Duration, "a cache hit has no pull to time")
				assert.Nil(t, pull.WaitingTime)
				return
			}
			assert.Equal(t, tc.expectDuration, mustDuration(t, pull.Duration))
			assert.Equal(t, tc.expectWaiting, mustDuration(t, pull.WaitingTime))

			if tc.expectSizeBytes == nil {
				assert.Nil(t, pull.SizeBytes)
				return
			}
			require.NotNil(t, pull.SizeBytes)
			assert.Equal(t, *tc.expectSizeBytes, pull.SizeBytes.Value)
		})
	}
}
