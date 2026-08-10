package config

import (
	"reflect"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-viper/mapstructure/v2"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

type HookTest struct {
	value       interface{}
	expected    interface{}
	expectError bool
}

func TestPulsarCompressionTypeHookFunc(t *testing.T) {
	tests := map[string]HookTest{
		"empty string": {
			value:    "",
			expected: pulsar.NoCompression,
		},
		"zlib": {
			value:    "zlib",
			expected: pulsar.ZLib,
		},
		"zstd": {
			value:    "zstd",
			expected: pulsar.ZSTD,
		},
		"lz4": {
			value:    "lz4",
			expected: pulsar.LZ4,
		},
		"case insensitive": {
			value:    "zLiB",
			expected: pulsar.ZLib,
		},
		"unknown": {
			value:       "not a valid compression",
			expectError: true,
		},
		"not string input": {
			value:    1,
			expected: 1,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			runHookTest(t, tc, reflect.TypeOf(pulsar.ZLib), PulsarCompressionTypeHookFunc())
		})
	}
}

func TestPulsarCompressionLevelHookFunc(t *testing.T) {
	tests := map[string]HookTest{
		"empty string": {
			value:    "",
			expected: pulsar.Default,
		},
		"faster": {
			value:    "faster",
			expected: pulsar.Faster,
		},
		"better": {
			value:    "better",
			expected: pulsar.Better,
		},
		"default": {
			value:    "default",
			expected: pulsar.Default,
		},
		"case insensitive": {
			value:    "FaSTer",
			expected: pulsar.Faster,
		},
		"unknown": {
			value:       "not a valid compression type",
			expectError: true,
		},
		"not string input": {
			value:    1,
			expected: 1,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			runHookTest(t, tc, reflect.TypeOf(pulsar.Better), PulsarCompressionLevelHookFunc())
		})
	}
}

func TestQuantityDecodeHook(t *testing.T) {
	tests := map[string]HookTest{
		"1": {
			value:    "1",
			expected: resource.MustParse("1"),
		},
		"100m": {
			value:    "100m",
			expected: resource.MustParse("100m"),
		},
		"100Mi": {
			value:    "100Mi",
			expected: resource.MustParse("100Mi"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			runHookTest(t, tc, reflect.TypeOf(resource.Quantity{}), QuantityDecodeHook())
		})
	}
}

// optInType opts in to string decoding via StringConfigUnmarshaler.
type optInType struct{ Name string }

func (o *optInType) UnmarshalConfigString(s string) error {
	o.Name = s
	return nil
}

// textOnlyType implements encoding.TextUnmarshaler (e.g. for JSON/proto) but does
// NOT opt in to StringConfigUnmarshaler, so the hook must leave it untouched.
type textOnlyType struct{ Name string }

func (o *textOnlyType) UnmarshalText(text []byte) error {
	o.Name = string(text)
	return nil
}

func TestStringConfigUnmarshalerHook(t *testing.T) {
	tests := map[string]struct {
		value     interface{}
		convertTo reflect.Type
		expected  interface{}
	}{
		"decodes opt-in type": {
			value:     "poolA",
			convertTo: reflect.TypeOf(optInType{}),
			expected:  &optInType{Name: "poolA"},
		},
		// textOnlyType implements encoding.TextUnmarshaler but does not opt in, so
		// the string passes through unchanged and its UnmarshalText is not invoked.
		"ignores plain TextUnmarshaler type": {
			value:     "poolA",
			convertTo: reflect.TypeOf(textOnlyType{}),
			expected:  "poolA",
		},
		"ignores non-string source": {
			value:     1,
			convertTo: reflect.TypeOf(optInType{}),
			expected:  1,
		},
	}
	hook := StringConfigUnmarshalerHook()
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			out, err := hook(reflect.TypeOf(tc.value), tc.convertTo, tc.value)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, out)
		})
	}
}

func runHookTest(t *testing.T, tc HookTest, convertTo reflect.Type, hookFunc mapstructure.DecodeHookFuncType) {
	parsed, err := hookFunc(reflect.TypeOf(tc.value), convertTo, tc.value)
	if tc.expectError {
		assert.NotNil(t, err)
	} else {
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, parsed)
	}
}
