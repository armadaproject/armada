package config

import (
	"github.com/mitchellh/mapstructure"
	"k8s.io/apimachinery/pkg/api/resource"
	"reflect"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
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

func runHookTest(t *testing.T, tc HookTest, convertTo reflect.Type, hookFunc mapstructure.DecodeHookFuncType) {
	parsed, err := hookFunc(reflect.TypeOf(tc.value), convertTo, tc.value)
	if tc.expectError {
		assert.NotNil(t, err)
	} else {
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, parsed)
	}
}
