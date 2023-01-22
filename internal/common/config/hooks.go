package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

var CustomHooks = []viper.DecoderConfigOption{
	addDecodeHook(PulsarCompressionTypeHookFunc()),
	addDecodeHook(PulsarCompressionLevelHookFunc()),
	addDecodeHook(QuantityDecodeHook()),
}

func PulsarCompressionTypeHookFunc() mapstructure.DecodeHookFuncType {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		// check that src and target types are valid
		if f.Kind() != reflect.String || t != reflect.TypeOf(pulsar.NoCompression) {
			return data, nil
		}
		switch strings.ToLower(data.(string)) {
		case "", "none":
			return pulsar.NoCompression, nil
		case "lz4":
			return pulsar.LZ4, nil
		case "zlib":
			return pulsar.ZLib, nil
		case "zstd":
			return pulsar.ZSTD, nil
		default:
			return pulsar.NoCompression, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "pulsar.CompressionType",
				Value:   data,
				Message: fmt.Sprintf("Unknown Pulsar compression type %s", data),
			})
		}
	}
}

func PulsarCompressionLevelHookFunc() mapstructure.DecodeHookFuncType {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		// check that src and target types are valid
		if f.Kind() != reflect.String || t != reflect.TypeOf(pulsar.Default) {
			return data, nil
		}
		switch strings.ToLower(data.(string)) {
		case "", "default":
			return pulsar.Default, nil
		case "faster":
			return pulsar.Faster, nil
		case "better":
			return pulsar.Better, nil
		default:
			return pulsar.Default, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "pulsar.CompressionLevel",
				Value:   data,
				Message: fmt.Sprintf("Unknown Pulsar compression level %s", data),
			})
		}
	}
}

func QuantityDecodeHook() mapstructure.DecodeHookFuncType {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if t != reflect.TypeOf(resource.Quantity{}) {
			return data, nil
		}
		return resource.ParseQuantity(fmt.Sprintf("%v", data))
	}
}

func addDecodeHook(hook mapstructure.DecodeHookFuncType) viper.DecoderConfigOption {
	return func(c *mapstructure.DecoderConfig) {
		c.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			c.DecodeHook,
			hook)
	}
}
