package config

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/api/resource"
	"reflect"
)

var CustomHooks = []viper.DecoderConfigOption{
	viper.DecodeHook(PulsarCompressionTypeHookFunc()),
	viper.DecodeHook(PulsarCompressionLevelHookFunc()),
	viper.DecodeHook(QuantityDecodeHook()),
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
		return pulsarutils.ParsePulsarCompressionType(data.(string))
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
		return pulsarutils.ParsePulsarCompressionType(data.(string))
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
