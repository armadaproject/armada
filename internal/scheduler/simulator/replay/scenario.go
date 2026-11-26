package replay

import (
	"bytes"
	"errors"
	"fmt"
	"maps"

	"github.com/spf13/viper"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

const maxNodes = 10_000

type queueSpec struct {
	Name     string                                      `json:"name"`
	Weight   float64                                     `json:"weight"`
	Cordoned bool                                        `json:"cordoned,omitempty"`
	Limits   map[string]*api.PriorityClassResourceLimits `json:"priority_class_limits,omitempty"`
}

type farmSpec struct {
	SchemaVersion int           `json:"schemaVersion"`
	Clusters      []clusterSpec `json:"clusters"`
}

type clusterSpec struct {
	Name          string         `json:"name"`
	Pool          string         `json:"pool"`
	NodeTemplates []nodeTemplate `json:"nodeTemplates"`
}

type nodeTemplate struct {
	Number         int64             `json:"number"`
	Labels         map[string]string `json:"labels"`
	Taints         []*v1.Taint       `json:"taints"`
	TotalResources struct {
		Resources map[string]string `json:"resources"`
	} `json:"totalResources"`
}

func readSchedulerConfig(raw []byte) (configuration.SchedulingConfig, error) {
	var config configuration.SchedulingConfig
	if _, err := yaml.YAMLToJSONStrict(raw); err != nil {
		return config, err
	}
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewReader(raw)); err != nil {
		return config, err
	}
	if err := v.UnmarshalExact(&config, commonconfig.CustomHooks...); err != nil {
		return config, err
	}
	return config, nil
}

func readFarm(raw []byte, cfg configuration.SchedulingConfig) ([]*schedulerobjects.Executor, resource.Quantity, error) {
	var farm farmSpec
	if err := yaml.UnmarshalStrict(raw, &farm); err != nil {
		return nil, resource.Quantity{}, err
	}
	if farm.SchemaVersion != 1 || farm.Clusters == nil {
		return nil, resource.Quantity{}, errors.New("expected farm schemaVersion 1 with explicit clusters")
	}
	var executors []*schedulerobjects.Executor
	var capacity resource.Quantity
	nodeCount := int64(0)
	for _, cluster := range farm.Clusters {
		executor := &schedulerobjects.Executor{Id: cluster.Name, Pool: cluster.Pool}
		for templateIndex, template := range cluster.NodeTemplates {
			if template.Number <= 0 || template.Number > maxNodes-nodeCount || len(template.TotalResources.Resources) == 0 {
				return nil, resource.Quantity{}, fmt.Errorf("cluster %q template %d has invalid number or missing resources", cluster.Name, templateIndex+1)
			}
			resources := make(map[string]*resource.Quantity, len(template.TotalResources.Resources))
			for name, text := range template.TotalResources.Resources {
				quantity, err := exactQuantity(text)
				if err != nil {
					return nil, resource.Quantity{}, fmt.Errorf("cluster %q template %d resource %q: %w", cluster.Name, templateIndex+1, name, err)
				}
				resources[name] = &quantity
			}
			nodeCount += template.Number
			for range template.Number {
				id := fmt.Sprintf("%s-%d-%d", cluster.Name, templateIndex, len(executor.Nodes))
				labels := maps.Clone(template.Labels)
				if labels == nil {
					labels = make(map[string]string)
				}
				labels["armadaproject.io/clusterName"] = cluster.Name
				labels[cfg.NodeIdLabel] = id
				if cpu := resources[string(v1.ResourceCPU)]; cpu != nil {
					capacity.Add(*cpu)
				}
				executor.Nodes = append(executor.Nodes, &schedulerobjects.Node{
					Id: id, Name: id, Executor: executor.Id, Pool: executor.Pool, Labels: labels,
					Taints: template.Taints, TotalResources: &schedulerobjects.ResourceList{Resources: resources},
				})
			}
		}
		executors = append(executors, executor)
	}
	return executors, capacity, nil
}
