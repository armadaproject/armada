package armadactl

import (
	"fmt"

	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/queue"
	"github.com/armadaproject/armada/pkg/client/util"
)

// CreateQueue calls app.QueueAPI.Create with the provided parameters.
func (a *App) CreateQueue(queue queue.Queue) error {
	if err := a.Params.QueueAPI.Create(queue); err != nil {
		return errors.Errorf("[armadactl.CreateQueue] error creating queue %s: %s", queue.Name, err)
	}
	fmt.Fprintf(a.Out, "Created queue %s\n", queue.Name)
	return nil
}

func (a *App) CreateResource(fileName string, dryRun bool) error {
	var resource client.Resource
	if err := util.BindJsonOrYaml(fileName, &resource); err != nil {
		return err
	}
	if resource.Version != client.APIVersionV1 {
		return errors.Errorf("file %s error: invalid resource field 'apiVersion': %s", fileName, resource.Version)
	}

	switch resource.Kind {
	case client.ResourceKindQueue:
		queue := queue.Queue{}
		if err := util.BindJsonOrYaml(fileName, &queue); err != nil {
			return errors.Errorf("file %s error: %s", fileName, err)
		}
		if !dryRun {
			return a.Params.QueueAPI.Create(queue)
		}
	default:
		return errors.Errorf("invalid resource kind: %s", resource.Kind)
	}

	return nil
}

// DeleteQueue calls app.QueueAPI.Delete with the provided parameters.
func (a *App) DeleteQueue(name string) error {
	if err := a.Params.QueueAPI.Delete(name); err != nil {
		return errors.Errorf("[armadactl.DeleteQueue] error deleting queue %s: %s", name, err)
	}
	fmt.Fprintf(a.Out, "Deleted queue %s (or it did not exist)\n", name)
	return nil
}

// GetQueue calls app.QueueAPI.Get with the provided parameters.
func (a *App) GetQueue(name string) error {
	queue, err := a.Params.QueueAPI.Get(name)
	if err != nil {
		return errors.Errorf("[armadactl.GetQueue] error getting queue %s: %s", name, err)
	}
	b, err := yaml.Marshal(queue)
	if err != nil {
		return errors.Errorf("[armadactl.GetQueue] error unmarshalling queue %s: %s", name, err)
	}
	fmt.Fprintf(a.Out, headerYaml()+string(b))
	return nil
}

func headerYaml() string {
	b, err := yaml.Marshal(client.Resource{
		Version: client.APIVersionV1,
		Kind:    client.ResourceKindQueue,
	})
	if err != nil {
		panic(err)
	}
	return string(b)
}

// UpdateQueue calls app.QueueAPI.Update with the provided parameters.
func (a *App) UpdateQueue(queue queue.Queue) error {
	if err := a.Params.QueueAPI.Update(queue); err != nil {
		return errors.Errorf("error updating queue %s: %s", queue.Name, err)
	}

	fmt.Fprintf(a.Out, "Updated queue %s\n", queue.Name)
	return nil
}
