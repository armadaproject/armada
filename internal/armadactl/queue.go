package armadactl

import (
	"fmt"
	"strings"

	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/api"

	"github.com/pkg/errors"
	goslices "golang.org/x/exp/slices"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/queue"
	"github.com/armadaproject/armada/pkg/client/util"
)

// QueueQueryArgs is used for retrieving queues or for cordoning/uncordoning
type QueueQueryArgs struct {
	// Filter for queues where the InQueueNames slice contains the queue name
	InQueueNames []string
	// Filter for queues where the queue contains all labels specified in the ContainsAllLabels slice
	ContainsAllLabels []string
	// Filter for cordoned queues only
	OnlyCordoned bool
	// Applies the above filters and inverts the result
	InvertResult bool
}

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
	fmt.Fprint(a.Out, headerYaml()+string(b))
	return nil
}

func (a *App) getAllQueuesAsAPIQueue(args *QueueQueryArgs) ([]*api.Queue, error) {
	queueFilters := func(q *api.Queue) bool {
		containsAllLabels := slices.AllFunc(args.ContainsAllLabels, func(label string) bool {
			splitLabel := strings.Split(label, "=")
			if len(splitLabel) >= 2 {
				queueLabelValue, ok := q.Labels[splitLabel[0]]
				return ok && queueLabelValue == strings.Join(splitLabel[1:], "")
			} else if len(splitLabel) == 1 {
				// If the label is a key, we compare on keys
				_, ok := q.Labels[splitLabel[0]]
				return ok
			}

			return false
		})
		inQueues := len(args.InQueueNames) == 0 || goslices.Contains(args.InQueueNames, q.Name)
		matchesLabelsAndQueues := containsAllLabels && inQueues
		onlyCordonedCheck := (args.OnlyCordoned && q.Cordoned) || !args.OnlyCordoned
		return args.InvertResult != (matchesLabelsAndQueues && onlyCordonedCheck)
	}
	queuesToReturn, err := a.Params.QueueAPI.GetAll()
	if err != nil {
		return nil, errors.Errorf("error getting all queues: %s", err)
	}

	return slices.Filter(queuesToReturn, queueFilters), nil
}

// GetAllQueues calls app.QueueAPI.GetAll with the provided parameters. This method fetches all queues, and filters
// for those where the queue name is in the provided queueNames and the queue contains all specified labels. If either
// the labels or queueNames slices are empty, any checks on them are ignored. The inverse flag inverts the result,
// returning all queues not matching the specified criteria.
func (a *App) GetAllQueues(args *QueueQueryArgs) error {
	queues, err := a.getAllQueuesAsAPIQueue(args)
	if err != nil {
		return errors.Errorf("error getting all queues: %s", err)
	}

	b, err := yaml.Marshal(queues)
	if err != nil {
		return errors.Errorf("error unmarshalling queues: %s", err)
	}
	fmt.Fprint(a.Out, headerYaml()+string(b))
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
