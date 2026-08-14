package armadactl

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

func (a *App) CreateRetryPolicy(policy *api.RetryPolicy) error {
	if err := a.Params.RetryPolicyAPI.Create(policy); err != nil {
		return errors.Errorf("error creating retry policy %s: %s", policy.Name, err)
	}
	fmt.Fprintf(a.Out, "Created retry policy %s\n", policy.Name)
	return nil
}

func (a *App) CreateRetryPolicyFromFile(fileName string) error {
	policy, err := retryPolicyFromFile(fileName)
	if err != nil {
		return err
	}
	return a.CreateRetryPolicy(policy)
}

func (a *App) UpdateRetryPolicy(policy *api.RetryPolicy) error {
	if err := a.Params.RetryPolicyAPI.Update(policy); err != nil {
		return errors.Errorf("error updating retry policy %s: %s", policy.Name, err)
	}
	fmt.Fprintf(a.Out, "Updated retry policy %s\n", policy.Name)
	return nil
}

func (a *App) UpdateRetryPolicyFromFile(fileName string) error {
	policy, err := retryPolicyFromFile(fileName)
	if err != nil {
		return err
	}
	return a.UpdateRetryPolicy(policy)
}

// retryPolicyDocument is the on-disk form of a policy: the resource envelope
// followed by the policy's own fields, written flat rather than nested.
type retryPolicyDocument struct {
	client.Resource
	api.RetryPolicy
}

// retryPolicyListBody is the body of a policy list document. It exists because
// api.RetryPolicyList tags the slice omitempty, which drops the key entirely
// for an empty list.
type retryPolicyListBody struct {
	RetryPolicies []*api.RetryPolicy `json:"retryPolicies"`
}

func retryPolicyFromFile(fileName string) (*api.RetryPolicy, error) {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return nil, errors.Errorf("file %s error: %s", fileName, err)
	}

	// Strict, because a silently dropped retryLimit leaves it at 0, which means
	// never retry.
	doc := &retryPolicyDocument{}
	if err := yaml.UnmarshalStrict(data, doc); err != nil {
		return nil, errors.Errorf("file %s error: %s", fileName, err)
	}

	// Without this a queue definition would pass as a policy, since both carry a name.
	if doc.Version != client.APIVersionV1 {
		return nil, errors.Errorf("file %s error: apiVersion must be %q", fileName, client.APIVersionV1)
	}
	if doc.Kind != client.ResourceKindRetryPolicy {
		return nil, errors.Errorf("file %s error: kind must be %q", fileName, client.ResourceKindRetryPolicy)
	}

	return &doc.RetryPolicy, nil
}

func (a *App) DeleteRetryPolicy(name string) error {
	if err := a.Params.RetryPolicyAPI.Delete(name); err != nil {
		return errors.Errorf("error deleting retry policy %s: %s", name, err)
	}
	fmt.Fprintf(a.Out, "Deleted retry policy %s (or it did not exist)\n", name)
	return nil
}

func (a *App) GetRetryPolicy(name string) error {
	policy, err := a.Params.RetryPolicyAPI.Get(name)
	if err != nil {
		return errors.Errorf("error getting retry policy %s: %s", name, err)
	}
	b, err := yaml.Marshal(policy)
	if err != nil {
		return errors.Errorf("error marshalling retry policy %s: %s", name, err)
	}
	fmt.Fprint(a.Out, retryPolicyHeaderYaml()+string(b))
	return nil
}

func (a *App) GetAllRetryPolicies() error {
	policies, err := a.Params.RetryPolicyAPI.GetAll()
	if err != nil {
		return errors.Errorf("error getting retry policies: %s", err)
	}
	if policies == nil {
		policies = []*api.RetryPolicy{}
	}
	// A mapping, so that it follows the mapping header the document opens with.
	b, err := yaml.Marshal(retryPolicyListBody{RetryPolicies: policies})
	if err != nil {
		return errors.Errorf("error marshalling retry policies: %s", err)
	}
	fmt.Fprint(a.Out, retryPolicyHeaderYaml()+string(b))
	return nil
}

func retryPolicyHeaderYaml() string {
	b, err := yaml.Marshal(client.Resource{
		Version: client.APIVersionV1,
		Kind:    client.ResourceKindRetryPolicy,
	})
	if err != nil {
		panic(err)
	}
	return string(b)
}
