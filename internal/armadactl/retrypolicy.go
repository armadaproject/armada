package armadactl

import (
	"fmt"

	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/util"
)

func (a *App) CreateRetryPolicy(policy *api.RetryPolicy) error {
	if err := a.Params.RetryPolicyAPI.Create(policy); err != nil {
		return errors.Errorf("error creating retry policy %s: %s", policy.Name, err)
	}
	fmt.Fprintf(a.Out, "Created retry policy %s\n", policy.Name)
	return nil
}

func (a *App) CreateRetryPolicyFromFile(fileName string) error {
	policy := &api.RetryPolicy{}
	if err := util.BindJsonOrYaml(fileName, policy); err != nil {
		return errors.Errorf("file %s error: %s", fileName, err)
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
	policy := &api.RetryPolicy{}
	if err := util.BindJsonOrYaml(fileName, policy); err != nil {
		return errors.Errorf("file %s error: %s", fileName, err)
	}
	return a.UpdateRetryPolicy(policy)
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
	b, err := yaml.Marshal(policies)
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
