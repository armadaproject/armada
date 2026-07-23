package armadactl

import (
	"fmt"

	"github.com/pkg/errors"
)

// DeleteExecutor publishes a control-plane executor delete request with the provided parameters.
func (a *App) DeleteExecutor(name string) error {
	if err := a.Params.ControlPlaneAPI.DeleteExecutor(name); err != nil {
		return errors.Errorf("[armadactl.DeleteExecutor] error deleting executor %s: %s", name, err)
	}
	fmt.Fprintf(a.Out, "Deleted executor %s (or it did not exist)\n", name)
	return nil
}
