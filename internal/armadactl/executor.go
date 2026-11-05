package armadactl

import (
	"fmt"
)

// DeleteExecutor calls app.ExecutorAPI.Delete with the provided executor name.
func (a *App) DeleteExecutor(name string) error {
	if err := a.Params.ExecutorAPI.Delete(name); err != nil {
		return fmt.Errorf("error deleting executor %s: %s", name, err)
	}
	fmt.Fprintf(a.Out, "Deleted executor %s (or it did not exist)\n", name)
	return nil
}
