package armadactl

import (
	"fmt"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"

	"github.com/pkg/errors"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/api/schedulerobjects"
	"github.com/armadaproject/armada/pkg/client"
)

// DeleteExecutor calls app.ExecutorAPI.Delete with the provided executor name.
func (a *App) DeleteExecutor(name string) error {
	if err := a.Params.ExecutorAPI.Delete(name); err != nil {
		return fmt.Errorf("error deleting executor %s: %s", name, err)
	}
	fmt.Fprintf(a.Out, "Deleted executor %s (or it did not exist)\n", name)
	return nil
}

type executorListBody struct {
	Executors []*schedulerobjects.ExecutorSummary `json:"executors"`
}

// GetExecutor prints the executor with the given name.
func (a *App) GetExecutor(name string) error {
	executors, err := a.Params.ExecutorAPI.Get(name)
	if err != nil {
		return errors.Errorf("error getting executor %s: %s", name, err)
	}
	if len(executors) == 0 {
		return errors.Errorf("executor %s not found", name)
	}
	if a.Params.Output == OutputText {
		return a.printExecutorTable(executors)
	}
	if err := a.emitDocument(client.ResourceKindExecutor, executors[0]); err != nil {
		return errors.Errorf("error writing executor: %s", err)
	}
	return nil
}

// GetAllExecutors prints all executors known to the scheduler.
func (a *App) GetAllExecutors() error {
	executors, err := a.Params.ExecutorAPI.Get("")
	if err != nil {
		return errors.Errorf("error getting executors: %s", err)
	}
	if a.Params.Output == OutputText {
		return a.printExecutorTable(executors)
	}
	body := executorListBody{Executors: executors}
	if body.Executors == nil {
		body.Executors = []*schedulerobjects.ExecutorSummary{}
	}
	if err := a.emitDocument(client.ResourceKindExecutor, body); err != nil {
		return errors.Errorf("error writing executors: %s", err)
	}
	return nil
}

func (a *App) printExecutorTable(executors []*schedulerobjects.ExecutorSummary) error {
	if len(executors) == 0 {
		fmt.Fprintln(a.Out, "No executors found")
		return nil
	}
	w := tabwriter.NewWriter(a.Out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tPOOL\tNODES\tLAST HEARTBEAT\tCORDONED\tCORDON REASON\tCORDONED BY")
	for _, e := range executors {
		lastUpdate := "-"
		if e.LastUpdateTime != nil {
			lastUpdate = protoutil.ToStdTime(e.LastUpdateTime).Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%t\t%s\t%s\n",
			escapeCell(e.Name), escapeCell(e.Pool), e.NodeCount, lastUpdate, e.Cordoned,
			dashIfEmpty(escapeCell(e.CordonReason)), dashIfEmpty(escapeCell(e.CordonedBy)))
	}
	return w.Flush()
}

// escapeCell makes a value safe to print in a table cell. Control characters such as newlines and tabs would
// otherwise split a row or shift the columns, so they are written as escape sequences.
func escapeCell(s string) string {
	if !strings.ContainsFunc(s, unicode.IsControl) {
		return s
	}
	var sb strings.Builder
	for _, r := range s {
		switch {
		case r == '\n':
			sb.WriteString(`\n`)
		case r == '\r':
			sb.WriteString(`\r`)
		case r == '\t':
			sb.WriteString(`\t`)
		case unicode.IsControl(r):
			fmt.Fprintf(&sb, `\x%02x`, r)
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
