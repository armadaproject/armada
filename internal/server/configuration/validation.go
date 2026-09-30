package configuration

import (
	"github.com/go-playground/validator/v10"
	"k8s.io/apimachinery/pkg/util/validation"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

// maxObjectNamePrefixLength keeps the generated service names of a job within the 63 characters of a DNS label.
// A service name is <prefix>-<jobId>-0-service-<index>. A job ID has 26 characters, so a prefix of 20 characters
// leaves 5 characters for the index. An ingress host also contains the container name and port of the job.
const maxObjectNamePrefixLength = 20

const objectNamePrefixErrorMessage = "must be a DNS-1035 label of at most 20 characters"

func (c ArmadaConfig) Validate() error {
	validate := validator.New()
	validate.RegisterStructValidation(submissionConfigValidation, SubmissionConfig{})
	return validate.Struct(c)
}

func submissionConfigValidation(sl validator.StructLevel) {
	c := sl.Current().Interface().(SubmissionConfig)
	if len(c.ObjectNamePrefix) > maxObjectNamePrefixLength || len(validation.IsDNS1035Label(c.ObjectNamePrefix)) > 0 {
		sl.ReportError(c.ObjectNamePrefix, "ObjectNamePrefix", "", objectNamePrefixErrorMessage, "")
	}
}

func (c *ArmadaConfig) Mutate() (commonconfig.Config, error) {
	c.Observability.ApplyResourceDefaults("server")
	return c, nil
}
