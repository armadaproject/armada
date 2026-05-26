package lookout

import (
	"github.com/go-openapi/runtime/middleware"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
	"github.com/armadaproject/armada/internal/lookout/gen/restapi/operations"
)

func versionHandler(version, commit, buildTime string) operations.GetVersionHandlerFunc {
	return operations.GetVersionHandlerFunc(
		func(params operations.GetVersionParams) middleware.Responder {
			return operations.NewGetVersionOK().WithPayload(&models.VersionInfo{
				Version:   version,
				Commit:    commit,
				BuildTime: buildTime,
			})
		},
	)
}
