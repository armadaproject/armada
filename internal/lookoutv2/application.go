//go:generate go run ./generate/main.go

package lookoutv2

import (
	"github.com/go-openapi/loads"
	"github.com/go-openapi/runtime/middleware"
	"github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutv2/configuration"
	"github.com/armadaproject/armada/internal/lookoutv2/conversions"
	"github.com/armadaproject/armada/internal/lookoutv2/gen/restapi"
	"github.com/armadaproject/armada/internal/lookoutv2/gen/restapi/operations"
	"github.com/armadaproject/armada/internal/lookoutv2/repository"
)

func Serve(configuration configuration.LookoutV2Config) error {
	// load embedded swagger file
	swaggerSpec, err := loads.Analyzed(restapi.SwaggerJSON, "")
	if err != nil {
		return err
	}

	db, err := database.OpenPgxPool(configuration.Postgres)
	if err != nil {
		return err
	}

	getJobsRepo := repository.NewSqlGetJobsRepository(db, false)
	getJobsJsonbRepo := repository.NewSqlGetJobsRepository(db, true)
	groupJobsRepo := repository.NewSqlGroupJobsRepository(db, false)
	groupJobsJsonbRepo := repository.NewSqlGroupJobsRepository(db, true)
	decompressor := compress.NewThreadSafeZlibDecompressor()
	getJobRunErrorRepo := repository.NewSqlGetJobRunErrorRepository(db, decompressor)
	getJobSpecRepo := repository.NewSqlGetJobSpecRepository(db, decompressor)

	// create new service API
	api := operations.NewLookoutAPI(swaggerSpec)

	logger := logrus.NewEntry(logrus.StandardLogger())

	api.Logger = logger.Debugf

	api.GetHealthHandler = operations.GetHealthHandlerFunc(
		func(params operations.GetHealthParams) middleware.Responder {
			return operations.NewGetHealthOK().WithPayload("Health check passed")
		},
	)

	api.GetJobsHandler = operations.GetJobsHandlerFunc(
		func(params operations.GetJobsParams) middleware.Responder {
			filters := util.Map(params.GetJobsRequest.Filters, conversions.FromSwaggerFilter)
			order := conversions.FromSwaggerOrder(params.GetJobsRequest.Order)
			repo := getJobsRepo
			if backend := params.Backend; backend != nil && *backend == "jsonb" {
				repo = getJobsJsonbRepo
			}
			result, err := repo.GetJobs(
				armadacontext.New(params.HTTPRequest.Context(), logger),
				filters,
				params.GetJobsRequest.ActiveJobSets,
				order,
				int(params.GetJobsRequest.Skip),
				int(params.GetJobsRequest.Take),
			)
			if err != nil {
				return operations.NewGetJobsBadRequest().WithPayload(conversions.ToSwaggerError(err.Error()))
			}
			return operations.NewGetJobsOK().WithPayload(&operations.GetJobsOKBody{
				Jobs: util.Map(result.Jobs, conversions.ToSwaggerJob),
			})
		},
	)

	api.GroupJobsHandler = operations.GroupJobsHandlerFunc(
		func(params operations.GroupJobsParams) middleware.Responder {
			filters := util.Map(params.GroupJobsRequest.Filters, conversions.FromSwaggerFilter)
			order := conversions.FromSwaggerOrder(params.GroupJobsRequest.Order)
			repo := groupJobsRepo
			if backend := params.Backend; backend != nil && *backend == "jsonb" {
				repo = groupJobsJsonbRepo
			}
			result, err := repo.GroupBy(
				armadacontext.New(params.HTTPRequest.Context(), logger),
				filters,
				params.GroupJobsRequest.ActiveJobSets,
				order,
				conversions.FromSwaggerGroupedField(params.GroupJobsRequest.GroupedField),
				params.GroupJobsRequest.Aggregates,
				int(params.GroupJobsRequest.Skip),
				int(params.GroupJobsRequest.Take),
			)
			if err != nil {
				return operations.NewGroupJobsBadRequest().WithPayload(conversions.ToSwaggerError(err.Error()))
			}
			return operations.NewGroupJobsOK().WithPayload(&operations.GroupJobsOKBody{
				Groups: util.Map(result.Groups, conversions.ToSwaggerGroup),
			})
		},
	)

	api.GetJobRunErrorHandler = operations.GetJobRunErrorHandlerFunc(
		func(params operations.GetJobRunErrorParams) middleware.Responder {
			ctx := armadacontext.New(params.HTTPRequest.Context(), logger)
			result, err := getJobRunErrorRepo.GetJobRunError(ctx, params.GetJobRunErrorRequest.RunID)
			if err != nil {
				return operations.NewGetJobRunErrorBadRequest().WithPayload(conversions.ToSwaggerError(err.Error()))
			}
			return operations.NewGetJobRunErrorOK().WithPayload(&operations.GetJobRunErrorOKBody{
				ErrorString: result,
			})
		},
	)

	api.GetJobSpecHandler = operations.GetJobSpecHandlerFunc(
		func(params operations.GetJobSpecParams) middleware.Responder {
			ctx := armadacontext.New(params.HTTPRequest.Context(), logger)
			result, err := getJobSpecRepo.GetJobSpec(ctx, params.GetJobSpecRequest.JobID)
			if err != nil {
				return operations.NewGetJobSpecBadRequest().WithPayload(conversions.ToSwaggerError(err.Error()))
			}
			return operations.NewGetJobSpecOK().WithPayload(&operations.GetJobSpecOKBody{
				Job: result,
			})
		},
	)

	server := restapi.NewServer(api)
	defer func() {
		shutdownErr := server.Shutdown()
		if shutdownErr != nil {
			logger.WithError(shutdownErr).Error("Failed to shut down server")
		}
	}()

	if configuration.Tls.Enabled {
		server.EnabledListeners = []string{"https"}
		server.TLSPort = configuration.ApiPort
		server.TLSCertificate = flags.Filename(configuration.Tls.CertPath)
		server.TLSCertificateKey = flags.Filename(configuration.Tls.KeyPath)
	} else {
		server.Port = configuration.ApiPort
	}

	restapi.SetCorsAllowedOrigins(configuration.CorsAllowedOrigins) // This needs to happen before ConfigureAPI
	server.ConfigureAPI()
	if err := server.Serve(); err != nil {
		return err
	}

	return err
}
