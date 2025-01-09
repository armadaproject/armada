//go:generate go run ./generate/main.go

package lookoutv2

import (
	"github.com/IBM/pgxpoolprometheus"
	"github.com/go-openapi/loads"
	"github.com/go-openapi/runtime/middleware"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/lookoutv2/configuration"
	"github.com/armadaproject/armada/internal/lookoutv2/conversions"
	"github.com/armadaproject/armada/internal/lookoutv2/gen/restapi"
	"github.com/armadaproject/armada/internal/lookoutv2/gen/restapi/operations"
	"github.com/armadaproject/armada/internal/lookoutv2/repository"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/client"
)

const defaultSchedulingReportVerbosity int32 = 0

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

	collector := pgxpoolprometheus.NewCollector(db, map[string]string{})
	prometheus.MustRegister(collector)

	schedulerAPIConn, err := client.CreateApiConnection(&configuration.SchedulerApiConnection)
	if err != nil {
		return err
	}
	defer schedulerAPIConn.Close()
	schedulerReportingClient := schedulerobjects.NewSchedulerReportingClient(schedulerAPIConn)

	getJobsRepo := repository.NewSqlGetJobsRepository(db)
	groupJobsRepo := repository.NewSqlGroupJobsRepository(db)
	decompressor := compress.NewThreadSafeZlibDecompressor()
	getJobErrorRepo := repository.NewSqlGetJobErrorRepository(db, decompressor)
	getJobRunErrorRepo := repository.NewSqlGetJobRunErrorRepository(db, decompressor)
	getJobRunDebugMessageRepo := repository.NewSqlGetJobRunDebugMessageRepository(db, decompressor)
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
			filters := slices.Map(params.GetJobsRequest.Filters, conversions.FromSwaggerFilter)
			order := conversions.FromSwaggerOrder(params.GetJobsRequest.Order)
			result, err := getJobsRepo.GetJobs(
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
				Jobs: slices.Map(result.Jobs, conversions.ToSwaggerJob),
			})
		},
	)

	api.GroupJobsHandler = operations.GroupJobsHandlerFunc(
		func(params operations.GroupJobsParams) middleware.Responder {
			filters := slices.Map(params.GroupJobsRequest.Filters, conversions.FromSwaggerFilter)
			order := conversions.FromSwaggerOrder(params.GroupJobsRequest.Order)
			result, err := groupJobsRepo.GroupBy(
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
				Groups: slices.Map(result.Groups, conversions.ToSwaggerGroup),
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

	api.GetJobRunDebugMessageHandler = operations.GetJobRunDebugMessageHandlerFunc(
		func(params operations.GetJobRunDebugMessageParams) middleware.Responder {
			ctx := armadacontext.New(params.HTTPRequest.Context(), logger)
			result, err := getJobRunDebugMessageRepo.GetJobRunDebugMessage(ctx, params.GetJobRunDebugMessageRequest.RunID)
			if err != nil {
				return operations.NewGetJobRunDebugMessageBadRequest().WithPayload(conversions.ToSwaggerError(err.Error()))
			}
			return operations.NewGetJobRunDebugMessageOK().WithPayload(&operations.GetJobRunDebugMessageOKBody{
				ErrorString: result,
			})
		},
	)

	api.GetJobErrorHandler = operations.GetJobErrorHandlerFunc(
		func(params operations.GetJobErrorParams) middleware.Responder {
			ctx := armadacontext.New(params.HTTPRequest.Context(), logger)
			result, err := getJobErrorRepo.GetJobErrorMessage(ctx, params.GetJobErrorRequest.JobID)
			if err != nil {
				return operations.NewGetJobErrorBadRequest().WithPayload(conversions.ToSwaggerError(err.Error()))
			}
			return operations.NewGetJobErrorOK().WithPayload(&operations.GetJobErrorOKBody{
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

	api.GetJobReportByIDHandler = operations.GetJobReportByIDHandlerFunc(
		func(params operations.GetJobReportByIDParams) middleware.Responder {
			ctx := armadacontext.New(params.HTTPRequest.Context(), logger)

			jobReportRequest := &schedulerobjects.JobReportRequest{JobId: params.JobID}
			response, err := schedulerReportingClient.GetJobReport(ctx, jobReportRequest)
			if err != nil {
				httpStatus := 500
				if statusError, ok := status.FromError(err); ok {
					httpStatus = runtime.HTTPStatusFromCode(statusError.Code())
				}
				return operations.NewGetJobReportByIDDefault(httpStatus).WithPayload(err.Error())
			}
			return operations.NewGetJobReportByIDOK().WithPayload(response.Report)
		},
	)

	api.GetQueueReportByNameHandler = operations.GetQueueReportByNameHandlerFunc(
		func(params operations.GetQueueReportByNameParams) middleware.Responder {
			ctx := armadacontext.New(params.HTTPRequest.Context(), logger)
			verbosity := defaultSchedulingReportVerbosity
			if params.Verbosity != nil {
				verbosity = int32(*params.Verbosity)
			}

			queueReportRequest := &schedulerobjects.QueueReportRequest{
				QueueName: params.QueueName,
				Verbosity: verbosity,
			}
			response, err := schedulerReportingClient.GetQueueReport(ctx, queueReportRequest)
			if err != nil {
				httpStatus := 500
				if statusError, ok := status.FromError(err); ok {
					httpStatus = runtime.HTTPStatusFromCode(statusError.Code())
				}
				return operations.NewGetQueueReportByNameDefault(httpStatus).WithPayload(err.Error())
			}
			return operations.NewGetQueueReportByNameOK().WithPayload(response.Report)
		},
	)

	api.GetSchedulingReportHandler = operations.GetSchedulingReportHandlerFunc(
		func(params operations.GetSchedulingReportParams) middleware.Responder {
			ctx := armadacontext.New(params.HTTPRequest.Context(), logger)
			verbosity := defaultSchedulingReportVerbosity
			if params.Verbosity != nil {
				verbosity = int32(*params.Verbosity)
			}

			schedulingReportRequest := &schedulerobjects.SchedulingReportRequest{Verbosity: verbosity}
			response, err := schedulerReportingClient.GetSchedulingReport(ctx, schedulingReportRequest)
			if err != nil {
				httpStatus := 500
				if statusError, ok := status.FromError(err); ok {
					httpStatus = runtime.HTTPStatusFromCode(statusError.Code())
				}
				return operations.NewGetSchedulingReportDefault(httpStatus).WithPayload(err.Error())
			}
			return operations.NewGetSchedulingReportOK().WithPayload(response.Report)
		},
	)

	shutdownMetricServer := common.ServeMetrics(uint16(configuration.MetricsPort))
	defer shutdownMetricServer()

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
