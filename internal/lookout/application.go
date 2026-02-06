//go:generate go run ./generate/main.go

package lookout

import (
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/IBM/pgxpoolprometheus"
	"github.com/go-openapi/loads"
	"github.com/go-openapi/runtime/middleware"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/lookout/configuration"
	"github.com/armadaproject/armada/internal/lookout/conversions"
	"github.com/armadaproject/armada/internal/lookout/gen/restapi"
	"github.com/armadaproject/armada/internal/lookout/gen/restapi/operations"
	"github.com/armadaproject/armada/internal/lookout/model"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/internal/lookoutmcp/mcp"
)

func Serve(configuration configuration.LookoutConfig) error {
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

	getJobsRepo := repository.NewSqlGetJobsRepository(db)
	groupJobsRepo := repository.NewSqlGroupJobsRepository(db)
	decompressor := compress.NewThreadSafeZlibDecompressor()
	getJobErrorRepo := repository.NewSqlGetJobErrorRepository(db, decompressor)
	getJobRunErrorRepo := repository.NewSqlGetJobRunErrorRepository(db, decompressor)
	getJobRunDebugMessageRepo := repository.NewSqlGetJobRunDebugMessageRepository(db, decompressor)
	getJobSpecRepo := repository.NewSqlGetJobSpecRepository(db, decompressor)

	// create new service API
	api := operations.NewLookoutAPI(swaggerSpec)

	logger := logging.StdLogger()

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

	authServices, err := auth.ConfigureAuth(configuration.Auth)
	if err != nil {
		return fmt.Errorf("creating auth services: %w", err)
	}

	restapi.SetAuthService(auth.NewMultiAuthService(authServices))
	restapi.SetCorsAllowedOrigins(configuration.CorsAllowedOrigins) // This needs to happen before ConfigureAPI

	if configuration.MCPEnabled {
		logger.Info("MCP endpoint enabled")
		restapi.SetMCPHandler(mcpHandler(db, configuration, logger))
	}

	server.ConfigureAPI()

	if err := server.Serve(); err != nil {
		return err
	}

	return err
}

type mcpServerState struct {
	config                configuration.LookoutConfig
	getJobsRepo           *repository.SqlGetJobsRepository
	getJobSpecRepo        repository.GetJobSpecRepository
	getJobErrorRepo       repository.GetJobErrorRepository
	getJobRunErrorRepo    repository.GetJobRunErrorRepository
	getJobRunDebugMsgRepo repository.GetJobRunDebugMessageRepository
	logger                *logging.Logger
	marshaler             jsonpb.Marshaler
}

func mcpHandler(db *pgxpool.Pool, config configuration.LookoutConfig, logger *logging.Logger) http.HandlerFunc {
	decompressor := compress.NewThreadSafeZlibDecompressor()
	state := &mcpServerState{
		config:                config,
		getJobsRepo:           repository.NewSqlGetJobsRepository(db),
		getJobSpecRepo:        repository.NewSqlGetJobSpecRepository(db, decompressor),
		getJobErrorRepo:       repository.NewSqlGetJobErrorRepository(db, decompressor),
		getJobRunErrorRepo:    repository.NewSqlGetJobRunErrorRepository(db, decompressor),
		getJobRunDebugMsgRepo: repository.NewSqlGetJobRunDebugMessageRepository(db, decompressor),
		logger:                logger,
		marshaler: jsonpb.Marshaler{
			OrigName:     true,
			EmitDefaults: false,
		},
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			logger.WithError(err).Error("error reading request body")
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		ctx := armadacontext.New(r.Context(), logger)
		if err := state.handleMCPRequest(*ctx, body, w); err != nil {
			logger.WithError(err).Error("error handling MCP request")
		}
	}
}

var jobIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

func validateJobID(jobID string) error {
	if jobID == "" {
		return errors.New("jobId is required")
	}
	if len(jobID) > 128 {
		return errors.New("jobId must be at most 128 characters")
	}
	if !jobIDPattern.MatchString(jobID) {
		return errors.New("jobId contains invalid characters (allowed: alphanumeric, underscore, hyphen)")
	}
	return nil
}

func extractJobID(args map[string]any) (string, error) {
	jobID, ok := args["jobId"].(string)
	if !ok {
		return "", errors.New("jobId must be a string")
	}
	if err := validateJobID(jobID); err != nil {
		return "", err
	}
	return jobID, nil
}

func (s *mcpServerState) handleMCPRequest(ctx armadacontext.Context, data []byte, w http.ResponseWriter) error {
	var req mcp.Request
	if err := json.Unmarshal(data, &req); err != nil {
		s.logger.WithError(err).WithField("data", string(data)).Error("failed to parse MCP request")
		_ = s.sendMCPError(w, nil, mcp.ErrorCodeParseError, "parse error", err.Error())
		return err
	}

	logger := s.logger.WithField("method", req.Method).WithField("requestId", req.ID)

	switch req.Method {
	case "initialize":
		logger.Debug("handling initialize request")
		return s.handleMCPInitialize(w, req)
	case "tools/list":
		logger.Debug("handling tools/list request")
		return s.handleMCPListTools(w, req)
	case "tools/call":
		logger.Debug("handling tools/call request")
		return s.handleMCPCallTool(ctx, w, req)
	default:
		logger.WithField("method", req.Method).Warn("unknown method")
		_ = s.sendMCPError(w, req.ID, mcp.ErrorCodeMethodNotFound, "method not found", nil)
		return nil
	}
}

func (s *mcpServerState) handleMCPInitialize(w http.ResponseWriter, req mcp.Request) error {
	var params mcp.InitializeParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		_ = s.sendMCPError(w, req.ID, mcp.ErrorCodeInvalidParams, "invalid params", err.Error())
		return err
	}

	result := mcp.InitializeResult{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities: mcp.ServerCapabilities{
			Tools: &mcp.ToolsCapability{},
		},
		ServerInfo: mcp.Implementation{
			Name:    "lookout-mcp-server",
			Version: "1.0.0",
		},
	}

	return s.sendMCPResult(w, req.ID, result)
}

func (s *mcpServerState) handleMCPListTools(w http.ResponseWriter, req mcp.Request) error {
	tools := []mcp.Tool{
		{
			Name:        "search_jobs",
			Description: "Search for jobs with flexible filters (queue, jobSet, owner, state, etc.)",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"filters": {
						"type": "array",
						"description": "List of filters to apply",
						"items": {
							"type": "object",
							"properties": {
								"field": {"type": "string", "description": "Field name (queue, jobSet, owner, state, etc.)"},
								"match": {"type": "string", "description": "Match type (exact, contains, startsWith, etc.)"},
								"value": {"description": "Value to match"}
							},
							"required": ["field", "match", "value"]
						}
					},
					"skip": {
						"type": "integer",
						"description": "Number of results to skip for pagination",
						"default": 0
					},
					"take": {
						"type": "integer",
						"description": "Number of results to return",
						"default": 10,
						"maximum": 100
					}
				}
			}`),
		},
		{
			Name:        "get_job_details",
			Description: "Get detailed information about a job by its ID, including state, resources, runs, and metadata",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"jobId": {
						"type": "string",
						"description": "The unique identifier of the job"
					}
				},
				"required": ["jobId"]
			}`),
		},
		{
			Name:        "get_job_spec",
			Description: "Get the full job specification for a job by its ID",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"jobId": {
						"type": "string",
						"description": "The unique identifier of the job"
					}
				},
				"required": ["jobId"]
			}`),
		},
		{
			Name:        "get_job_error",
			Description: "Get the error message for a failed job",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"jobId": {
						"type": "string",
						"description": "The unique identifier of the job"
					}
				},
				"required": ["jobId"]
			}`),
		},
		{
			Name:        "get_job_run_error",
			Description: "Get the error message for a specific job run",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"runId": {
						"type": "string",
						"description": "The unique identifier of the job run"
					}
				},
				"required": ["runId"]
			}`),
		},
		{
			Name:        "get_job_run_debug",
			Description: "Get debug information for a specific job run",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"runId": {
						"type": "string",
						"description": "The unique identifier of the job run"
					}
				},
				"required": ["runId"]
			}`),
		},
		{
			Name:        "get_job_commands",
			Description: "Get the list of available commands for a job (e.g., kubectl logs, kubectl exec)",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"jobId": {
						"type": "string",
						"description": "The unique identifier of the job"
					}
				},
				"required": ["jobId"]
			}`),
		},
		{
			Name:        "get_job_links",
			Description: "Get the list of external links related to a job",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"jobId": {
						"type": "string",
						"description": "The unique identifier of the job"
					}
				},
				"required": ["jobId"]
			}`),
		},
		{
			Name:        "get_lookout_ui_link",
			Description: "Get the Lookout UI link for viewing a job",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"jobId": {
						"type": "string",
						"description": "The unique identifier of the job"
					}
				},
				"required": ["jobId"]
			}`),
		},
	}

	result := mcp.ListToolsResult{
		Tools: tools,
	}

	return s.sendMCPResult(w, req.ID, result)
}

func (s *mcpServerState) handleMCPCallTool(ctx armadacontext.Context, w http.ResponseWriter, req mcp.Request) error {
	var params mcp.CallToolParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		_ = s.sendMCPError(w, req.ID, mcp.ErrorCodeInvalidParams, "invalid params", err.Error())
		return err
	}

	var result mcp.CallToolResult
	var err error

	switch params.Name {
	case "search_jobs":
		result, err = s.handleSearchJobs(ctx, params.Arguments)
	case "get_job_details":
		result, err = s.handleGetJobDetails(ctx, params.Arguments)
	case "get_job_spec":
		result, err = s.handleGetJobSpec(ctx, params.Arguments)
	case "get_job_error":
		result, err = s.handleGetJobError(ctx, params.Arguments)
	case "get_job_run_error":
		result, err = s.handleGetJobRunError(ctx, params.Arguments)
	case "get_job_run_debug":
		result, err = s.handleGetJobRunDebug(ctx, params.Arguments)
	case "get_job_commands":
		result, err = s.handleGetJobCommands(ctx, params.Arguments)
	case "get_job_links":
		result, err = s.handleGetJobLinks(ctx, params.Arguments)
	case "get_lookout_ui_link":
		result, err = s.handleGetLookoutUILink(params.Arguments)
	default:
		_ = s.sendMCPError(w, req.ID, mcp.ErrorCodeInvalidParams, "unknown tool", params.Name)
		return nil
	}

	if err != nil {
		result = mcp.CallToolResult{
			Content: []mcp.Content{
				{
					Type: "text",
					Text: fmt.Sprintf("Error: %v", err),
				},
			},
			IsError: true,
		}
	}

	return s.sendMCPResult(w, req.ID, result)
}

func (s *mcpServerState) handleGetJobDetails(ctx armadacontext.Context, args map[string]any) (mcp.CallToolResult, error) {
	jobID, err := extractJobID(args)
	if err != nil {
		return mcp.CallToolResult{}, err
	}

	logger := s.logger.WithField("jobId", jobID).WithField("tool", "get_job_details")
	logger.Debug("getting job details")

	filters := []*model.Filter{
		{
			Field: "jobId",
			Match: model.MatchExact,
			Value: jobID,
		},
	}

	result, err := s.getJobsRepo.GetJobs(&ctx, filters, false, nil, 0, 1)
	if err != nil {
		logger.WithError(err).Error("failed to get job")
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to get job")
	}

	if len(result.Jobs) == 0 {
		logger.Warn("job not found")
		return mcp.CallToolResult{}, errors.Errorf("job with ID %s not found", jobID)
	}

	job := result.Jobs[0]

	jsonData, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to marshal job to JSON")
	}

	return mcp.CallToolResult{
		Content: []mcp.Content{
			{
				Type: "text",
				Text: string(jsonData),
			},
		},
	}, nil
}

func (s *mcpServerState) handleGetJobSpec(ctx armadacontext.Context, args map[string]any) (mcp.CallToolResult, error) {
	jobID, err := extractJobID(args)
	if err != nil {
		return mcp.CallToolResult{}, err
	}

	logger := s.logger.WithField("jobId", jobID).WithField("tool", "get_job_spec")
	logger.Debug("getting job spec")

	jobSpec, err := s.getJobSpecRepo.GetJobSpec(&ctx, jobID)
	if err != nil {
		logger.WithError(err).Error("failed to get job spec")
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to get job spec")
	}

	jsonStr, err := s.marshaler.MarshalToString(jobSpec)
	if err != nil {
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to marshal job spec to JSON")
	}

	var formatted map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &formatted); err != nil {
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to parse JSON for formatting")
	}

	jsonData, err := json.MarshalIndent(formatted, "", "  ")
	if err != nil {
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to format JSON")
	}

	return mcp.CallToolResult{
		Content: []mcp.Content{
			{
				Type: "text",
				Text: string(jsonData),
			},
		},
	}, nil
}

func (s *mcpServerState) handleGetJobCommands(ctx armadacontext.Context, args map[string]any) (mcp.CallToolResult, error) {
	jobID, err := extractJobID(args)
	if err != nil {
		return mcp.CallToolResult{}, err
	}

	logger := s.logger.WithField("jobId", jobID).WithField("tool", "get_job_commands")
	logger.Debug("getting job commands")

	filters := []*model.Filter{
		{
			Field: "jobId",
			Match: model.MatchExact,
			Value: jobID,
		},
	}

	result, err := s.getJobsRepo.GetJobs(&ctx, filters, false, nil, 0, 1)
	if err != nil {
		logger.WithError(err).Error("failed to get job")
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to get job")
	}

	if len(result.Jobs) == 0 {
		logger.Warn("job not found")
		return mcp.CallToolResult{}, errors.Errorf("job with ID %s not found", jobID)
	}

	job := result.Jobs[0]

	commands := s.renderCommands(job)

	jsonData, err := json.MarshalIndent(commands, "", "  ")
	if err != nil {
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to marshal commands to JSON")
	}

	return mcp.CallToolResult{
		Content: []mcp.Content{
			{
				Type: "text",
				Text: string(jsonData),
			},
		},
	}, nil
}

func (s *mcpServerState) handleGetJobLinks(ctx armadacontext.Context, args map[string]any) (mcp.CallToolResult, error) {
	jobID, err := extractJobID(args)
	if err != nil {
		return mcp.CallToolResult{}, err
	}

	logger := s.logger.WithField("jobId", jobID).WithField("tool", "get_job_links")
	logger.Debug("getting job links")

	filters := []*model.Filter{
		{
			Field: "jobId",
			Match: model.MatchExact,
			Value: jobID,
		},
	}

	result, err := s.getJobsRepo.GetJobs(&ctx, filters, false, nil, 0, 1)
	if err != nil {
		logger.WithError(err).Error("failed to get job")
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to get job")
	}

	if len(result.Jobs) == 0 {
		logger.Warn("job not found")
		return mcp.CallToolResult{}, errors.Errorf("job with ID %s not found", jobID)
	}

	job := result.Jobs[0]

	links := s.renderLinks(job)

	jsonData, err := json.MarshalIndent(links, "", "  ")
	if err != nil {
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to marshal links to JSON")
	}

	return mcp.CallToolResult{
		Content: []mcp.Content{
			{
				Type: "text",
				Text: string(jsonData),
			},
		},
	}, nil
}

func (s *mcpServerState) handleGetLookoutUILink(args map[string]any) (mcp.CallToolResult, error) {
	jobID, err := extractJobID(args)
	if err != nil {
		return mcp.CallToolResult{}, err
	}

	logger := s.logger.WithField("jobId", jobID).WithField("tool", "get_lookout_ui_link")
	logger.Debug("getting lookout UI link")

	baseURL := strings.TrimSuffix(s.config.LookoutUIBaseUrl, "/")

	link := fmt.Sprintf("%s/jobs/%s", baseURL, jobID)

	type LinkResult struct {
		URL string `json:"url"`
	}

	linkResult := LinkResult{URL: link}

	jsonData, err := json.MarshalIndent(linkResult, "", "  ")
	if err != nil {
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to marshal link to JSON")
	}

	return mcp.CallToolResult{
		Content: []mcp.Content{
			{
				Type: "text",
				Text: string(jsonData),
			},
		},
	}, nil
}

func (s *mcpServerState) handleSearchJobs(ctx armadacontext.Context, args map[string]any) (mcp.CallToolResult, error) {
	logger := s.logger.WithField("tool", "search_jobs")
	logger.Debug("searching jobs")

	var filters []*model.Filter
	if filtersArg, ok := args["filters"].([]any); ok {
		filters = make([]*model.Filter, 0, len(filtersArg))
		for _, f := range filtersArg {
			filterMap, ok := f.(map[string]any)
			if !ok {
				continue
			}
			field, _ := filterMap["field"].(string)
			match, _ := filterMap["match"].(string)
			value := filterMap["value"]

			filters = append(filters, &model.Filter{
				Field: field,
				Match: match,
				Value: value,
			})
		}
	}

	skip := 0
	if s, ok := args["skip"].(float64); ok {
		skip = int(s)
	}

	take := 10
	if t, ok := args["take"].(float64); ok {
		take = int(t)
		if take > 100 {
			take = 100
		}
	}

	result, err := s.getJobsRepo.GetJobs(&ctx, filters, false, nil, skip, take)
	if err != nil {
		logger.WithError(err).Error("failed to search jobs")
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to search jobs")
	}

	jsonData, err := json.MarshalIndent(result.Jobs, "", "  ")
	if err != nil {
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to marshal jobs to JSON")
	}

	return mcp.CallToolResult{
		Content: []mcp.Content{
			{
				Type: "text",
				Text: fmt.Sprintf("Found %d jobs:\n\n%s", len(result.Jobs), string(jsonData)),
			},
		},
	}, nil
}

func (s *mcpServerState) handleGetJobError(ctx armadacontext.Context, args map[string]any) (mcp.CallToolResult, error) {
	jobID, err := extractJobID(args)
	if err != nil {
		return mcp.CallToolResult{}, err
	}

	logger := s.logger.WithField("jobId", jobID).WithField("tool", "get_job_error")
	logger.Debug("getting job error")

	errorMsg, err := s.getJobErrorRepo.GetJobErrorMessage(&ctx, jobID)
	if err != nil {
		logger.WithError(err).Error("failed to get job error")
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to get job error")
	}

	return mcp.CallToolResult{
		Content: []mcp.Content{
			{
				Type: "text",
				Text: errorMsg,
			},
		},
	}, nil
}

func validateRunID(runID string) error {
	if runID == "" {
		return errors.New("runId is required")
	}
	if len(runID) > 128 {
		return errors.New("runId must be at most 128 characters")
	}
	return nil
}

func extractRunID(args map[string]any) (string, error) {
	runID, ok := args["runId"].(string)
	if !ok {
		return "", errors.New("runId must be a string")
	}
	if err := validateRunID(runID); err != nil {
		return "", err
	}
	return runID, nil
}

func (s *mcpServerState) handleGetJobRunError(ctx armadacontext.Context, args map[string]any) (mcp.CallToolResult, error) {
	runID, err := extractRunID(args)
	if err != nil {
		return mcp.CallToolResult{}, err
	}

	logger := s.logger.WithField("runId", runID).WithField("tool", "get_job_run_error")
	logger.Debug("getting job run error")

	errorMsg, err := s.getJobRunErrorRepo.GetJobRunError(&ctx, runID)
	if err != nil {
		logger.WithError(err).Error("failed to get job run error")
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to get job run error")
	}

	return mcp.CallToolResult{
		Content: []mcp.Content{
			{
				Type: "text",
				Text: errorMsg,
			},
		},
	}, nil
}

func (s *mcpServerState) handleGetJobRunDebug(ctx armadacontext.Context, args map[string]any) (mcp.CallToolResult, error) {
	runID, err := extractRunID(args)
	if err != nil {
		return mcp.CallToolResult{}, err
	}

	logger := s.logger.WithField("runId", runID).WithField("tool", "get_job_run_debug")
	logger.Debug("getting job run debug info")

	debugMsg, err := s.getJobRunDebugMsgRepo.GetJobRunDebugMessage(&ctx, runID)
	if err != nil {
		logger.WithError(err).Error("failed to get job run debug message")
		return mcp.CallToolResult{}, errors.Wrap(err, "failed to get job run debug message")
	}

	return mcp.CallToolResult{
		Content: []mcp.Content{
			{
				Type: "text",
				Text: debugMsg,
			},
		},
	}, nil
}

func (s *mcpServerState) renderCommands(job *model.Job) []map[string]any {
	commands := make([]map[string]any, 0, len(s.config.UIConfig.CommandSpecs))

	for _, spec := range s.config.UIConfig.CommandSpecs {
		rendered := s.renderTemplate(spec.Template, job)

		cmd := map[string]any{
			"name":     spec.Name,
			"command":  rendered,
			"template": spec.Template,
		}

		if spec.DescriptionMd != nil {
			cmd["description"] = *spec.DescriptionMd
		}

		if spec.AlertMessageMd != nil {
			cmd["alertMessage"] = *spec.AlertMessageMd
			cmd["alertLevel"] = string(spec.AlertLevel)
		}

		commands = append(commands, cmd)
	}

	return commands
}

func (s *mcpServerState) renderLinks(job *model.Job) []map[string]any {
	links := make([]map[string]any, 0, len(s.config.UIConfig.JobLinks))

	for _, linkConfig := range s.config.UIConfig.JobLinks {
		rendered := s.renderTemplate(linkConfig.LinkTemplate, job)

		link := map[string]any{
			"label": linkConfig.Label,
			"url":   rendered,
			"color": linkConfig.Colour,
		}

		links = append(links, link)
	}

	return links
}

func (s *mcpServerState) renderTemplate(templateStr string, job *model.Job) string {
	result := templateStr

	replacements := map[string]string{
		"{{ jobId }}":  job.JobId,
		"{{jobId}}":    job.JobId,
		"{{ queue }}":  job.Queue,
		"{{queue}}":    job.Queue,
		"{{ jobSet }}": job.JobSet,
		"{{jobSet}}":   job.JobSet,
		"{{ owner }}":  job.Owner,
		"{{owner}}":    job.Owner,
	}

	if job.Namespace != nil {
		replacements["{{ namespace }}"] = *job.Namespace
		replacements["{{namespace}}"] = *job.Namespace
	}

	if len(job.Runs) > 0 {
		lastRun := job.Runs[len(job.Runs)-1]
		replacements["{{ runs[runs.length - 1].cluster }}"] = lastRun.Cluster
		replacements["{{runs[runs.length - 1].cluster}}"] = lastRun.Cluster
		replacements["{{ runs[runs.length - 1].runId }}"] = lastRun.RunId
		replacements["{{runs[runs.length - 1].runId}}"] = lastRun.RunId

		if lastRun.Node != nil {
			replacements["{{ runs[runs.length - 1].node }}"] = *lastRun.Node
			replacements["{{runs[runs.length - 1].node}}"] = *lastRun.Node
		}
	}

	for placeholder, value := range replacements {
		escapedValue := html.EscapeString(value)
		result = strings.ReplaceAll(result, placeholder, escapedValue)
	}

	for key, value := range job.Annotations {
		placeholder := fmt.Sprintf(`{{ annotations["%s"] }}`, key)
		escapedValue := html.EscapeString(value)
		result = strings.ReplaceAll(result, placeholder, escapedValue)

		placeholder = fmt.Sprintf(`{{annotations["%s"]}}`, key)
		result = strings.ReplaceAll(result, placeholder, escapedValue)
	}

	return result
}

func (s *mcpServerState) sendMCPResult(w http.ResponseWriter, id any, result any) error {
	response := mcp.Response{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	}

	return s.sendMCPResponse(w, response)
}

func (s *mcpServerState) sendMCPError(w http.ResponseWriter, id any, code int, message string, data any) error {
	response := mcp.Response{
		JSONRPC: "2.0",
		ID:      id,
		Error: &mcp.Error{
			Code:    code,
			Message: message,
			Data:    data,
		},
	}

	return s.sendMCPResponse(w, response)
}

func (s *mcpServerState) sendMCPResponse(w http.ResponseWriter, response mcp.Response) error {
	data, err := json.Marshal(response)
	if err != nil {
		return errors.Wrap(err, "failed to marshal response")
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(append(data, '\n')); err != nil {
		return errors.Wrap(err, "failed to write response")
	}

	return nil
}
