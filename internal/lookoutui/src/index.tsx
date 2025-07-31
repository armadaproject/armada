import { createRoot } from "react-dom/client"
import { ErrorBoundary } from "react-error-boundary"

import { App } from "./App"
import { FullPageErrorFallback } from "./components/FullPageErrorFallback"
import { getConfig } from "./config"
import { reactErrorHandlers } from "./errorMonitoring"
import { SubmitApi, Configuration as SubmitConfiguration } from "./openapi/armada"
import { GetJobInfoService } from "./services/lookout/GetJobInfoService"
import { GetJobsService } from "./services/lookout/GetJobsService"
import { GroupJobsService } from "./services/lookout/GroupJobsService"
import { UpdateJobSetsService } from "./services/lookout/UpdateJobSetsService"
import { UpdateJobsService } from "./services/lookout/UpdateJobsService"
import FakeGetJobInfoService from "./services/lookout/mocks/FakeGetJobInfoService"
import FakeGetJobsService from "./services/lookout/mocks/FakeGetJobsService"
import FakeGroupJobsService from "./services/lookout/mocks/FakeGroupJobsService"
import { makeRandomJobs } from "./utils/fakeJobsUtils"

import "./index.css"

const NodeToRender = () => {
  const config = getConfig()

  const submitApi = new SubmitApi(
    new SubmitConfiguration({
      basePath: config.armadaApiBaseUrl,
      credentials: "include",
    }),
  )

  const fakeDataEnabled = config.fakeDataEnabled

  const v2TestJobs = fakeDataEnabled ? makeRandomJobs(10000, 42) : []
  const v2GetJobsService = fakeDataEnabled ? new FakeGetJobsService(v2TestJobs) : new GetJobsService(config.backend)
  const v2GroupJobsService = fakeDataEnabled
    ? new FakeGroupJobsService(v2TestJobs)
    : new GroupJobsService(config.backend)
  const v2JobSpecService = fakeDataEnabled ? new FakeGetJobInfoService() : new GetJobInfoService()
  const v2UpdateJobsService = new UpdateJobsService(submitApi)
  const v2UpdateJobSetsService = new UpdateJobSetsService(submitApi)

  return (
    <App
      customTitle={config.customTitle}
      oidcConfig={config.oidcEnabled ? config.oidc : undefined}
      services={{
        v2GetJobsService,
        v2GroupJobsService,
        v2UpdateJobsService,
        v2UpdateJobSetsService,
        v2JobSpecService,
      }}
      jobSetsAutoRefreshMs={config.jobSetsAutoRefreshMs}
      jobsAutoRefreshMs={config.jobsAutoRefreshMs}
      debugEnabled={config.debugEnabled}
      commandSpecs={config.commandSpecs}
    />
  )
}

const container = document.getElementById("root")

if (container === null) {
  throw new Error('DOM element with ID "root" was not found')
}

createRoot(container, { ...reactErrorHandlers }).render(
  <ErrorBoundary FallbackComponent={FullPageErrorFallback}>
    <NodeToRender />
  </ErrorBoundary>,
)
