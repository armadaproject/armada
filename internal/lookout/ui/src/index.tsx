import { createRoot } from "react-dom/client"
import { GetJobsService } from "services/lookoutV2/GetJobsService"
import { GroupJobsService } from "services/lookoutV2/GroupJobsService"
import { UpdateJobSetsService } from "services/lookoutV2/UpdateJobSetsService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import FakeGetJobsService from "services/lookoutV2/mocks/FakeGetJobsService"
import FakeGroupJobsService from "services/lookoutV2/mocks/FakeGroupJobsService"
import { makeRandomJobs } from "utils/fakeJobsUtils"

import { App } from "./App"
import { SubmitApi, Configuration as SubmitConfiguration } from "./openapi/armada"
import reportWebVitals from "./reportWebVitals"
import { CordonService } from "./services/lookoutV2/CordonService"
import { GetJobInfoService } from "./services/lookoutV2/GetJobInfoService"
import { GetRunInfoService } from "./services/lookoutV2/GetRunInfoService"
import { LogService as V2LogService } from "./services/lookoutV2/LogService"
import { FakeCordonService } from "./services/lookoutV2/mocks/FakeCordonService"
import FakeGetJobInfoService from "./services/lookoutV2/mocks/FakeGetJobInfoService"
import { FakeGetRunInfoService } from "./services/lookoutV2/mocks/FakeGetRunInfoService"
import { FakeLogService } from "./services/lookoutV2/mocks/FakeLogService"
import { getUIConfig } from "./utils"

import "react-virtualized/styles.css"
import "./index.css"
;(async () => {
  const uiConfig = await getUIConfig()

  const submitApi = new SubmitApi(
    new SubmitConfiguration({
      basePath: uiConfig.armadaApiBaseUrl,
      credentials: "include",
    }),
  )

  const fakeDataEnabled = uiConfig.fakeDataEnabled

  const v2TestJobs = fakeDataEnabled ? makeRandomJobs(10000, 42) : []
  const v2GetJobsService = fakeDataEnabled ? new FakeGetJobsService(v2TestJobs) : new GetJobsService(uiConfig.backend)
  const v2GroupJobsService = fakeDataEnabled
    ? new FakeGroupJobsService(v2TestJobs)
    : new GroupJobsService(uiConfig.backend)
  const v2RunInfoService = fakeDataEnabled ? new FakeGetRunInfoService() : new GetRunInfoService()
  const v2LogService = fakeDataEnabled
    ? new FakeLogService()
    : new V2LogService({ credentials: "include" }, uiConfig.binocularsBaseUrlPattern)
  const v2JobSpecService = fakeDataEnabled ? new FakeGetJobInfoService() : new GetJobInfoService()
  const v2UpdateJobsService = new UpdateJobsService(submitApi)
  const v2UpdateJobSetsService = new UpdateJobSetsService(submitApi)
  const v2CordonService = fakeDataEnabled
    ? new FakeCordonService()
    : new CordonService({ credentials: "include" }, uiConfig.binocularsBaseUrlPattern)

  const container = document.getElementById("root")

  if (container === null) {
    throw new Error('DOM element with ID "root" was not found')
  }

  createRoot(container).render(
    <App
      customTitle={uiConfig.customTitle}
      oidcConfig={uiConfig.oidcEnabled ? uiConfig.oidc : undefined}
      v2GetJobsService={v2GetJobsService}
      v2GroupJobsService={v2GroupJobsService}
      v2UpdateJobsService={v2UpdateJobsService}
      v2UpdateJobSetsService={v2UpdateJobSetsService}
      v2RunInfoService={v2RunInfoService}
      v2JobSpecService={v2JobSpecService}
      v2LogService={v2LogService}
      v2CordonService={v2CordonService}
      jobSetsAutoRefreshMs={uiConfig.jobSetsAutoRefreshMs}
      jobsAutoRefreshMs={uiConfig.jobsAutoRefreshMs}
      debugEnabled={uiConfig.debugEnabled}
      commandSpecs={uiConfig.commandSpecs}
    />,
  )

  reportWebVitals()
})()
