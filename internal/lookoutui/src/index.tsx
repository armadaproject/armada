import { createRoot } from "react-dom/client"

import { App } from "./App"
import { SubmitApi, Configuration as SubmitConfiguration } from "./openapi/armada"
import { GetJobInfoService } from "./services/lookout/GetJobInfoService"
import { GetJobsService } from "./services/lookout/GetJobsService"
import { GroupJobsService } from "./services/lookout/GroupJobsService"
import { UpdateJobSetsService } from "./services/lookout/UpdateJobSetsService"
import { UpdateJobsService } from "./services/lookout/UpdateJobsService"
import FakeGetJobInfoService from "./services/lookout/mocks/FakeGetJobInfoService"
import FakeGetJobsService from "./services/lookout/mocks/FakeGetJobsService"
import FakeGroupJobsService from "./services/lookout/mocks/FakeGroupJobsService"
import { getUIConfig } from "./utils"
import { makeRandomJobs } from "./utils/fakeJobsUtils"

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
  const v2JobSpecService = fakeDataEnabled ? new FakeGetJobInfoService() : new GetJobInfoService()
  const v2UpdateJobsService = new UpdateJobsService(submitApi)
  const v2UpdateJobSetsService = new UpdateJobSetsService(submitApi)

  const container = document.getElementById("root")

  if (container === null) {
    throw new Error('DOM element with ID "root" was not found')
  }

  createRoot(container).render(
    <App
      customTitle={uiConfig.customTitle}
      oidcConfig={uiConfig.oidcEnabled ? uiConfig.oidc : undefined}
      services={{
        v2GetJobsService,
        v2GroupJobsService,
        v2UpdateJobsService,
        v2UpdateJobSetsService,
        v2JobSpecService,
      }}
      jobSetsAutoRefreshMs={uiConfig.jobSetsAutoRefreshMs}
      jobsAutoRefreshMs={uiConfig.jobsAutoRefreshMs}
      debugEnabled={uiConfig.debugEnabled}
      commandSpecs={uiConfig.commandSpecs}
    />,
  )
})()
