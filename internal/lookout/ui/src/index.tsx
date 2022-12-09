import ReactDOM from "react-dom"
import { CancelJobsService } from "services/lookoutV2/CancelJobsService"
import { GetJobsService } from "services/lookoutV2/GetJobsService"
import { GroupJobsService } from "services/lookoutV2/GroupJobsService"
import FakeGetJobsService from "services/lookoutV2/mocks/FakeGetJobsService"
import FakeGroupJobsService from "services/lookoutV2/mocks/FakeGroupJobsService"
import { makeTestJobs } from "utils/fakeJobsUtils"

import { App } from "./App"
import { SubmitApi, Configuration as SubmitConfiguration } from "./openapi/armada"
import { LookoutApi, Configuration as LookoutConfiguration } from "./openapi/lookout"
import reportWebVitals from "./reportWebVitals"
import { LookoutJobService } from "./services/JobService"
import LogService from "./services/LogService"
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

  const jobService = new LookoutJobService(
    new LookoutApi(new LookoutConfiguration({ basePath: "" })),
    submitApi,
    uiConfig.userAnnotationPrefix,
  )

  const logService = new LogService(
    { credentials: "include" },
    uiConfig.binocularsBaseUrlPattern,
    uiConfig.binocularsEnabled,
  )

  const fakeDataEnabled = uiConfig.fakeDataEnabled
  const lookoutV2BaseUrl = "http://localhost:10000/api/v1"

  const v2TestJobs = fakeDataEnabled ? makeTestJobs(10000, 42) : []
  const v2GetJobsService = fakeDataEnabled ? new FakeGetJobsService(v2TestJobs) : new GetJobsService(lookoutV2BaseUrl)
  const v2GroupJobsService = fakeDataEnabled
    ? new FakeGroupJobsService(v2TestJobs)
    : new GroupJobsService(lookoutV2BaseUrl)
  const v2CancelJobsService = new CancelJobsService(submitApi)

  ReactDOM.render(
    <App
      jobService={jobService}
      v2GetJobsService={v2GetJobsService}
      v2GroupJobsService={v2GroupJobsService}
      v2CancelJobsService={v2CancelJobsService}
      logService={logService}
      overviewAutoRefreshMs={uiConfig.overviewAutoRefreshMs}
      jobSetsAutoRefreshMs={uiConfig.jobSetsAutoRefreshMs}
      jobsAutoRefreshMs={uiConfig.jobsAutoRefreshMs}
      debugEnabled={uiConfig.debugEnabled}
    />,
    document.getElementById("root"),
  )

  reportWebVitals()
})()
