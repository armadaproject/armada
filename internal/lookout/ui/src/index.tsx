import ReactDOM from "react-dom"
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

  const jobService = new LookoutJobService(
    new LookoutApi(new LookoutConfiguration({ basePath: "" })),
    new SubmitApi(
      new SubmitConfiguration({
        basePath: uiConfig.armadaApiBaseUrl,
        credentials: "include",
      }),
    ),
    uiConfig.userAnnotationPrefix,
  )

  const logService = new LogService(
    { credentials: "include" },
    uiConfig.binocularsBaseUrlPattern,
    uiConfig.binocularsEnabled,
  )

  const v2TestJobs = makeTestJobs(10000, 42)
  const v2GetJobsService = new FakeGetJobsService(v2TestJobs)
  const v2GroupJobsService = new FakeGroupJobsService(v2TestJobs)

  ReactDOM.render(
    <App
      jobService={jobService}
      v2GetJobsService={v2GetJobsService}
      v2GroupJobsService={v2GroupJobsService}
      logService={logService}
      overviewAutoRefreshMs={uiConfig.overviewAutoRefreshMs}
      jobSetsAutoRefreshMs={uiConfig.jobSetsAutoRefreshMs}
      jobsAutoRefreshMs={uiConfig.jobsAutoRefreshMs}
    />,
    document.getElementById("root"),
  )

  reportWebVitals()
})()
