import { createBrowserHistory } from "history"
import ReactDOM from "react-dom"
import { GetJobsService } from "services/lookoutV2/GetJobsService"
import { GroupJobsService } from "services/lookoutV2/GroupJobsService"
import { JobsTablePreferencesService } from "services/lookoutV2/JobsTablePreferencesService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import FakeGetJobsService from "services/lookoutV2/mocks/FakeGetJobsService"
import FakeGroupJobsService from "services/lookoutV2/mocks/FakeGroupJobsService"
import { makeRandomJobs } from "utils/fakeJobsUtils"

import { App } from "./App"
import { SubmitApi, Configuration as SubmitConfiguration } from "./openapi/armada"
import { LookoutApi, Configuration as LookoutConfiguration } from "./openapi/lookout"
import reportWebVitals from "./reportWebVitals"
import { LookoutJobService } from "./services/JobService"
import LogService from "./services/LogService"
import { GetRunErrorService } from "./services/lookoutV2/GetRunErrorService"
import { LogService as V2LogService } from "./services/lookoutV2/LogService"
import { FakeGetRunErrorService } from "./services/lookoutV2/mocks/FakeGetRunErrorService"
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
  const lookoutV2BaseUrl = uiConfig.lookoutV2ApiBaseUrl

  const v2JobsTablePrefsService = new JobsTablePreferencesService(createBrowserHistory())
  const v2TestJobs = fakeDataEnabled ? makeRandomJobs(10000, 42) : []
  const v2GetJobsService = fakeDataEnabled ? new FakeGetJobsService(v2TestJobs) : new GetJobsService(lookoutV2BaseUrl)
  const v2GroupJobsService = fakeDataEnabled
    ? new FakeGroupJobsService(v2TestJobs)
    : new GroupJobsService(lookoutV2BaseUrl)
  const v2RunErrorService = fakeDataEnabled ? new FakeGetRunErrorService() : new GetRunErrorService(lookoutV2BaseUrl)
  const v2LogService = fakeDataEnabled
    ? new FakeLogService()
    : new V2LogService({ credentials: "include" }, uiConfig.binocularsBaseUrlPattern)
  const v2UpdateJobsService = new UpdateJobsService(submitApi)

  ReactDOM.render(
    <App
      jobService={jobService}
      v2JobsTablePrefsService={v2JobsTablePrefsService}
      v2GetJobsService={v2GetJobsService}
      v2GroupJobsService={v2GroupJobsService}
      v2UpdateJobsService={v2UpdateJobsService}
      v2RunErrorService={v2RunErrorService}
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
