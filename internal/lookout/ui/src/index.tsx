import ReactDOM from "react-dom"
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
import { GetJobSpecService } from "./services/lookoutV2/GetJobSpecService"
import { GetRunErrorService } from "./services/lookoutV2/GetRunErrorService"
import { LogService as V2LogService } from "./services/lookoutV2/LogService"
import { FakeCordonService } from "./services/lookoutV2/mocks/FakeCordonService"
import FakeGetJobSpecService from "./services/lookoutV2/mocks/FakeGetJobSpecService"
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

  const fakeDataEnabled = uiConfig.fakeDataEnabled

  const v2TestJobs = fakeDataEnabled ? makeRandomJobs(10000, 42) : []
  const v2GetJobsService = fakeDataEnabled ? new FakeGetJobsService(v2TestJobs) : new GetJobsService()
  const v2GroupJobsService = fakeDataEnabled ? new FakeGroupJobsService(v2TestJobs) : new GroupJobsService()
  const v2RunErrorService = fakeDataEnabled ? new FakeGetRunErrorService() : new GetRunErrorService()
  const v2LogService = fakeDataEnabled
    ? new FakeLogService()
    : new V2LogService({ credentials: "include" }, uiConfig.binocularsBaseUrlPattern)
  const v2JobSpecService = fakeDataEnabled ? new FakeGetJobSpecService() : new GetJobSpecService()
  const v2UpdateJobsService = new UpdateJobsService(submitApi)
  const v2UpdateJobSetsService = new UpdateJobSetsService(submitApi)
  const v2CordonService = fakeDataEnabled
    ? new FakeCordonService()
    : new CordonService({ credentials: "include" }, uiConfig.binocularsBaseUrlPattern)

  ReactDOM.render(
    <App
      customTitle={uiConfig.customTitle}
      oidcConfig={uiConfig.oidcEnabled ? uiConfig.oidc : undefined}
      v2GetJobsService={v2GetJobsService}
      v2GroupJobsService={v2GroupJobsService}
      v2UpdateJobsService={v2UpdateJobsService}
      v2UpdateJobSetsService={v2UpdateJobSetsService}
      v2RunErrorService={v2RunErrorService}
      v2JobSpecService={v2JobSpecService}
      v2LogService={v2LogService}
      v2CordonService={v2CordonService}
      jobSetsAutoRefreshMs={uiConfig.jobSetsAutoRefreshMs}
      debugEnabled={uiConfig.debugEnabled}
    />,
    document.getElementById("root"),
  )

  reportWebVitals()
})()
