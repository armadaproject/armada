import React from "react"

import ReactDOM from "react-dom"

import { App } from "./App"
import { SubmitApi, Configuration as SubmitConfiguration } from "./openapi/armada"
import { LookoutApi, Configuration as LookoutConfiguration } from "./openapi/lookout"
import reportWebVitals from "./reportWebVitals"
import { JobService, LookoutJobService } from "./services/JobService"
import { MockJobService } from "./services/JobService.test"
import LogService from "./services/LogService"
import { getUIConfig } from "./utils"

import "react-virtualized/styles.css"
import "./index.css"
;(async () => {
  const uiConfig = await getUIConfig()

  let jobService: JobService
  if (false) {
    jobService = new LookoutJobService(
      new LookoutApi(new LookoutConfiguration({ basePath: "" })),
      new SubmitApi(
        new SubmitConfiguration({
          basePath: uiConfig.armadaApiBaseUrl,
          credentials: "include",
        }),
      ),
      uiConfig.userAnnotationPrefix,
    )
  } else {
    jobService = new MockJobService({
      getJobs: {
        delays: {
          test: 5000,
          testt: 100,
        },
      },
    })
  }

  const logService = new LogService(
    { credentials: "include" },
    uiConfig.binocularsBaseUrlPattern,
    uiConfig.binocularsEnabled,
  )

  ReactDOM.render(
    <App
      jobService={jobService}
      logService={logService}
      overviewAutoRefreshMs={uiConfig.overviewAutoRefreshMs}
      jobSetsAutoRefreshMs={uiConfig.jobSetsAutoRefreshMs}
      jobsAutoRefreshMs={uiConfig.jobsAutoRefreshMs}
    />,
    document.getElementById("root"),
  )

  reportWebVitals()
})()
