import React from "react"

import ReactDOM from "react-dom"

import { App } from "./App"
import { SubmitApi, Configuration as SubmitConfiguration } from "./openapi/armada"
import { LookoutApi, Configuration as LookoutConfiguration } from "./openapi/lookout"
import reportWebVitals from "./reportWebVitals"
import JobService from "./services/JobService"
import { getUIConfig } from "./utils"

import "react-virtualized/styles.css"
import "./index.css"
;(async () => {
  const uiConfig = await getUIConfig()

  const jobService = new JobService(
    new LookoutApi(new LookoutConfiguration({ basePath: "" })),
    new SubmitApi(
      new SubmitConfiguration({
        basePath: uiConfig.armadaApiBaseUrl,
        credentials: "include",
      }),
    ),
    uiConfig.userAnnotationPrefix,
  )

  ReactDOM.render(<App jobService={jobService} />, document.getElementById("root"))

  reportWebVitals()
})()
