import React from 'react';
import ReactDOM from 'react-dom';

import { App } from './App';
import reportWebVitals from './reportWebVitals';
import JobService from './services/JobService';
import { Configuration as LookoutConfiguration, LookoutApi } from './openapi/lookout';
import { Configuration as SubmitConfiguration, SubmitApi } from "./openapi/armada";
import { getUIConfig } from "./utils";

import 'react-virtualized/styles.css'
import './index.css';

(async () => {
  const uiConfig = await getUIConfig()

  const jobService = new JobService(
    new LookoutApi(new LookoutConfiguration({ basePath: "" })),
    new SubmitApi(new SubmitConfiguration({
      basePath: uiConfig.armadaApiBaseUrl,
      credentials: "include",
    })),
    uiConfig.userAnnotationPrefix,
  );

  ReactDOM.render(
    <App jobService={jobService}/>,
    document.getElementById('root')
  );

  reportWebVitals();
})()
