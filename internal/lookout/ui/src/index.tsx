import React from 'react';
import ReactDOM from 'react-dom';

import { App } from './App';
import reportWebVitals from './reportWebVitals';
import JobService from './services/JobService';
import { LookoutApi, Configuration  as LookoutConfiguration } from './openapi/lookout';
import { SubmitApi, Configuration as SubmitConfiguration } from "./openapi/armada";

import 'react-virtualized/styles.css'
import './index.css';

let jobService = new JobService(
  new LookoutApi(new LookoutConfiguration({ basePath: "" })),
  new SubmitApi(new SubmitConfiguration({ basePath: process.env.REACT_APP_ARMADA_API_BASE_URL }))
)

ReactDOM.render(
  <App jobService={jobService} />,
  document.getElementById('root')
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
