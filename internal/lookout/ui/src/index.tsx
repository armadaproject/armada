import React from 'react';
import ReactDOM from 'react-dom';

import { App } from './App';
import reportWebVitals from './reportWebVitals';
import JobService from './services/JobService';
import { LookoutApi, Configuration } from './openapi';

import 'react-virtualized/styles.css'
import './index.css';

let jobService = new JobService(new LookoutApi(new Configuration({basePath: ""})))

ReactDOM.render(
  <React.StrictMode>
    <App jobService={jobService} />
  </React.StrictMode>,
  document.getElementById('root')
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
