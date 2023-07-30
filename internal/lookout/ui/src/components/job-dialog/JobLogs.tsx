import React, { useEffect } from "react"

import { Button } from "@material-ui/core"
import Alert from "@material-ui/lab/Alert"
import OpenInNewTwoToneIcon from "@mui/icons-material/OpenInNewTwoTone"
import { useDispatch } from "react-redux"
import { Link } from "react-router-dom"
import { setJobLog } from "store/features/jobLogSlice"

import { getContainersForRun } from "../../containers/JobLogsContainer"
import { Job } from "../../services/JobService"
import { LogLine } from "../../services/LogService"
import JobLogsHeader from "./JobLogsHeader"
import JobLogsLoadMoreBtn from "./JobLogsLoadMoreBtn"

import "./JobLogs.css"
import "../Dialog.css"

type JobLogsProps = {
  job: Job
  loadFromStart: boolean
  runIndex: number
  container: string
  log: LogLine[]
  error: string | undefined
  onLoadFromStartChange: (loadFromStart: boolean) => void
  onRunIndexChange: (runIndex: number) => void
  onContainerChange: (container: string) => void
  onLoadMoreClick: () => void
}

export default function JobLogs(props: JobLogsProps) {
  const dispatch = useDispatch()

  const setJobLogState = () => {
    dispatch(setJobLog(props?.log))
  }

  useEffect(() => setJobLogState(), [props?.log])

  return (
    <div className="lookout-dialog-container">
      <div className="lookout-dialog-fixed job-logs-options">
        <div className="job-logs-option">
          {props.job.runs?.map((run, i) => (
            <section key={i} className="job-logs-option-group">
              <JobLogsHeader header="Cluster" headerValue={run?.cluster} />
              <JobLogsHeader header="Pod number" headerValue={run?.podNumber} />
              <JobLogsHeader header="Start time" headerValue={run?.podStartTime} />
            </section>
          ))}
          {getContainersForRun(props.job, props.runIndex).map((container) => (
            <span key={container}>
              <JobLogsHeader header="Command" headerValue={container} />
            </span>
          ))}
        </div>
        <JobLogsLoadMoreBtn text="Load from start" func={() => ({})} />
      </div>
      {!props.error && (
        <>
          <p className="lookout-dialog-varying job-logs">
            {props.log.map((l) => (
              <p key={l?.timestamp}>{l?.line} </p>
            ))}
          </p>
          <div className="lookout-dialog-centered lookout-dialog-fixed">
            <Button onClick={props.onLoadMoreClick}>Load more</Button>
            <Link to={`/job/${props?.job?.jobId}`} target="_blank" className="lookout-dialog-centered-link">
              <OpenInNewTwoToneIcon style={{ color: "#00aae1", fontSize: "2em" }} />
            </Link>
          </div>
        </>
      )}
      {props.error && (
        <Alert className="lookout-dialog-centered lookout-dialog-fixed" severity="error">
          {props.error}
        </Alert>
      )}
    </div>
  )
}
