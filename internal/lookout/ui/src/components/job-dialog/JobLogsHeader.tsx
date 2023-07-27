import React from "react"
import "./JobLogsHeader.css"

type JobLogsHeaderProps = {
  header: string
  headerValue: string | number | undefined
}

export default function JobLogsHeader(props: JobLogsHeaderProps) {
  return (
    <div className="header-job-log">
      <h4>
        {props?.header}
        <span className="header-job-log-divider"> : </span>
      </h4>
      <div>{props?.headerValue} </div>
    </div>
  )
}
