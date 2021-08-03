import React, { Fragment } from "react"

import { Run } from "../../services/JobService"
import "./Details.css"
import { MakeJobDetailsRow, MakeJobDetailsRowDetailed } from "./JobDetailsUtils"

interface RunDetailsRowsProps {
  run: Run
}

export function RunDetailsRows(props: RunDetailsRowsProps) {
  return (
    <Fragment>
      {MakeJobDetailsRow("Cluster", props.run.cluster)}
      {MakeJobDetailsRow("Pod number", props.run.podNumber?.toString())}
      {MakeJobDetailsRow("Kubernetes Id", props.run.k8sId?.toString())}

      {props.run.node && MakeJobDetailsRow("Cluster node", props.run.node)}

      {props.run.podCreationTime && MakeJobDetailsRow("Scheduled on cluster", props.run.podCreationTime)}
      {props.run.podStartTime && MakeJobDetailsRow("Job started", props.run.podStartTime)}
      {props.run.finishTime && MakeJobDetailsRow("Finished", props.run.finishTime)}
      {props.run.error && MakeJobDetailsRowDetailed("Error", "Error", props.run.error, "error-message")}
    </Fragment>
  )
}
