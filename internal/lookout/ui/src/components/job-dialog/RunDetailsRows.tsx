import React, { Fragment } from "react"

import { Run } from "../../services/JobService"
import DetailRow from "./DetailRow"

import "./Details.css"

interface RunDetailsRowsProps {
  run: Run
}

export function RunDetailsRows(props: RunDetailsRowsProps) {
  return (
    <Fragment>
      <DetailRow name="Cluster" value={props.run.cluster} />
      <DetailRow name="Pod number" value={props.run.podNumber.toString()} />
      <DetailRow name="Kubernetes Id" value={props.run.k8sId} />
      {props.run.node && <DetailRow name="Cluster node" value={props.run.node} />}
      {props.run.podCreationTime && <DetailRow name="Scheduled on cluster" value={props.run.podCreationTime} />}
      {props.run.podStartTime && <DetailRow name="Job started" value={props.run.podStartTime} />}
      {props.run.finishTime && <DetailRow name="Finished" value={props.run.finishTime} />}
      {props.run.error && <DetailRow name="Error" value={props.run.error} className="error-message" />}
    </Fragment>
  )
}
