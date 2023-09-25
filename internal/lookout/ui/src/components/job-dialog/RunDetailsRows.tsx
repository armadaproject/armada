import React from "react"

import DetailRow from "./DetailRow"
import { Run } from "../../services/JobService"

import "./Details.css"

interface RunDetailsRowsProps {
  run: Run
  jobId?: string
}

export default function RunDetailsRows(props: RunDetailsRowsProps) {
  const armadaPodName = props.jobId ? `armada-${props.jobId}-${props.run.podNumber.toString()}` : null
  return (
    <>
      <DetailRow name="Cluster" value={props.run.cluster} />
      <DetailRow name="Pod number" value={props.run.podNumber.toString()} />
      {armadaPodName && <DetailRow name="Pod Name" value={armadaPodName} />}
      <DetailRow name="Kubernetes Id" value={props.run.k8sId} />
      {props.run.node && <DetailRow name="Cluster node" value={props.run.node} />}
      {props.run.podCreationTime && <DetailRow name="Scheduled on cluster" value={props.run.podCreationTime} />}
      {props.run.podStartTime && <DetailRow name="Job started" value={props.run.podStartTime} />}
      {props.run.finishTime && <DetailRow name="Finished" value={props.run.finishTime} />}
      {props.run.preemptedTime && <DetailRow name="Preempted" value={props.run.preemptedTime} />}
      {props.run.error && <DetailRow name="Error" value={props.run.error} className="error-message" />}
    </>
  )
}
