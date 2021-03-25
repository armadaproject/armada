import React from 'react'

import { Job } from "../../services/JobService"
import { Dialog, DialogTitle } from "@material-ui/core"
import JobDetails from "./JobDetails"

import "./JobDetailsModal.css"

export interface JobDetailsModalContext {
  open: boolean
  job?: Job
  expandedItems: Set<string>
}

interface JobDetailsModalProps extends JobDetailsModalContext {
  onToggleExpanded: (k8sId: string, isExpanded: boolean) => void
  onClose: () => void
}

export function toggleExpanded(item: string, isExpanded: boolean, expandedItems: Set<string>): Set<string> {
  const newExpanded = new Set(expandedItems)
  if (isExpanded) {
    newExpanded.add(item)
  } else {
    newExpanded.delete(item)
  }
  return newExpanded
}

export default function JobDetailsModal(props: JobDetailsModalProps) {
  return (
    <Dialog
      open={props.open}
      aria-labelledby="job-details-dialog-title"
      onClose={props.onClose}
      fullWidth={true}
      maxWidth={"md"}>
      <DialogTitle id="job-details-dialog-title">Job details</DialogTitle>
      <div className="dialog-body">
      {props.job && <JobDetails
        job={props.job}
        expandedItems={props.expandedItems}
        onToggleExpand={props.onToggleExpanded}/>}
      </div>
    </Dialog>
  )
}
