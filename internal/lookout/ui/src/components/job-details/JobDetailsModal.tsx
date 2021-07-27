import React, { useState, useEffect } from "react"

import { Dialog, Tabs, Tab } from "@material-ui/core"

import { Job } from "../../services/JobService"
import LogService from "../../services/LogService"
import JobDetails from "./JobDetails"
import JobLogs from "./JobLogs"
import "./JobDetailsModal.css"

export interface JobDetailsModalContext {
  open: boolean
  job?: Job
  expandedItems: Set<string>
}

interface JobDetailsModalProps extends JobDetailsModalContext {
  logService: LogService
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
  const [tab, changeTab] = useState<"detail" | "logs">("detail")

  // reset tab on job change
  useEffect(() => {
    changeTab("detail")
  }, [props.job])

  return (
    <Dialog
      open={props.open}
      aria-labelledby="job-details-dialog-title"
      onClose={props.onClose}
      fullWidth={true}
      maxWidth={"md"}
    >
      <Tabs
        value={tab}
        onChange={(_, t) => {
          changeTab(t)
        }}
      >
        <Tab label="Job details" value="detail" />
        {props.logService.isEnabled && props.job?.runs && props.job?.runs.length > 0 && (
          <Tab label="Logs" value="logs" />
        )}
      </Tabs>
      <div className="dialog-body">
        {tab == "detail" && props.job && (
          <JobDetails job={props.job} expandedItems={props.expandedItems} onToggleExpand={props.onToggleExpanded} />
        )}
        {tab == "logs" && props.job && <JobLogs job={props.job} logService={props.logService} />}
      </div>
    </Dialog>
  )
}
