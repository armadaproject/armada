import React, { useState } from "react"

import { Dialog, Tabs, Tab, DialogContent } from "@material-ui/core"

import JobDetails from "../components/job-dialog/JobDetails"
import { Job } from "../services/JobService"
import LogService from "../services/LogService"
import JobLogsContainer from "./JobLogsContainer"

type JobDetailsModalProps = {
  isOpen: boolean
  job?: Job
  logService: LogService
  onClose: () => void
}

type JobDialogTab = "Details" | "Logs"

const JOB_DIALOG_TABS = ["Details", "Logs"]
const LOGGABLE_JOB_STATES = ["Running", "Succeeded", "Failed", "Cancelled"]

export default function JobDialog(props: JobDetailsModalProps) {
  const [tab, changeTab] = useState<JobDialogTab>("Details")

  const showLogs = props.logService.isEnabled && props.job && LOGGABLE_JOB_STATES.includes(props.job?.jobState)

  return (
    <Dialog
      open={props.isOpen}
      aria-labelledby="job-details-dialog-title"
      onClose={props.onClose}
      fullWidth={true}
      maxWidth={"md"}
      TransitionProps={{
        onExited: () => changeTab("Details"),
      }}
    >
      <Tabs
        value={tab}
        onChange={(_, t) => {
          if (JOB_DIALOG_TABS.includes(t)) {
            changeTab(t)
          }
        }}
      >
        <Tab label="Details" value="Details" />
        {showLogs && <Tab label="Logs" value="Logs" />}
      </Tabs>
      {tab === "Details" && props.job && (
        <DialogContent>
          <JobDetails job={props.job} />
        </DialogContent>
      )}
      {tab === "Logs" && props.job && (
        <DialogContent className="lookout-dialog">
          <JobLogsContainer job={props.job} logService={props.logService} />
        </DialogContent>
      )}
    </Dialog>
  )
}
