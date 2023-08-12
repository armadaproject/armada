import React, { useEffect, useState } from "react"

import { useSelector } from "react-redux"
import { useNavigate, useParams } from "react-router-dom"

import ActionButton from "./ActionButton"
import styles from "./JobLogDetailTab.module.css"

type JobLogDetailTabProps = {
  line: string
  timestamp: string
}

interface JobDetailLogInterface {
  jobLogSlice: {
    jobLog: { line: string; timestamp: string }[] | []
    loginfo: {
      runId: string
      jobRun: string
      container: string
    }
  }
}

export default function JobLogDetailTab() {
  const navigate = useNavigate()
  const { id } = useParams()
  const [showTimestamps, setShowTimestamps] = useState<boolean>(false)
  const { jobLogSlice } = useSelector((state: JobDetailLogInterface) => state)
  const [jobLogState, setJobLogState] = useState(jobLogSlice)

  useEffect(() => {
    if (!jobLogSlice) navigate("/")

    setJobLogState(jobLogSlice)
  }, [jobLogSlice])

  return (
    <section className={styles.jobLogTabContainer}>
      <div className={styles.jobLogTabHeaderContainer}>
        <div>
          <h4>Job Id</h4>
          {id}
        </div>
        <div>
          <h4>Start Time</h4>
          {jobLogSlice?.loginfo?.jobRun}
        </div>

        <div>
          <h4>Job Run Id</h4>
          {jobLogSlice?.loginfo?.runId}
        </div>

        <div>
          <h4>Container</h4>
          {jobLogSlice?.loginfo?.container}
        </div>
      </div>

      <div>
        <div className={styles.jobLogBtnContainer}>
          <ActionButton
            text={showTimestamps ? "Hide timestamps" : "Show timestamps"}
            actionFunc={() => setShowTimestamps((prevState) => !prevState)}
          />
        </div>
        <div className={styles.logView}>
          {jobLogState?.jobLog.map((logLine: JobLogDetailTabProps, i) => (
            <span key={`${i}-${logLine.timestamp}`}>
              {showTimestamps && <span className={styles.timestamp}>{logLine.timestamp}</span>}
              {logLine.line + "\n"}
            </span>
          ))}
        </div>
      </div>
    </section>
  )
}
