import React, { useEffect, useState } from "react"
import { useSelector } from "react-redux"
import { useNavigate } from "react-router-dom"
import styles from "./JobLogDetailTab.module.css"

type JobLogDetailTabProps = {
  line: string
  timestamp: string
}

interface JobDetailLogInterface {
  jobLogSlice: JobLogDetailTabProps[]
}

export default function JobLogDetailTab() {
  const navigate = useNavigate()
  const [showTimestamps, setShowTimestamps] = useState<boolean>(false)

  const { jobLogSlice } = useSelector((state: JobDetailLogInterface) => state)

  useEffect(() => {
    if (!jobLogSlice) navigate("/")
  }, [])

  return (
    <div className="job-detail-log">
      <h2 className="job-detail-log-header">V2 Job Log View</h2>
      <div className={styles.logView}>
        {jobLogSlice.map((logLine: JobLogDetailTabProps, i) => (
          <span key={`${i}-${logLine.timestamp}`}>
            {showTimestamps && <span className={styles.timestamp}>{logLine.timestamp}</span>}
            {logLine.line + "\n"}
          </span>
        ))}
      </div>
    </div>
  )
}
