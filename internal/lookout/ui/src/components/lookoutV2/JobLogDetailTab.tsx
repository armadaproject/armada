import React, { useEffect, useState } from "react"
import { useSelector } from "react-redux"
import { useNavigate } from "react-router-dom"
import styles from "./JobLogDetailTab.module.css"
import ActionButton from "./ActionButton"

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
    <section className={styles.jobLogTabContainer}>
      <h2>Job Log View</h2>

      <div>
        <div className={styles.jobLogBtnContainer}>
          <ActionButton
            text={showTimestamps ? "Hide timestamps" : "Show timestamps"}
            actionFunc={() => setShowTimestamps((prevState) => !prevState)}
          />
        </div>
        <div className={styles.logView}>
          {jobLogSlice.map((logLine: JobLogDetailTabProps, i) => (
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
