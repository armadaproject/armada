import React, { useEffect, useState } from "react"

import "./JobDetailLog.css"
import { useNavigate } from "react-router-dom"

type JobDetailLogProps = {
  line: string
  timestamp: string
}

export default function JobDetailLog() {
  const navigate = useNavigate()
  const [jobLogList, setJobLogLst] = useState<JobDetailLogProps[]>([])

  useEffect(() => {
    const cachedLog = JSON.parse(localStorage.getItem("jobLog") || "[]")
    if (!cachedLog) navigate("/")
    else setJobLogLst(cachedLog)
  }, [])
  return (
    <div className="job-detail-log">
      <h2 className="job-detail-log-header">Job Log View</h2>
      <div>
        {jobLogList.map((l: JobDetailLogProps) => (
          <p key={l?.timestamp}>{l?.line} </p>
        ))}
      </div>
    </div>
  )
}
