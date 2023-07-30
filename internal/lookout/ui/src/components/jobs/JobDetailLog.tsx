import React, { useEffect } from "react"

import "./JobDetailLog.css"
import { useSelector } from "react-redux"
import { useNavigate } from "react-router-dom"

type JobDetailLogProps = {
  line: string
  timestamp: string
}

interface JobDetailLogInterface {
  jobLogSlice: JobDetailLogProps[]
}

export default function JobDetailLog() {
  const navigate = useNavigate()

  const { jobLogSlice } = useSelector((state: JobDetailLogInterface) => state)

  useEffect(() => {
    if (!jobLogSlice) navigate("/")
  }, [])

  return (
    <div className="job-detail-log">
      <h2 className="job-detail-log-header">Job Log View</h2>
      <div>
        {jobLogSlice.map((l: JobDetailLogProps) => (
          <p key={l?.timestamp}>{l?.line} </p>
        ))}
      </div>
    </div>
  )
}
