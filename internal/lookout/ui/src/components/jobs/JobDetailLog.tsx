import React, { useEffect } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import './JobDetailLog.css'

type JobDetailLogProps = {
  line: string
  timestamp: string
}


export default function JobDetailLog() {
  const navigate = useNavigate()
  const location = useLocation()
  const {jobLogList} = location.state
  useEffect(() => {
    if(!jobLogList) navigate('/')
  },[])
  return (
    <div className='job-detail-log'>
      <h2 className='job-detail-log-header'>Job Log View</h2>
      <div>
       {jobLogList.map((l: JobDetailLogProps) => <p key={l?.timestamp}>{l?.line} </p>
            )}
      </div>
   </div>
  )
}

