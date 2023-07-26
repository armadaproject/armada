import React, { useEffect } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'

type JobDetailLogProps = {
  line: string
  timestamp: string
}

// For displaying only job log strings
export default function JobDetailLog() {
  const navigate = useNavigate()
  const location = useLocation()
  const {jobLogList} = location.state
  useEffect(() => {
    if(!jobLogList) navigate('/')
  },[])
  return (
    <div>
       {jobLogList.map((l: JobDetailLogProps) => <p key={l?.timestamp}>{l?.line} </p>
            )}
   </div>
  )
}

