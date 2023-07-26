import React from 'react'

type JobDetailLogProps = {
  jobLog: string
  key: string
}

// For displaying only job log strings
export default function JobDetailLog (props: JobDetailLogProps) {
  return (
    <p key={props?.key}>{props?.jobLog}</p>
  )
}

