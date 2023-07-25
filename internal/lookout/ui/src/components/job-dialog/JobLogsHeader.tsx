import React from 'react'
type JobLogsHeaderProps = {
  header: string,
  headerValue: string | number | undefined
}

export default  function JobLogsHeader(props: JobLogsHeaderProps) {
  return (
    <div>
      <h3>{ props?.header}</h3>
    <div>{props?.headerValue} </div>
  </div>
  )
}



