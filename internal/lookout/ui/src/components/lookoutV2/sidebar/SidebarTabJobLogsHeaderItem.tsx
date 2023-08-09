import React from "react"

type SidebarTabJobLogsHeaderItemProps = {
  header: string
  headerValue: string | number | undefined
}

export default function SidebarTabJobLogsHeaderItem(props: SidebarTabJobLogsHeaderItemProps) {
  return (
    <div className="header-job-log">
      <h4>
        {props?.header}
        <span className="header-job-log-divider"> : </span>
      </h4>
      <div>{props?.headerValue} </div>
    </div>
  )
}
