import React from "react"
import styles from "./SidebarTabJobLogsHeaderItem.module.css"

type SidebarTabJobLogsHeaderItemProps = {
  header: string
  headerValue: string | number | undefined
  keyValue: number | string
}

export default function SidebarTabJobLogsHeaderItem(props: SidebarTabJobLogsHeaderItemProps) {
  return (
    <div className={styles.headerJobLog} key={props?.keyValue}>
      <h4>
        {props?.header}
        <span className={styles.headerJobLogDivider}> : </span>
      </h4>
      <div>{props?.headerValue} </div>
    </div>
  )
}
