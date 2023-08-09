import React from "react"
import styles from "./SidebarTabJobLogsHeaderItem.module.css"

type SidebarTabJobLogsHeaderItemProps = {
  header: string
  headerValue: string | number | undefined
}

export default function SidebarTabJobLogsHeaderItem(props: SidebarTabJobLogsHeaderItemProps) {
  return (
    <div className={styles.headerJobLog}>
      <h4>
        {props?.header}
        <span className={styles.headerJobLogDivider}> : </span>
      </h4>
      <div>{props?.headerValue} </div>
    </div>
  )
}
