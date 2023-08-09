import React from "react"
import styles from "./ActionButton.module.css"
type ActionButtonProps = {
  text: string
  actionFunc: () => void
}

export default function ActionButton(props: ActionButtonProps) {
  return (
    <button className={styles.actionBtn} onClick={() => (props?.actionFunc ? props?.actionFunc() : {})}>
      {props?.text}
    </button>
  )
}
