import React from "react"

import { Button, CircularProgress, Theme, createStyles } from "@material-ui/core"
import { makeStyles } from "@material-ui/core/styles"

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    loadingWrapper: {
      position: "relative",
    },
    loading: {
      color: theme.palette.secondary.main,
      position: "absolute",
      top: "50%",
      left: "50%",
      marginTop: -12,
      marginLeft: -12,
    },
  }),
)

type LoadingButtonProps = {
  content: string
  isLoading: boolean
  isDisabled?: boolean
  onClick: () => void
}

export default function LoadingButton(props: LoadingButtonProps) {
  const classes = useStyles()

  return (
    <div className={classes.loadingWrapper}>
      <Button color="secondary" onClick={props.onClick} disabled={props.isLoading || props.isDisabled}>
        {props.content}
      </Button>
      {props.isLoading && <CircularProgress size={24} className={classes.loading} />}
    </div>
  )
}
