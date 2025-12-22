import { useMemo, useState } from "react"

import { Alert, AlertTitle, Autocomplete, Button, styled, TextField } from "@mui/material"

import { useUsername } from "../../../oidcAuth"
import { useGetQueues } from "../../../services/lookout/useGetQueues"

const StyledAutocomplete = styled(Autocomplete<string>)({
  width: 300,
})

export interface QueueSelectorProps {
  value: string
  onChange: (val: string) => void
}

export const QueueSelector = ({ value, onChange }: QueueSelectorProps) => {
  const username = useUsername()

  const [open, setOpen] = useState(false)
  const [enableGetQueues, setEnableGetQueues] = useState(false)
  const { refetch, status, error, data } = useGetQueues(enableGetQueues)
  const queueNames = useMemo(() => {
    // Display queues which contain the username first, since this likely indicates ownership of the queue.
    const allQueueNames = (data ?? []).flatMap(({ name }) => (name ? [name] : []))
    const queueNamesContainingUsername: string[] = []
    const queueNamesNotContainingUsername: string[] = []
    allQueueNames.forEach((name) => {
      if (username && name.includes(username)) {
        queueNamesContainingUsername.push(name)
      } else {
        queueNamesNotContainingUsername.push(name)
      }
    })
    return [...queueNamesContainingUsername, ...queueNamesNotContainingUsername]
  }, [data])

  if (status === "error") {
    return (
      <Alert
        severity="error"
        action={
          <Button color="inherit" size="small" onClick={() => refetch()}>
            Retry
          </Button>
        }
      >
        <AlertTitle>Failed to list available queues.</AlertTitle>
        {error}
      </Alert>
    )
  }

  return (
    <StyledAutocomplete
      id="queue"
      options={queueNames}
      open={open}
      onOpen={() => {
        setOpen(true)
        setEnableGetQueues(true)
      }}
      onClose={() => setOpen(false)}
      value={value ?? ""}
      onChange={(_, v) => {
        onChange(v ?? "")
      }}
      loading={status === "pending"}
      renderInput={(params) => <TextField {...params} label="Queue" variant="outlined" />}
    />
  )
}
