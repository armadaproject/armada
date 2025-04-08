import { RefObject, useEffect, useMemo, useRef, useState } from "react"

import { CheckBox, CheckBoxOutlineBlank } from "@mui/icons-material"
import {
  Alert,
  AlertTitle,
  Autocomplete,
  AutocompleteProps,
  Button,
  Checkbox,
  debounce,
  styled,
  TextField,
} from "@mui/material"

import { useGetQueues } from "../../services/lookout/useGetQueues"

const ELLIPSIS = "\u2026"
const FILTER_CHANGE_DEBOUNCE_MS = 300

const StyledAutocomplete = styled(Autocomplete<string, true>)({
  padding: 0,

  "& .MuiTextField-root, .MuiOutlinedInput-root.MuiInputBase-sizeSmall": {
    padding: 0,

    "& .MuiInputBase-input": {
      padding: "3.5px 7px",
      height: "1em",
      width: "100%",
    },
  },
})

const uncheckedIcon = <CheckBoxOutlineBlank fontSize="inherit" />
const checkedIcon = <CheckBox fontSize="inherit" />

const renderOption: AutocompleteProps<string, true, false, false>["renderOption"] = (
  { key, ...optionProps },
  option,
  { selected },
) => (
  <li key={key} {...optionProps}>
    <Checkbox icon={uncheckedIcon} checkedIcon={checkedIcon} checked={selected} />
    {option}
  </li>
)

const areQueueValuesEqual = (a: string[] | undefined, b: string[] | undefined) =>
  JSON.stringify((a ?? []).slice().sort()) === JSON.stringify((b ?? []).slice().sort())

export interface QueueFilterProps {
  filterValue: string[] | undefined
  parseError: string | undefined
  onFilterChange: (vals: string[] | undefined) => void
  onSetTextFieldRef: (ref: RefObject<HTMLInputElement | undefined>) => void
}

export const QueueFilter = ({ filterValue, parseError, onFilterChange, onSetTextFieldRef }: QueueFilterProps) => {
  const ref = useRef<HTMLInputElement>(undefined)
  useEffect(() => {
    onSetTextFieldRef(ref)
  }, [ref])

  const [open, setOpen] = useState(false)
  const [enableGetQueues, setEnableGetQueues] = useState(false)
  const { refetch, status, error, data } = useGetQueues(enableGetQueues)
  const queueNames = useMemo(() => (data ?? []).flatMap(({ name }) => (name ? [name] : [])), [data])

  const [autocompleteValue, setAutocompleteValue] = useState(filterValue ?? ([] as string[]))
  useEffect(() => {
    const newAutocompleteValue = filterValue ?? []
    if (!areQueueValuesEqual(newAutocompleteValue, autocompleteValue)) {
      setAutocompleteValue(newAutocompleteValue)
    }
  }, [filterValue])

  const debouncedOnFilterChange = useMemo(() => debounce(onFilterChange, FILTER_CHANGE_DEBOUNCE_MS), [onFilterChange])
  useEffect(() => {
    debouncedOnFilterChange(autocompleteValue.length === 0 ? undefined : autocompleteValue)
  }, [debouncedOnFilterChange, autocompleteValue])

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
      id="queue-filter"
      multiple
      limitTags={1}
      disableCloseOnSelect
      renderOption={renderOption}
      options={queueNames}
      size="small"
      open={open}
      onOpen={() => {
        setOpen(true)
        setEnableGetQueues(true)
      }}
      onClose={() => {
        setOpen(false)
      }}
      value={autocompleteValue}
      onChange={(_, v) => {
        setAutocompleteValue(v)
      }}
      loading={status === "pending"}
      renderInput={(params) => (
        <TextField
          {...params}
          inputRef={ref}
          placeholder={`Select queues${ELLIPSIS}`}
          error={parseError !== undefined}
          helperText={parseError}
        />
      )}
    />
  )
}
