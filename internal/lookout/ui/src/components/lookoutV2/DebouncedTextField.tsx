import { useRef } from "react"

import { TextField, TextFieldProps } from "@mui/material"
import _ from "lodash"

export interface DebouncedTextFieldProps {
  textFieldProps: TextFieldProps
  debouncedOnChange: (newValue: string) => void
  debounceWaitMs: number
}

export const DebouncedTextField = ({ debouncedOnChange, debounceWaitMs, textFieldProps }: DebouncedTextFieldProps) => {
  const onChange = _.debounce(debouncedOnChange, debounceWaitMs)
  return <TextField {...textFieldProps} onChange={(e) => onChange(e.currentTarget.value)} />
}
