import _ from "lodash"
import { TextField, TextFieldProps } from "@mui/material"

export interface DebouncedTextFieldProps {
  textFieldProps: TextFieldProps
  debouncedOnChange: (newValue: string) => void
  debounceWaitMs: number
}
export const DebouncedTextField = ({ debouncedOnChange, debounceWaitMs, textFieldProps }: DebouncedTextFieldProps) => {
  const onChange = _.debounce(debouncedOnChange, debounceWaitMs)
  return <TextField {...textFieldProps} onChange={(e) => onChange(e.currentTarget.value)} />
}
