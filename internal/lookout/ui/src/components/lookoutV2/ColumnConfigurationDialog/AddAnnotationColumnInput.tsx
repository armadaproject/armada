import { useState } from "react"

import { AddCircle } from "@mui/icons-material"
import { FormControl, FormHelperText, IconButton, InputAdornment, InputLabel, OutlinedInput } from "@mui/material"

const INPUT_LABEL_TEXT = "Annotation key"
const INPUT_ID = "annotation-key"

export interface AddAnnotationColumnInputProps {
  onCreate: (annotationKey: string) => void
  existingAnnotationColumnKeysSet: Set<string>
}

export const AddAnnotationColumnInput = ({
  onCreate,
  existingAnnotationColumnKeysSet,
}: AddAnnotationColumnInputProps) => {
  const [value, setValue] = useState("")

  const isValueFilled = value.length !== 0

  const valueHasNoLeadingTrailingWhitespace = value.trim() === value
  const valueIsNew = !existingAnnotationColumnKeysSet.has(value)
  const isValueValid = [valueHasNoLeadingTrailingWhitespace, valueIsNew].every(Boolean)

  const handleCreate = () => {
    onCreate(value)
    setValue("")
  }

  return (
    <FormControl fullWidth size="small" margin="normal" error={!isValueValid}>
      <InputLabel htmlFor={INPUT_ID}>{INPUT_LABEL_TEXT}</InputLabel>
      <OutlinedInput
        id={INPUT_ID}
        label={INPUT_LABEL_TEXT}
        value={value}
        onChange={({ target }) => {
          setValue(target.value)
        }}
        onKeyDown={(event) => {
          if (event.key === "Enter" && isValueFilled && isValueValid) {
            handleCreate()
            event.currentTarget.blur()
            event.preventDefault()
          }
        }}
        endAdornment={
          isValueFilled && isValueValid ? (
            <InputAdornment position="end">
              <IconButton
                aria-label={`Add a column for annotation '${value}'`}
                edge="end"
                onClick={handleCreate}
                title={`Add a column for annotation '${value}'`}
              >
                <AddCircle />
              </IconButton>
            </InputAdornment>
          ) : undefined
        }
      />
      {!valueHasNoLeadingTrailingWhitespace && (
        <FormHelperText>The annotation key must not have leading or trailing whitespace.</FormHelperText>
      )}
      {!valueIsNew && <FormHelperText>A column for this annotation key already exists.</FormHelperText>}
    </FormControl>
  )
}
