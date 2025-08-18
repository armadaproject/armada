import { useState } from "react"

import { Check } from "@mui/icons-material"
import { FormControl, FormHelperText, IconButton, InputAdornment, InputLabel, OutlinedInput } from "@mui/material"

const INPUT_LABEL_TEXT = "Annotation key"
const INPUT_ID = "annotation-key"

export interface EditAnnotationColumnInputProps {
  onEdit: (annotationKey: string) => void
  currentAnnotationKey: string
  existingAnnotationColumnKeys: Set<string>
}

export const EditAnnotationColumnInput = ({
  onEdit,
  existingAnnotationColumnKeys,
  currentAnnotationKey,
}: EditAnnotationColumnInputProps) => {
  const [value, setValue] = useState(currentAnnotationKey)

  const isValueFilled = value.length !== 0

  const valueHasNoLeadingTrailingWhitespace = value.trim() === value
  const valueIsUnique = !existingAnnotationColumnKeys.has(value) || value === currentAnnotationKey
  const isValueValid = [valueHasNoLeadingTrailingWhitespace, valueIsUnique].every(Boolean)

  const handleEdit = () => {
    onEdit(value)
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
          event.stopPropagation()
          if (event.key === "Enter" && isValueFilled && isValueValid) {
            handleEdit()
          }
        }}
        endAdornment={
          isValueFilled && isValueValid ? (
            <InputAdornment position="end">
              <IconButton
                aria-label={`Change this column's annotation to '${value}'`}
                edge="end"
                onClick={handleEdit}
                title={`Change this column's annotation to '${value}'`}
              >
                <Check />
              </IconButton>
            </InputAdornment>
          ) : undefined
        }
      />
      {!valueHasNoLeadingTrailingWhitespace && (
        <FormHelperText>The annotation key must not have leading or trailing whitespace.</FormHelperText>
      )}
      {!valueIsUnique && <FormHelperText>A column for this annotation key already exists.</FormHelperText>}
    </FormControl>
  )
}
