import { FormControl, FormControlProps, InputLabel, MenuItem, Select } from "@mui/material"

import { isNumberNotation, NUMBER_NOTATIONS, NumberNotation, numberNotationDisplayNames } from "../common/formatNumber"

export interface NumberNotationSelectorProps {
  idPrefix: string
  label: string
  value: NumberNotation
  onChange: (notation: NumberNotation) => void
  fullWidth?: FormControlProps["fullWidth"]
  size?: FormControlProps["size"]
}

export const NumberNotationSelector = ({
  idPrefix,
  label,
  value,
  onChange,
  fullWidth,
  size,
}: NumberNotationSelectorProps) => (
  <FormControl fullWidth={fullWidth} size={size}>
    <InputLabel id={`${idPrefix}-number-notation-selector-label`}>{label}</InputLabel>
    <Select
      labelId={`${idPrefix}-number-notation-selector-label`}
      id={`${idPrefix}-number-notation-selector`}
      value={value}
      label={label}
      onChange={({ target: { value } }) => {
        if (value && isNumberNotation(value)) {
          onChange(value)
        }
      }}
    >
      {NUMBER_NOTATIONS.map((notation) => (
        <MenuItem key={notation} value={notation}>
          {numberNotationDisplayNames[notation]}
        </MenuItem>
      ))}
    </Select>
  </FormControl>
)
