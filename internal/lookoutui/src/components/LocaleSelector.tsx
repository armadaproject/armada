import { FormControl, FormControlProps, InputLabel, MenuItem, Select } from "@mui/material"

import {
  BROWSER_LOCALE,
  isSupportedLocale,
  SUPPORTED_LOCALES,
  SupportedLocale,
  supportedLocaleDisplayNames,
} from "../common/locales"

export interface LocaleSelectorProps {
  idPrefix: string
  label: string
  value: SupportedLocale | typeof BROWSER_LOCALE
  onChange: (locale: SupportedLocale | typeof BROWSER_LOCALE) => void
  fullWidth?: FormControlProps["fullWidth"]
  size?: FormControlProps["size"]
}

export const LocaleSelector = ({ idPrefix, label, value, onChange, fullWidth, size }: LocaleSelectorProps) => (
  <FormControl fullWidth={fullWidth} size={size}>
    <InputLabel id={`${idPrefix}-locale-selector-label`}>{label}</InputLabel>
    <Select
      labelId={`${idPrefix}-locale-selector-label`}
      id={`${idPrefix}-locale-selector`}
      value={value}
      label={label}
      onChange={({ target: { value } }) => {
        if (value && (isSupportedLocale(value) || value === BROWSER_LOCALE)) {
          onChange(value)
        }
      }}
    >
      <MenuItem value={BROWSER_LOCALE}>Use your browser's language</MenuItem>
      {SUPPORTED_LOCALES.map((locale) => (
        <MenuItem key={locale} value={locale}>
          {supportedLocaleDisplayNames[locale]}
        </MenuItem>
      ))}
    </Select>
  </FormControl>
)
