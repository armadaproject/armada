import {
  Stack,
  Typography,
  Divider,
  FormControlLabel,
  FormGroup,
  Switch,
  FormHelperText,
  Alert,
  AlertTitle,
} from "@mui/material"

import { TIMESTAMP_FORMATS, timestampFormatDisplayNames } from "../../../common/formatTime"
import { SPACING } from "../../../common/spacing"
import { LocaleSelector } from "../../../components/LocaleSelector"
import { NumberNotationSelector } from "../../../components/NumberNotationSelector"
import { TimeZoneSelector } from "../../../components/TimeZoneSelector"
import { useFormatNumberWithUserSettings } from "../../../components/hooks/formatNumberWithUserSettings"
import { useFormatIsoTimestampWithUserSettings } from "../../../components/hooks/formatTimeWithUserSettings"
import {
  useFormatTimestampShouldFormat,
  useFormatTimestampTimeZone,
  useFormatTimestampLocale,
  useFormatNumberShouldFormat,
  useFormatNumberLocale,
  useFormatNumberNotation,
} from "../../../userSettings"

const FORMAT_NUMBER_PREVIEW_VALUES = [0, 0.034, 7, 888, 65536, 11197253, 6412378912]

export const ValueDisplayPage = () => {
  const [formatTimestampShouldFormat, setFormatTimestampShouldFormat] = useFormatTimestampShouldFormat()
  const [formatTimestampTimeZone, setFormatTimestampTimeZone] = useFormatTimestampTimeZone()
  const [formatTimestampLocale, setFormatTimestampLocale] = useFormatTimestampLocale()
  const [formatNumberShouldFormat, setFormatNumberShouldFormat] = useFormatNumberShouldFormat()
  const [formatNumberLocale, setFormatNumberLocale] = useFormatNumberLocale()
  const [formatNumberNotation, setFormatNumberNotation] = useFormatNumberNotation()
  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()
  const formatNumber = useFormatNumberWithUserSettings()

  return (
    <Stack spacing={SPACING.md}>
      <div>
        <Typography variant="h4">Timestamp display</Typography>
      </div>
      <div>
        <Typography>Choose how timestamp values are displayed across the app.</Typography>
      </div>
      <div>
        <FormGroup>
          <FormControlLabel
            control={
              <Switch
                checked={formatTimestampShouldFormat}
                onChange={({ target: { checked } }) => setFormatTimestampShouldFormat(checked)}
              />
            }
            label="Format timestamps"
          />
          {!formatTimestampShouldFormat && (
            <FormHelperText>Timestamps will be displayed in the ISO 8601 format.</FormHelperText>
          )}
        </FormGroup>
      </div>
      {formatTimestampShouldFormat && (
        <div>
          <Stack direction="row" spacing={SPACING.sm}>
            <div>
              <LocaleSelector
                idPrefix="settings-dialog-timestamps"
                label="Locale for timestamps"
                value={formatTimestampLocale}
                onChange={setFormatTimestampLocale}
                fullWidth
                size="small"
              />
            </div>
            <div>
              <TimeZoneSelector
                idPrefix="settings-dialog-timestamps"
                label="Time zone location for timestamps"
                value={formatTimestampTimeZone}
                onChange={setFormatTimestampTimeZone}
                fullWidth
                size="small"
              />
            </div>
          </Stack>
        </div>
      )}
      <div>
        <Alert icon={false} severity="info">
          {formatTimestampShouldFormat ? (
            <>
              <AlertTitle>Examples</AlertTitle>
              <Stack direction="row" spacing={SPACING.sm} flexWrap="wrap">
                <ul>
                  {TIMESTAMP_FORMATS.map((format) => (
                    <li key={format}>
                      {timestampFormatDisplayNames[format]}: {formatIsoTimestamp(new Date().toISOString(), format)}
                    </li>
                  ))}
                </ul>
              </Stack>
            </>
          ) : (
            <>
              <AlertTitle>Example</AlertTitle>
              <Stack direction="row" spacing={SPACING.sm} flexWrap="wrap">
                {formatIsoTimestamp(new Date().toISOString(), "full")}
              </Stack>
            </>
          )}
        </Alert>
      </div>
      <div>
        <Divider variant="fullWidth" />
      </div>
      <div>
        <Typography variant="h4">Number display</Typography>
      </div>
      <div>
        <Typography>Choose how numbers are displayed across the app.</Typography>
      </div>
      <div>
        <FormGroup>
          <FormControlLabel
            control={
              <Switch
                checked={formatNumberShouldFormat}
                onChange={({ target: { checked } }) => setFormatNumberShouldFormat(checked)}
              />
            }
            label="Format numbers"
          />
        </FormGroup>
      </div>
      {formatNumberShouldFormat && (
        <div>
          <Stack direction="row" spacing={SPACING.sm}>
            <div>
              <LocaleSelector
                idPrefix="settings-dialog-numbers"
                label="Locale for numbers"
                value={formatNumberLocale}
                onChange={setFormatNumberLocale}
                fullWidth
                size="small"
              />
            </div>
            <div>
              <NumberNotationSelector
                idPrefix="settings-dialog-numbers"
                label="Number notation"
                value={formatNumberNotation}
                onChange={setFormatNumberNotation}
                fullWidth
                size="small"
              />
            </div>
          </Stack>
        </div>
      )}
      <div>
        <Alert icon={false} severity="info">
          <AlertTitle>Examples</AlertTitle>
          <Stack direction="row" spacing={SPACING.sm} flexWrap="wrap">
            {FORMAT_NUMBER_PREVIEW_VALUES.map((v) => (
              <div key={v}>{formatNumber(v)}</div>
            ))}
          </Stack>
        </Alert>
      </div>
    </Stack>
  )
}
