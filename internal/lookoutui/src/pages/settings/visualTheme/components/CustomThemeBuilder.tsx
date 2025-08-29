import { useCallback, useEffect, useState } from "react"

import { ArrowForward, Restore } from "@mui/icons-material"
import {
  Alert,
  Button,
  Checkbox,
  Divider,
  FormControlLabel,
  FormLabel,
  Grid2,
  IconButton,
  Paper,
  Slider,
  Stack,
  styled,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material"
import { useDebouncedCallback } from "use-debounce"

import { SPACING } from "../../../../common/spacing"
import { CodeBlock } from "../../../../components/CodeBlock"
import { HexColourPicker } from "../../../../components/HexColourPicker"
import {
  DEFAULT_LOOKOUT_THEME_CONFIG_OPTIONS,
  LookoutThemeConfigOptions,
  lookoutThemeConfigOptionsFromJSON,
  mergeLookoutThemeConfigWithDefaults,
} from "../../../../theme"
import { useVisualThemeCustomConfig } from "../../../../userSettings"

const DEBOUNCE_MS = 600

const SliderContainer = styled("div")({ padding: "0 20px" })

const ColoursTableContainer = styled(TableContainer<typeof Paper>)({ width: "fit-content" })

const TYPOGRAPHY_FIELDS = [
  { displayName: "Font family", key: "fontFamily" as const },
  { displayName: "Monospace font family", key: "monospaceFontFamily" as const },
] as const

const CHECKBOX_FIELDS = [
  { displayName: "Uppercase button text", key: "uppercaseButtonText" as const },
  { displayName: "Uppercase overline text", key: "uppercaseOverlineText" as const },
] as const

const COLOUR_FIELDS = [
  { displayName: "Primary colour", key: "primaryColour" as const },
  { displayName: "Secondary colour", key: "secondaryColour" as const },
  { displayName: "App bar colour", key: "appBarColour" as const },
  { displayName: "Error colour", key: "errorColour" as const },
  { displayName: "Warning colour", key: "warningColour" as const },
  { displayName: "Info colour", key: "infoColour" as const },
  { displayName: "Success colour", key: "successColour" as const },
  { displayName: "Queued status colour", key: "statusQueuedColour" as const },
  { displayName: "Pending status colour", key: "statusPendingColour" as const },
  { displayName: "Running status colour", key: "statusRunningColour" as const },
  { displayName: "Succeeded status colour", key: "statusSucceededColour" as const },
  { displayName: "Failed status colour", key: "statusFailedColour" as const },
  { displayName: "Cancelled status colour", key: "statusCancelledColour" as const },
  { displayName: "Preempted status colour", key: "statusPreemptedColour" as const },
  { displayName: "Leased status colour", key: "statusLeasedColour" as const },
  { displayName: "Rejected status colour", key: "statusRejectedColour" as const },
  { displayName: "Default background colour", key: "defaultBackgroundColour" as const },
  { displayName: "Paper surface background colour", key: "paperSurfaceBackgroundColour" as const },
] as const

interface TextFieldProps {
  id: string
  label: string
  value: string
  onChange: (value: string) => void
}

const ThemedTextField = ({ id, label, value, onChange }: TextFieldProps) => (
  <Grid2 size={{ xs: 6 }}>
    <TextField
      id={id}
      margin="normal"
      label={label}
      variant="outlined"
      size="small"
      fullWidth
      value={value}
      onChange={({ target: { value } }) => onChange(value)}
    />
  </Grid2>
)

interface CheckboxFieldProps {
  label: string
  checked: boolean
  onChange: (checked: boolean) => void
}

const ThemedCheckboxField = ({ label, checked, onChange }: CheckboxFieldProps) => (
  <Grid2 size={{ xs: 6 }}>
    <FormControlLabel
      control={<Checkbox checked={checked} />}
      onChange={(_, checked) => onChange(checked)}
      label={label}
    />
  </Grid2>
)

interface ColourRowProps {
  displayName: string
  lightColour: string | number | boolean
  darkColour: string | number | boolean
  onLightColourChange: (colour: string) => void
  onDarkColourChange: (colour: string) => void
  onCopyLightToDark: () => void
}

const ColourTableRow = ({
  displayName,
  lightColour,
  darkColour,
  onLightColourChange,
  onDarkColourChange,
  onCopyLightToDark,
}: ColourRowProps) => (
  <TableRow>
    <TableCell component="th" scope="row">
      {displayName}
    </TableCell>
    <TableCell align="center">
      <HexColourPicker value={String(lightColour)} onChange={onLightColourChange} />
    </TableCell>
    <TableCell align="center">
      <Tooltip title="Set dark mode colour to light mode colour">
        <IconButton onClick={onCopyLightToDark}>
          <ArrowForward />
        </IconButton>
      </Tooltip>
    </TableCell>
    <TableCell align="center">
      <HexColourPicker value={String(darkColour)} onChange={onDarkColourChange} />
    </TableCell>
  </TableRow>
)

export const CustomThemeBuilder = () => {
  const [visualThemeCustomConfig, setVisualThemeCustomConfig] = useVisualThemeCustomConfig()

  // State for theme configuration
  const [inputState, setInputState] = useState<Required<LookoutThemeConfigOptions>>(
    mergeLookoutThemeConfigWithDefaults(visualThemeCustomConfig),
  )

  const debouncedSetVisualThemeCustomConfig = useDebouncedCallback(setVisualThemeCustomConfig, DEBOUNCE_MS)

  // JSON import/export functionality
  const [customThemeJSONCode, setCustomThemeJSONCode] = useState("")
  const [customThemeJSONCodeInvalid, setCustomThemeJSONCodeInvalid] = useState(false)

  const applyCustomThemeCode = useCallback(
    (code: string) => {
      if (!code) {
        return
      }
      const parsed = lookoutThemeConfigOptionsFromJSON(code)
      if (Object.keys(parsed).length === 0) {
        setCustomThemeJSONCodeInvalid(true)
        return
      }
      setCustomThemeJSONCodeInvalid(false)
      setVisualThemeCustomConfig(parsed)
      setInputState(mergeLookoutThemeConfigWithDefaults(parsed))
    },
    [setVisualThemeCustomConfig],
  )

  const debouncedApplyCustomThemeCode = useDebouncedCallback(applyCustomThemeCode, DEBOUNCE_MS)

  const onReset = useCallback(() => {
    setVisualThemeCustomConfig(DEFAULT_LOOKOUT_THEME_CONFIG_OPTIONS)
    setInputState(DEFAULT_LOOKOUT_THEME_CONFIG_OPTIONS)
  }, [setVisualThemeCustomConfig])

  const handleFieldChange = useCallback(
    <T extends keyof Required<LookoutThemeConfigOptions>>(key: T, value: Required<LookoutThemeConfigOptions>[T]) => {
      setInputState((prev) => ({ ...prev, [key]: value }))
    },
    [],
  )

  const handleCopyLightToDark = useCallback(
    (colourKey: keyof Required<LookoutThemeConfigOptions>) => {
      const darkKey = `${colourKey}Dark` as keyof Required<LookoutThemeConfigOptions>
      const lightValue = inputState[colourKey]
      setInputState((prev) => ({ ...prev, [darkKey]: lightValue }))
    },
    [inputState],
  )

  // Sync input state with visual theme config
  useEffect(() => {
    debouncedSetVisualThemeCustomConfig(inputState)
  }, [debouncedSetVisualThemeCustomConfig, inputState])

  // Apply custom theme code
  useEffect(() => {
    debouncedApplyCustomThemeCode(customThemeJSONCode)
  }, [debouncedApplyCustomThemeCode, customThemeJSONCode])

  return (
    <Stack spacing={SPACING.sm}>
      <Grid2 container spacing={SPACING.sm}>
        <Grid2 size={12}>
          <Typography variant="h6">Typography</Typography>
        </Grid2>
        {TYPOGRAPHY_FIELDS.map(({ displayName, key }) => (
          <ThemedTextField
            key={key}
            id={key}
            label={displayName}
            value={inputState[key]}
            onChange={(value) => handleFieldChange(key, value)}
          />
        ))}
        {CHECKBOX_FIELDS.map(({ displayName, key }) => (
          <ThemedCheckboxField
            key={key}
            label={displayName}
            checked={inputState[key]}
            onChange={(checked) => handleFieldChange(key, checked)}
          />
        ))}
        <Grid2 size={12}>
          <Typography variant="h6">Shape</Typography>
        </Grid2>
        <Grid2 size={{ xs: 6 }}>
          <FormLabel>Border radius</FormLabel>
          <SliderContainer>
            <Slider
              value={inputState.borderRadiusPx}
              onChange={(_, value) => {
                const newValue = Array.isArray(value) ? value[0] : value
                handleFieldChange("borderRadiusPx", newValue)
              }}
              step={1}
              marks={[
                { value: 0, label: "0px" },
                { value: 6, label: "6px" },
                { value: 12, label: "12px" },
              ]}
              valueLabelDisplay="auto"
              min={0}
              max={12}
            />
          </SliderContainer>
        </Grid2>
        <Grid2 size={12}>
          <Typography variant="h6">Colour palette</Typography>
        </Grid2>
        <Grid2 size={12}>
          <ColoursTableContainer component={Paper}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell></TableCell>
                  <TableCell>Light mode</TableCell>
                  <TableCell />
                  <TableCell>Dark mode</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {COLOUR_FIELDS.map(({ displayName, key }) => (
                  <ColourTableRow
                    key={key}
                    displayName={displayName}
                    lightColour={inputState[key]}
                    darkColour={inputState[`${key}Dark` as keyof typeof inputState]}
                    onLightColourChange={(newColour) => handleFieldChange(key, newColour)}
                    onDarkColourChange={(newColour) => {
                      const darkKey = `${key}Dark` as keyof Required<LookoutThemeConfigOptions>
                      handleFieldChange(darkKey, newColour)
                    }}
                    onCopyLightToDark={() => handleCopyLightToDark(key)}
                  />
                ))}
              </TableBody>
            </Table>
          </ColoursTableContainer>
        </Grid2>
      </Grid2>
      <div>
        <Divider variant="fullWidth" />
      </div>
      <div>
        <Typography variant="h6">Reset custom theme</Typography>
      </div>
      <div>
        <Typography>Discard your customisations and reset it to the default Armada theme.</Typography>
      </div>
      <div>
        <Button variant="outlined" startIcon={<Restore />} color="error" onClick={onReset}>
          Reset to default theme
        </Button>
      </div>

      <Divider variant="fullWidth" />

      <Typography variant="h6">Save and share your custom theme</Typography>
      <Typography>Copy this JSON code to save your custom theme.</Typography>
      <CodeBlock
        code={JSON.stringify(visualThemeCustomConfig, undefined, 2)}
        language="json"
        showLineNumbers={false}
        loading={false}
        downloadable={false}
      />

      <Typography variant="h6">Import a custom theme</Typography>
      <Typography>Paste the JSON code for a custom theme to import it.</Typography>
      <TextField
        id="import-custom-theme-json-code"
        margin="normal"
        label="Custom theme JSON code"
        value={customThemeJSONCode}
        onChange={({ target: { value } }) => setCustomThemeJSONCode(value)}
        multiline
        rows={4}
        variant="outlined"
        fullWidth
      />
      {customThemeJSONCodeInvalid && <Alert severity="error">This custom theme code is not valid.</Alert>}
    </Stack>
  )
}
