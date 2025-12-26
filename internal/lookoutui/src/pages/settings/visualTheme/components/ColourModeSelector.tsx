import {
  FormControlLabel,
  styled,
  SupportedColorScheme,
  Switch,
  ToggleButton,
  ToggleButtonGroup,
  useColorScheme,
  useMediaQuery,
} from "@mui/material"

import { SPACING } from "../../../../common/spacing"

const ColourModeSelectorContainer = styled("div")(({ theme }) => ({
  display: "flex",
  flexDirection: "row",
  gap: theme.spacing(SPACING.md),
  flexWrap: "wrap",
}))

export const ColourModeSelector = () => {
  const { mode, setMode } = useColorScheme()
  const systemMode: SupportedColorScheme = useMediaQuery("(prefers-color-scheme: dark)") ? "dark" : "light"

  return (
    <ColourModeSelectorContainer>
      <div>
        <FormControlLabel
          control={
            <Switch
              checked={mode === "system"}
              onChange={(_, checked) => {
                if (checked) {
                  setMode("system")
                } else {
                  setMode(systemMode === "dark" ? "dark" : "light")
                }
              }}
            />
          }
          label="Sync with system"
        />
      </div>
      <div>
        <ToggleButtonGroup
          color="primary"
          disabled={mode === undefined}
          value={mode === "system" ? systemMode : (mode ?? "light")}
          exclusive
          aria-label="colour mode"
          onChange={(_, colorMode) => setMode(colorMode)}
        >
          <ToggleButton value="light">
            Light mode{mode === "system" && systemMode === "light" && <> (synced with system)</>}
          </ToggleButton>
          <ToggleButton value="dark">
            Dark mode{mode === "system" && systemMode === "dark" && <> (synced with system)</>}
          </ToggleButton>
        </ToggleButtonGroup>
      </div>
    </ColourModeSelectorContainer>
  )
}
