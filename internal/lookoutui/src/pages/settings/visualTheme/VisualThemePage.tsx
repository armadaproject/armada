import { ExpandMore } from "@mui/icons-material"
import { Accordion, AccordionDetails, AccordionSummary, Chip, Divider, Stack, Typography } from "@mui/material"

import { SPACING } from "../../../common/spacing"
import { CUSTOM_LOOKOUT_UI_THEME_NAME, useLookoutThemeConfigState } from "../../../lookoutThemeState"

import { ColourModeSelector } from "./components/ColourModeSelector"
import { CustomThemeBuilder } from "./components/CustomThemeBuilder"
import { ThemePreviewCard } from "./components/ThemePreviewCard"

export const VisualThemePage = () => {
  const { selectedThemeConfigIndex, setSelectedThemeConfigIndex, selectableThemeConfigs } = useLookoutThemeConfigState()

  const customThemeConfig = selectableThemeConfigs.find(({ name }) => name === CUSTOM_LOOKOUT_UI_THEME_NAME)
  const customThemeConfigIndex = selectableThemeConfigs.findIndex(({ name }) => name === CUSTOM_LOOKOUT_UI_THEME_NAME)

  return (
    <Stack spacing={SPACING.md}>
      <div>
        <Typography variant="h4">Colour mode</Typography>
      </div>
      <div>
        <Typography>Select your preferred colour mode, or sync it with your system.</Typography>
      </div>
      <ColourModeSelector />
      <div>
        <Divider variant="fullWidth" />
      </div>
      <div>
        <Typography variant="h4">Visual theme</Typography>
      </div>
      <div>
        <Typography>
          Choose the appearance of the Lookout UI. You can choose from the default Armada Lookout theme and preset
          themes set by your Armada administrator, or create your own theme.
        </Typography>
      </div>
      <Stack spacing={SPACING.sm}>
        {selectableThemeConfigs
          .filter(({ name }) => name !== CUSTOM_LOOKOUT_UI_THEME_NAME)
          .map(({ name, displayName, themeConfig }, i) => (
            <ThemePreviewCard
              key={name}
              displayName={displayName}
              onClick={() => setSelectedThemeConfigIndex(i)}
              selected={selectedThemeConfigIndex === i}
              customTheme={false}
              themeConfig={themeConfig}
            />
          ))}
      </Stack>
      {customThemeConfig && (
        <>
          <Stack direction="row" spacing={SPACING.md} alignItems="center">
            <Typography component="div" variant="h5">
              Custom theme
            </Typography>
            {selectedThemeConfigIndex === customThemeConfigIndex && (
              <div>
                <Chip label="Selected" variant="outlined" color="primary" />
              </div>
            )}
          </Stack>
          <div>
            <Accordion>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Typography component="span">Modify your custom theme</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <div>
                  <CustomThemeBuilder />
                </div>
              </AccordionDetails>
            </Accordion>
          </div>
          <div>
            <ThemePreviewCard
              displayName={customThemeConfig.displayName}
              onClick={() => setSelectedThemeConfigIndex(customThemeConfigIndex)}
              selected={selectedThemeConfigIndex === customThemeConfigIndex}
              customTheme
              themeConfig={customThemeConfig.themeConfig}
            />
          </div>
        </>
      )}
    </Stack>
  )
}
