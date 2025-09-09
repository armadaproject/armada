import { Container, Grid } from "@mui/material"
import { Outlet } from "react-router-dom"

import { SPACING } from "../../common/spacing"

import { SettingsNav } from "./components/SettingsNav"

export const SettingsPage = () => (
  <Container maxWidth="xl" sx={{ py: SPACING.md }}>
    <Grid container spacing={{ xs: SPACING.md, md: SPACING.xl }}>
      <Grid size={{ xs: 12, md: 3, xl: 2 }}>
        <SettingsNav />
      </Grid>
      <Grid size={{ xs: 12, md: 9, xl: 10 }}>
        <Outlet />
      </Grid>
    </Grid>
  </Container>
)
