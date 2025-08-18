import { Container, Grid2 } from "@mui/material"
import { Outlet } from "react-router-dom"

import { SPACING } from "../../common/spacing"

import { SettingsNav } from "./components/SettingsNav"

export const SettingsPage = () => (
  <Container maxWidth="xl" sx={{ py: SPACING.md }}>
    <Grid2 container spacing={{ xs: SPACING.md, md: SPACING.xl }}>
      <Grid2 size={{ xs: 12, md: 3, xl: 2 }}>
        <SettingsNav />
      </Grid2>
      <Grid2 size={{ xs: 12, md: 9, xl: 10 }}>
        <Outlet />
      </Grid2>
    </Grid2>
  </Container>
)
