import { Container, styled } from "@mui/material"
import { FallbackProps } from "react-error-boundary"

import { SPACING } from "../common/spacing"

import { AlertErrorFallback } from "./AlertErrorFallback"

const StyledContainer = styled(Container)(({ theme }) => ({
  marginTop: theme.spacing(SPACING.lg),
  marginBottom: theme.spacing(SPACING.lg),
}))

export const AlertInPageContainerErrorFallback = (props: FallbackProps) => (
  <StyledContainer maxWidth="md">
    <div>
      <AlertErrorFallback {...props} />
    </div>
  </StyledContainer>
)
