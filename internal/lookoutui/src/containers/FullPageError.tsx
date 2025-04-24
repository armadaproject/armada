import { ReactNode } from "react"

import { Alert, AlertTitle, Button, Container, styled } from "@mui/material"

import { SPACING } from "../styling/spacing"

const Wrapper = styled("main")(({ theme }) => ({
  minHeight: "100vh",
  backgroundColor: theme.palette.background.default,
  display: "flex",
  justifyContent: "center",
  alignItems: "center",
}))

const ContentContainer = styled(Container)(({ theme }) => ({
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
  gap: theme.spacing(SPACING.xl),
}))

const StyledAlert = styled(Alert)({
  width: "100%",
})

const IconImg = styled("img")({
  maxHeight: 200,
})

export interface FullPageErrorProps {
  errorTitle?: string
  errorMessage: string | ReactNode
  retry?: () => void
}

export const FullPageError = ({ errorTitle, errorMessage, retry }: FullPageErrorProps) => (
  <Wrapper>
    <ContentContainer maxWidth="md">
      <div>
        <IconImg src="/logo.svg" alt="Armada Lookout" />
      </div>
      <StyledAlert
        severity="error"
        action={
          retry ? (
            <Button color="inherit" size="small" onClick={retry}>
              Retry
            </Button>
          ) : undefined
        }
      >
        <AlertTitle>{errorTitle}</AlertTitle>
        {errorMessage}
      </StyledAlert>
    </ContentContainer>
  </Wrapper>
)
