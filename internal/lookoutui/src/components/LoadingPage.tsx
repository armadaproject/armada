import { Container, LinearProgress, styled, Typography } from "@mui/material"

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

const ProgressContainer = styled("div")({
  width: "100%",
})

const IconImg = styled("img")({
  maxHeight: 200,
})

export interface LoadingPageProps {
  loadingContextMessage: string
}

export const LoadingPage = ({ loadingContextMessage }: LoadingPageProps) => (
  <Wrapper>
    <ContentContainer maxWidth="md">
      <div>
        <IconImg src="/logo.svg" alt="Armada Lookout" />
      </div>
      <ProgressContainer>
        <LinearProgress />
      </ProgressContainer>
      <div>
        <Typography component="p" variant="h4" textAlign="center">
          Armada Lookout
        </Typography>
        {loadingContextMessage && (
          <Typography component="p" variant="h6" color="text.secondary" textAlign="center">
            {loadingContextMessage}
          </Typography>
        )}
      </div>
    </ContentContainer>
  </Wrapper>
)
