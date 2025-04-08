import { Replay } from "@mui/icons-material"
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  AlertTitle,
  Button,
  Container,
  Stack,
  styled,
  Typography,
} from "@mui/material"

import { CodeBlock } from "./CodeBlock"
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

const StyledAccordion = styled(Accordion)({
  width: "100%",
})

const IconImg = styled("img")({
  maxHeight: 200,
})

export interface ErrorPageProps {
  error: any
  errorTitle: string
  errorContextMessage?: string
  retry?: () => void
}

export const ErrorPage = ({ error, errorTitle, errorContextMessage, retry }: ErrorPageProps) => {
  const { errorMessage, errorDetails } =
    error instanceof Error
      ? { errorMessage: `${error.name}: ${error.message}`, errorDetails: error.stack }
      : { errorMessage: String(error), errorDetails: undefined }

  return (
    <Wrapper>
      <ContentContainer maxWidth="md">
        <div>
          <IconImg src="/logo.svg" alt="Armada Lookout" />
        </div>
        <Stack spacing={SPACING.sm} width="100%">
          <StyledAlert
            severity="error"
            action={
              retry ? (
                <Button color="inherit" size="small" onClick={retry} endIcon={<Replay />}>
                  Retry
                </Button>
              ) : undefined
            }
          >
            <AlertTitle>{errorTitle}</AlertTitle>
            <Typography component="p" gutterBottom>
              {errorMessage}
            </Typography>
            {errorContextMessage && (
              <Typography component="p" variant="body2">
                {errorContextMessage}
              </Typography>
            )}
          </StyledAlert>
          <StyledAccordion>
            <AccordionSummary>
              <Typography>Error details</Typography>
            </AccordionSummary>
            <AccordionDetails>
              {errorDetails && (
                <CodeBlock
                  language="text"
                  loading={false}
                  downloadable={false}
                  showLineNumbers={false}
                  code={errorDetails}
                />
              )}
            </AccordionDetails>
          </StyledAccordion>
        </Stack>
      </ContentContainer>
    </Wrapper>
  )
}
