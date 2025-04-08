import { useState } from "react"

import { ExpandLess, ExpandMore } from "@mui/icons-material"
import { Alert, AlertTitle, Button, Collapse, Stack, Typography } from "@mui/material"
import { FallbackProps } from "react-error-boundary"

import { CodeBlock } from "./CodeBlock"
import { SPACING } from "../styling/spacing"

export const AlertErrorFallback = ({ error }: FallbackProps) => {
  const [detailsExpanded, setDetailsExpanded] = useState(false)

  const { errorMessage, errorDetails } =
    error instanceof Error
      ? { errorMessage: `${error.name}: ${error.message}`, errorDetails: error.stack }
      : { errorMessage: String(error), errorDetails: undefined }

  return (
    <Alert variant="filled" severity="error">
      <AlertTitle>Something went wrong</AlertTitle>
      <Typography component="p" gutterBottom>
        {errorMessage}
      </Typography>
      <Typography component="p" variant="body2" gutterBottom>
        This is an unexpected error. Please reach out to the maintainers of Armada for help with this problem.
      </Typography>
      {errorDetails && (
        <Stack spacing={SPACING.sm}>
          <Button
            color="inherit"
            fullWidth
            size="small"
            variant="outlined"
            startIcon={detailsExpanded ? <ExpandLess /> : <ExpandMore />}
            onClick={() => setDetailsExpanded((prev) => !prev)}
          >
            {detailsExpanded ? "Hide" : "Show"} error details
          </Button>
          <Collapse in={detailsExpanded}>
            <CodeBlock
              language="text"
              loading={false}
              downloadable={false}
              showLineNumbers={false}
              code={errorDetails}
            />
          </Collapse>
        </Stack>
      )}
    </Alert>
  )
}
