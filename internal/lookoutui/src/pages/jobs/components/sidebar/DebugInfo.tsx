import { useState } from "react"

import { Stack, ToggleButton, ToggleButtonGroup } from "@mui/material"
import { ErrorBoundary } from "react-error-boundary"

import { SPACING } from "../../../../common/spacing"
import { CodeBlock } from "../../../../components/CodeBlock"

import { GenericJsonDetails, JsonValue } from "./GenericJsonDetails"

export const DebugInfo = ({ message }: { message: string }) => {
  let parsedMessage: JsonValue
  try {
    parsedMessage = JSON.parse(message)
  } catch {
    return <CodeBlock code={message} language="text" downloadable={false} showLineNumbers={false} loading={false} />
  }

  const formattedMessage = JSON.stringify(parsedMessage, undefined, 2)
  return (
    <ErrorBoundary fallbackRender={() => <JsonCodeBlock code={formattedMessage} />}>
      <StructuredDebugInfo value={parsedMessage} formattedMessage={formattedMessage} />
    </ErrorBoundary>
  )
}

const JsonCodeBlock = ({ code }: { code: string }) => (
  <CodeBlock code={code} language="json" downloadable={false} showLineNumbers={false} loading={false} />
)

const StructuredDebugInfo = ({ value, formattedMessage }: { value: JsonValue; formattedMessage: string }) => {
  const [view, setView] = useState<"details" | "json">("details")

  return (
    <Stack spacing={SPACING.sm}>
      <ToggleButtonGroup
        color="primary"
        value={view}
        exclusive
        aria-label="Debug view"
        onChange={(_, nextView: "details" | "json" | null) => nextView && setView(nextView)}
        size="small"
      >
        <ToggleButton value="details">Details</ToggleButton>
        <ToggleButton value="json">JSON</ToggleButton>
      </ToggleButtonGroup>
      {view === "details" ? <GenericJsonDetails value={value} /> : <JsonCodeBlock code={formattedMessage} />}
    </Stack>
  )
}
