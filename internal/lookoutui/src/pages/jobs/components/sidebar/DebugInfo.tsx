import { useState } from "react"

import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Stack,
  styled,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  ToggleButton,
  Typography,
} from "@mui/material"

import { tryFormatJson } from "../../../../common/formatJson"
import { formatDuration, TimestampFormat } from "../../../../common/formatTime"
import { SPACING } from "../../../../common/spacing"
import { CodeBlock } from "../../../../components/CodeBlock"
import { CopyIconButton } from "../../../../components/CopyIconButton"
import {
  DebugCondition,
  DebugContainer,
  DebugEvent,
  DebugNode,
  DebugPod,
  JobRunDebugMessage,
  PodDebugInfo,
} from "../../../../openapi/lookout"

import { KeyValuePairTable } from "./KeyValuePairTable"
import { summarizeDebugInfo } from "./debugSummary"

type FormatIsoTimestamp = (isoTimestamp: string | undefined, format: TimestampFormat) => string

const CompactTable = styled(Table)({
  "& th, & td": {
    verticalAlign: "top",
    paddingLeft: 6,
    paddingRight: 6,
  },
})

const MessageCell = styled(TableCell)({
  wordBreak: "break-word",
})

const NoWrapCell = styled(TableCell)({
  whiteSpace: "nowrap",
})

const WarningText = styled("span")(({ theme }) => ({
  color: theme.palette.warning.main,
  fontWeight: 600,
}))

const CollapsedAccordion = ({ title, children }: { title: string; children: React.ReactNode }) => (
  <Accordion variant="elevation" square slotProps={{ transition: { unmountOnExit: true } }}>
    <AccordionSummary>{title}</AccordionSummary>
    <AccordionDetails>
      <Stack spacing={SPACING.md}>{children}</Stack>
    </AccordionDetails>
  </Accordion>
)

const EventsTable = ({
  label,
  events,
  formatIsoTimestamp,
}: {
  label: string
  events: DebugEvent[] | undefined
  formatIsoTimestamp: FormatIsoTimestamp
}) => {
  if (!events || events.length === 0) {
    return <Typography variant="body2">No events recorded</Typography>
  }

  return (
    <CompactTable size="small" aria-label={label}>
      <TableHead>
        <TableRow>
          <NoWrapCell>Type</NoWrapCell>
          <NoWrapCell>Reason</NoWrapCell>
          <NoWrapCell>Time</NoWrapCell>
          <NoWrapCell>From</NoWrapCell>
          <TableCell>Message</TableCell>
        </TableRow>
      </TableHead>
      <TableBody>
        {events.map((event, index) => {
          const isWarning = event.type === "Warning"
          const emphasise = (value: string | undefined) => (isWarning ? <WarningText>{value}</WarningText> : value)
          return (
            <TableRow key={index}>
              <NoWrapCell>{emphasise(event.type)}</NoWrapCell>
              <NoWrapCell>{emphasise(event.reason)}</NoWrapCell>
              <NoWrapCell>{formatIsoTimestamp(event.timestamp, "compact")}</NoWrapCell>
              <NoWrapCell>{event.from}</NoWrapCell>
              <MessageCell>{event.message}</MessageCell>
            </TableRow>
          )
        })}
      </TableBody>
    </CompactTable>
  )
}

const ContainersTable = ({ label, containers }: { label: string; containers: DebugContainer[] }) => (
  <CompactTable size="small" aria-label={label}>
    <TableHead>
      <TableRow>
        <NoWrapCell>Name</NoWrapCell>
        <NoWrapCell>State</NoWrapCell>
        <NoWrapCell>Exit code</NoWrapCell>
        <NoWrapCell>Run time</NoWrapCell>
        <TableCell>Reason</TableCell>
      </TableRow>
    </TableHead>
    <TableBody>
      {containers.map((container) => (
        <TableRow key={container.name}>
          <NoWrapCell>{container.name}</NoWrapCell>
          <NoWrapCell>{container.state}</NoWrapCell>
          <NoWrapCell>{container.exitCode ?? ""}</NoWrapCell>
          <NoWrapCell>{container.runSeconds == null ? "" : formatDuration(container.runSeconds)}</NoWrapCell>
          <MessageCell>{container.reason}</MessageCell>
        </TableRow>
      ))}
    </TableBody>
  </CompactTable>
)

const ConditionsTable = ({ conditions }: { conditions: DebugCondition[] }) => (
  <CompactTable size="small" aria-label="Node conditions">
    <TableHead>
      <TableRow>
        <NoWrapCell>Type</NoWrapCell>
        <NoWrapCell>Status</NoWrapCell>
        <NoWrapCell>Reason</NoWrapCell>
        <TableCell>Message</TableCell>
      </TableRow>
    </TableHead>
    <TableBody>
      {conditions.map((condition) => (
        <TableRow key={condition.type}>
          <NoWrapCell>{condition.type}</NoWrapCell>
          <NoWrapCell>{condition.status}</NoWrapCell>
          <NoWrapCell>{condition.reason}</NoWrapCell>
          <MessageCell>{condition.message}</MessageCell>
        </TableRow>
      ))}
    </TableBody>
  </CompactTable>
)

const keyValuePairs = (entries: [string, string | number | boolean | string[] | null | undefined][]) =>
  entries
    .filter((entry): entry is [string, string | number | boolean | string[]] => {
      const value = entry[1]
      return value != null && value !== "" && !(Array.isArray(value) && value.length === 0)
    })
    .map(([key, value]) => ({ key, value: Array.isArray(value) ? value.join(", ") : String(value) }))

const podDetails = (debugInfo: PodDebugInfo, pod: DebugPod) =>
  keyValuePairs([
    ["Trigger", debugInfo.trigger],
    ["Phase", pod.phase],
    ["Reason", pod.reason],
    ["Restart policy", pod.restartPolicy],
    ["Deletion timestamp", pod.deletionTimestamp],
    ["Force terminated", pod.forceTerminated],
    ["Finalizers", pod.finalizers],
  ])

const nodeDetails = (node: DebugNode) =>
  keyValuePairs([
    ["Name", node.name],
    // The generated model calls this _exists, since "exists" is a helper in the generated runtime
    ["Exists", node._exists],
    ["Unschedulable", node.unschedulable],
    ["Ready", node.ready],
    ["Ready reason", node.readyReason],
    ...Object.entries(node.labels ?? {}).map(([key, value]): [string, string] => [`Label ${key}`, value]),
  ])

const nodeAnnotations = (node: DebugNode) =>
  Object.entries(node.annotations ?? {})
    .filter(([, value]) => value !== "")
    .map(([key, value]) => ({ key: `Annotation ${key}`, value, isAnnotation: true }))

const DebugSummary = ({ debugInfo }: { debugInfo: PodDebugInfo }) => (
  <Stack
    direction="row"
    flexWrap="wrap"
    columnGap={SPACING.md}
    rowGap={SPACING.xs}
    role="group"
    aria-label="Debug summary"
  >
    {summarizeDebugInfo(debugInfo).map(({ label, value }) => (
      <Typography key={label} variant="body2" component="div">
        <Typography variant="body2" component="span" color="text.secondary">
          {label}:{" "}
        </Typography>
        <span>{value}</span>
      </Typography>
    ))}
  </Stack>
)

const StructuredDebugInfo = ({
  debugInfo,
  formatIsoTimestamp,
}: {
  debugInfo: PodDebugInfo
  formatIsoTimestamp: FormatIsoTimestamp
}) => {
  const { pod, node } = debugInfo
  const initContainers = pod.initContainers ?? []
  const containers = pod.containers ?? []

  return (
    <Stack spacing={SPACING.md}>
      <DebugSummary debugInfo={debugInfo} />
      <EventsTable label="Pod events" events={pod.events} formatIsoTimestamp={formatIsoTimestamp} />
      <div>
        <CollapsedAccordion title="Pod">
          <KeyValuePairTable data={podDetails(debugInfo, pod)} />
        </CollapsedAccordion>
        {(initContainers.length > 0 || containers.length > 0) && (
          <CollapsedAccordion title="Containers">
            {initContainers.length > 0 && <ContainersTable label="Init containers" containers={initContainers} />}
            {containers.length > 0 && <ContainersTable label="Containers" containers={containers} />}
          </CollapsedAccordion>
        )}
        {node && (
          <CollapsedAccordion title="Node">
            <KeyValuePairTable data={[...nodeDetails(node), ...nodeAnnotations(node)]} />
            {node.conditions && node.conditions.length > 0 && <ConditionsTable conditions={node.conditions} />}
            <Typography variant="subtitle2">Node events</Typography>
            <EventsTable label="Node events" events={node.events} formatIsoTimestamp={formatIsoTimestamp} />
          </CollapsedAccordion>
        )}
      </div>
    </Stack>
  )
}

interface DebugInfoProps {
  debugMessage: JobRunDebugMessage
  // Formats event times with the user's timestamp settings, so that they read like the run's own times
  formatIsoTimestamp: FormatIsoTimestamp
}

export const DebugInfo = ({ debugMessage, formatIsoTimestamp }: DebugInfoProps) => {
  const [showRawJson, setShowRawJson] = useState(false)

  const { debugInfo } = debugMessage
  const errorString = debugMessage.errorString ?? ""
  // Reformatted without reading it, so fields that the generated types do not know about are kept
  const formattedJson = tryFormatJson(errorString)

  if (!debugInfo) {
    // The API found no structured debug info in the message. It is either text from before debug info was JSON, or
    // JSON this version of Lookout cannot structure, e.g. from an executor on a newer schema version.
    return (
      <CodeBlock
        code={formattedJson ?? errorString}
        language={formattedJson ? "json" : "text"}
        downloadable={false}
        showLineNumbers={false}
        loading={false}
      />
    )
  }

  const storedJson = formattedJson ?? errorString

  return (
    <Stack spacing={SPACING.md}>
      <Stack direction="row" alignItems="center" spacing={SPACING.sm}>
        <ToggleButton
          value="rawJson"
          size="small"
          selected={showRawJson}
          onChange={() => setShowRawJson((current) => !current)}
        >
          Raw JSON
        </ToggleButton>
        <CopyIconButton content={storedJson} size="small" />
      </Stack>
      {showRawJson ? (
        <CodeBlock code={storedJson} language="json" downloadable={false} showLineNumbers={false} loading={false} />
      ) : (
        <StructuredDebugInfo debugInfo={debugInfo} formatIsoTimestamp={formatIsoTimestamp} />
      )}
    </Stack>
  )
}
