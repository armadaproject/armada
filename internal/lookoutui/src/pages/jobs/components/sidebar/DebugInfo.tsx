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
import { formatEventAge } from "./eventAge"

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

const EventsTable = ({ label, events, now }: { label: string; events: DebugEvent[] | undefined; now: Date }) => {
  if (!events || events.length === 0) {
    return <Typography variant="body2">No events recorded</Typography>
  }

  return (
    <CompactTable size="small" aria-label={label}>
      <TableHead>
        <TableRow>
          <NoWrapCell>Type</NoWrapCell>
          <NoWrapCell>Reason</NoWrapCell>
          <NoWrapCell>Age</NoWrapCell>
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
              <NoWrapCell title={event.timestamp}>{formatEventAge(event.timestamp, now)}</NoWrapCell>
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
          <NoWrapCell>{container.runSeconds === undefined ? "" : `${container.runSeconds}s`}</NoWrapCell>
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
    ...Object.entries(node.annotations ?? {}).map(([key, value]): [string, string] => [`Annotation ${key}`, value]),
  ])

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

const StructuredDebugInfo = ({ debugInfo, now }: { debugInfo: PodDebugInfo; now: Date }) => {
  const { pod, node } = debugInfo
  const initContainers = pod.initContainers ?? []
  const containers = pod.containers ?? []

  return (
    <Stack spacing={SPACING.md}>
      <DebugSummary debugInfo={debugInfo} />
      <EventsTable label="Pod events" events={pod.events} now={now} />
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
            <KeyValuePairTable data={nodeDetails(node)} />
            {node.conditions && node.conditions.length > 0 && <ConditionsTable conditions={node.conditions} />}
            <Typography variant="subtitle2">Node events</Typography>
            <EventsTable label="Node events" events={node.events} now={now} />
          </CollapsedAccordion>
        )}
      </div>
    </Stack>
  )
}

// Reformats the stored message so that it can be read. This only changes whitespace and does not depend on the
// shape of the message, so it keeps fields that the generated types do not know about, e.g. from a newer executor.
const formatStoredJson = (stored: string): string => {
  try {
    return JSON.stringify(JSON.parse(stored), undefined, 2)
  } catch {
    return stored
  }
}

interface DebugInfoProps {
  debugMessage: JobRunDebugMessage
  // The time that event ages are measured against. Exists so that tests can fix it.
  now?: Date
}

export const DebugInfo = ({ debugMessage, now }: DebugInfoProps) => {
  const [showRawJson, setShowRawJson] = useState(false)

  const { debugInfo, errorString } = debugMessage
  if (!debugInfo) {
    // The API found no structured debug info in the message, e.g. it was stored before debug info was JSON.
    return (
      <CodeBlock
        code={errorString ?? ""}
        language="text"
        downloadable={false}
        showLineNumbers={false}
        loading={false}
      />
    )
  }

  const storedJson = formatStoredJson(errorString ?? "")

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
        <StructuredDebugInfo debugInfo={debugInfo} now={now ?? new Date()} />
      )}
    </Stack>
  )
}
