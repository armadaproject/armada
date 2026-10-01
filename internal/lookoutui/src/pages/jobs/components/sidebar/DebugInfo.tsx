import { useState } from "react"

import { Accordion, AccordionDetails, AccordionSummary, Stack, ToggleButton, ToggleButtonGroup } from "@mui/material"
import { ErrorBoundary } from "react-error-boundary"

import { SPACING } from "../../../../common/spacing"
import { CodeBlock } from "../../../../components/CodeBlock"

import { KeyValuePairTable } from "./KeyValuePairTable"

interface ContainerInfo {
  name: string
  restartPolicy?: string
  state: string
  exitCode?: number
  reason?: string
  runSeconds?: number
}

interface ConditionInfo {
  type: string
  status: string
  reason?: string
  message?: string
}

interface EventInfo {
  type?: string
  reason?: string
  from?: string
  message?: string
  timestamp?: string
}

interface PodInfo {
  phase?: string
  reason?: string
  restartPolicy?: string
  deletionTimestamp?: string
  forceTerminated?: boolean
  finalizers?: string[]
  initContainers?: ContainerInfo[]
  containers?: ContainerInfo[]
  events?: EventInfo[]
}

interface NodeInfo {
  name: string
  exists?: boolean
  unschedulable?: boolean
  ready?: string
  readyReason?: string
  labels?: Record<string, string>
  annotations?: Record<string, string>
  conditions?: ConditionInfo[]
  events?: EventInfo[]
}

interface DebugInfoPayload {
  schemaVersion: number
  trigger: string
  pod: PodInfo
  node?: NodeInfo
}

const toRows = (values: Record<string, string | number | boolean | undefined>) =>
  Object.entries(values)
    .filter(([, value]) => value !== undefined && value !== "")
    .map(([key, value]) => ({ key, value: value!.toString() }))

const ContainerDetails = ({ container }: { container: ContainerInfo }) => (
  <Accordion variant="elevation" square>
    <AccordionSummary>Container: {container.name}</AccordionSummary>
    <AccordionDetails>
      <KeyValuePairTable
        data={toRows({
          Name: container.name,
          State: container.state,
          "Restart policy": container.restartPolicy,
          "Exit code": container.exitCode,
          Reason: container.reason,
          "Run time (seconds)": container.runSeconds,
        })}
      />
    </AccordionDetails>
  </Accordion>
)

const EventDetails = ({ event, index }: { event: EventInfo; index: number }) => (
  <Accordion variant="elevation" square>
    <AccordionSummary>Event {index + 1}</AccordionSummary>
    <AccordionDetails>
      <KeyValuePairTable
        data={toRows({
          Type: event.type,
          Reason: event.reason,
          From: event.from,
          Message: event.message,
          Timestamp: event.timestamp,
        })}
      />
    </AccordionDetails>
  </Accordion>
)

const Events = ({ title, events }: { title: string; events?: EventInfo[] }) => {
  if (!events?.length) {
    return null
  }
  return (
    <Accordion variant="elevation" square>
      <AccordionSummary>{title}</AccordionSummary>
      <AccordionDetails>
        {events.map((event, index) => (
          <EventDetails key={`${event.timestamp}-${event.reason}-${index}`} event={event} index={index} />
        ))}
      </AccordionDetails>
    </Accordion>
  )
}

const DebugInfoContent = ({ payload }: { payload: DebugInfoPayload }) => {
  const pod = payload.pod
  const node = payload.node

  return (
    <>
      <KeyValuePairTable data={toRows({ "Schema version": payload.schemaVersion, Trigger: payload.trigger })} />
      <Accordion variant="elevation" square>
        <AccordionSummary>Pod</AccordionSummary>
        <AccordionDetails>
          <KeyValuePairTable
            data={toRows({
              Phase: pod.phase,
              Reason: pod.reason,
              "Restart policy": pod.restartPolicy,
              "Deletion timestamp": pod.deletionTimestamp,
              "Force terminated": pod.forceTerminated,
              Finalizers: pod.finalizers?.join(", "),
            })}
          />
          {pod.initContainers?.map((container) => (
            <ContainerDetails key={container.name} container={container} />
          ))}
          {pod.containers?.map((container) => (
            <ContainerDetails key={container.name} container={container} />
          ))}
          <Events title="Pod events" events={pod.events} />
        </AccordionDetails>
      </Accordion>
      {node && (
        <Accordion variant="elevation" square>
          <AccordionSummary>Node</AccordionSummary>
          <AccordionDetails>
            <KeyValuePairTable
              data={toRows({
                Name: node.name,
                Exists: node.exists,
                Unschedulable: node.unschedulable,
                Ready: node.ready,
                "Ready reason": node.readyReason,
              })}
            />
            {node.conditions?.map((condition) => (
              <Accordion key={condition.type} variant="elevation" square>
                <AccordionSummary>Condition: {condition.type}</AccordionSummary>
                <AccordionDetails>
                  <KeyValuePairTable
                    data={toRows({
                      Type: condition.type,
                      Status: condition.status,
                      Reason: condition.reason,
                      Message: condition.message,
                    })}
                  />
                </AccordionDetails>
              </Accordion>
            ))}
            {Object.entries(node.labels ?? {}).map(([key, value]) => (
              <KeyValuePairTable key={`label-${key}`} data={[{ key: `Label: ${key}`, value }]} />
            ))}
            {Object.entries(node.annotations ?? {}).map(([key, value]) => (
              <KeyValuePairTable key={`annotation-${key}`} data={[{ key: `Annotation: ${key}`, value }]} />
            ))}
            <Events title="Node events" events={node.events} />
          </AccordionDetails>
        </Accordion>
      )}
    </>
  )
}

export const DebugInfo = ({ message }: { message: string }) => {
  let parsedMessage: unknown
  try {
    parsedMessage = JSON.parse(message)
  } catch {
    return <CodeBlock code={message} language="text" downloadable={false} showLineNumbers={false} loading={false} />
  }

  const formattedMessage = JSON.stringify(parsedMessage, undefined, 2)

  return (
    <ErrorBoundary fallbackRender={() => <JsonCodeBlock code={formattedMessage} />}>
      <StructuredDebugInfo payload={parsedMessage as DebugInfoPayload} />
    </ErrorBoundary>
  )
}

const JsonCodeBlock = ({ code }: { code: string }) => (
  <CodeBlock code={code} language="json" downloadable={false} showLineNumbers={false} loading={false} />
)

const StructuredDebugInfo = ({ payload }: { payload: DebugInfoPayload }) => {
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
      {view === "details" ? (
        <DebugInfoContent payload={payload} />
      ) : (
        <CodeBlock
          code={JSON.stringify(payload, undefined, 2)}
          language="json"
          downloadable={false}
          showLineNumbers={false}
          loading={false}
        />
      )}
    </Stack>
  )
}
