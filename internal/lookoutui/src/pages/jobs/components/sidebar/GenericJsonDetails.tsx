import { Fragment } from "react"

import { Accordion, AccordionDetails, AccordionSummary, Divider } from "@mui/material"

import { KeyValuePairTable } from "./KeyValuePairTable"

export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue }

const isScalar = (value: JsonValue): value is string | number | boolean | null =>
  value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean"

const formatScalar = (value: string | number | boolean | null) => (value === null ? "null" : value.toString())

export const GenericJsonDetails = ({ value }: { value: JsonValue }) => {
  if (isScalar(value)) {
    return <KeyValuePairTable data={[{ key: "Value", value: formatScalar(value) }]} />
  }

  if (Array.isArray(value)) {
    return (
      <>
        {value.map((entry, index) => (
          <Fragment key={index}>
            {index > 0 && <Divider />}
            <GenericJsonDetails value={entry} />
          </Fragment>
        ))}
      </>
    )
  }

  const entries: [string, JsonValue][] = Object.entries(value)
  const scalarEntries = entries.filter((entry): entry is [string, string | number | boolean | null] =>
    isScalar(entry[1]),
  )
  const nestedEntries = entries.filter(([, entry]) => !isScalar(entry))

  return (
    <>
      {scalarEntries.length > 0 && (
        <KeyValuePairTable data={scalarEntries.map(([key, entry]) => ({ key, value: formatScalar(entry) }))} />
      )}
      {nestedEntries.map(([key, entry]) => (
        <Accordion key={key} variant="elevation" square>
          <AccordionSummary>{key}</AccordionSummary>
          <AccordionDetails>
            <GenericJsonDetails value={entry} />
          </AccordionDetails>
        </Accordion>
      ))}
    </>
  )
}
