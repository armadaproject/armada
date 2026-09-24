import { expect, test } from "vitest"

import { TimestampFormat } from "../../../../common/formatTime"
import { JobRunState } from "../../../../models/lookoutModels"

import { makeKeyValuePairsData } from "./JobRunDetails"

test("floors runtime when a run finishes before it starts", () => {
  const formatIsoTimestamp = (timestamp: string | undefined, _format: TimestampFormat) => timestamp ?? ""

  const rows = makeKeyValuePairsData(formatIsoTimestamp, {
    runId: "run-1",
    cluster: "cluster-1",
    jobRunState: JobRunState.RunSucceeded,
    started: "2026-01-01T00:01:00Z",
    finished: "2026-01-01T00:00:00Z",
  })

  expect(rows).toContainEqual({ key: "Runtime", value: "0s" })
})
