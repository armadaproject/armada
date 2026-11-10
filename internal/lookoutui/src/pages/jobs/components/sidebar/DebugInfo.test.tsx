import { render, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { SnackbarProvider } from "notistack"

import { JobRunDebugMessage, PodDebugInfo, PodDebugInfoToJSON } from "../../../../openapi/lookout"

import { DebugInfo } from "./DebugInfo"

const now = new Date("2026-11-03T19:00:00Z")

const debugInfo: PodDebugInfo = {
  schemaVersion: 1,
  trigger: "podFailed",
  pod: {
    phase: "Pending",
    containers: [{ name: "alpine", state: "waiting", reason: "ContainerCreating" }],
    events: [
      {
        type: "Normal",
        reason: "Scheduled",
        from: "bin-packing",
        message: "Successfully assigned ns/pod-0 to node-1",
        timestamp: "2026-11-03T18:59:34Z",
      },
      {
        type: "Warning",
        reason: "FailedMount",
        from: "kubelet",
        message: 'MountVolume.SetUp failed for volume "ca-bundle"',
        timestamp: "2026-11-03T18:59:35Z",
      },
    ],
  },
  node: {
    name: "node-1",
    _exists: true,
    ready: "True",
    conditions: [{ type: "DiskPressure", status: "False", reason: "KubeletHasNoDiskPressure" }],
    events: [
      {
        type: "Normal",
        reason: "NodeReady",
        from: "kubelet",
        message: "node is ready",
        timestamp: "2026-11-03T18:00:00Z",
      },
    ],
  },
}

// What the API stores and sends back as errorString. It has a field that the schema above does not know about,
// as if the executor were newer than this UI.
const storedJson = JSON.stringify({ ...PodDebugInfoToJSON(debugInfo), futureField: 7 })

const debugMessage: JobRunDebugMessage = { errorString: storedJson, debugInfo }

const renderDebugInfo = (message: JobRunDebugMessage) =>
  render(
    <SnackbarProvider>
      <DebugInfo debugMessage={message} now={now} />
    </SnackbarProvider>,
  )

describe("DebugInfo", () => {
  it("shows pod events in a table without needing to expand anything", () => {
    const { getByRole } = renderDebugInfo(debugMessage)

    const table = getByRole("table", { name: "Pod events" })
    expect(
      within(table)
        .getAllByRole("columnheader")
        .map((header) => header.textContent),
    ).toEqual(["Type", "Reason", "Age", "From", "Message"])

    const scheduled = within(table).getByRole("row", { name: /Scheduled/ })
    expect(
      within(scheduled)
        .getAllByRole("cell")
        .map((cell) => cell.textContent),
    ).toEqual(["Normal", "Scheduled", "26s", "bin-packing", "Successfully assigned ns/pod-0 to node-1"])
    within(table).getByRole("row", { name: /FailedMount.*25s.*kubelet.*ca-bundle/ })
  })

  it("summarises the cause above the events", () => {
    const { getByRole } = renderDebugInfo(debugMessage)

    const summary = getByRole("group", { name: "Debug summary" })
    within(summary).getByText("podFailed")
    within(summary).getByText("Pending")
    within(summary).getByText("alpine waiting (ContainerCreating)")
    within(summary).getByText("1")
  })

  it("keeps the pod, containers and node sections collapsed until opened", async () => {
    const { getByRole, queryByText, findByText } = renderDebugInfo(debugMessage)

    expect(queryByText("ContainerCreating")).toBeNull()
    await userEvent.click(getByRole("button", { name: "Containers" }))
    expect(await findByText("ContainerCreating")).toBeInTheDocument()

    expect(queryByText("node is ready")).toBeNull()
    await userEvent.click(getByRole("button", { name: "Node" }))
    expect(await findByText("node is ready")).toBeInTheDocument()
    expect(await findByText("DiskPressure")).toBeInTheDocument()
    within(getByRole("row", { name: /Exists/ })).getByText("true")
  })

  describe("raw JSON", () => {
    // The code block splits each line into highlighted token spans, so match on the whole line.
    const hasLine = (line: string) => (_: string, element: Element | null) =>
      element?.tagName === "DIV" && element.textContent === line

    it("shows the stored message as it was captured, including fields this UI does not know about", async () => {
      const { getByRole, findByText } = renderDebugInfo(debugMessage)

      await userEvent.click(getByRole("button", { name: "Raw JSON" }))

      expect(await findByText(hasLine('  "schemaVersion": 1,'))).toBeInTheDocument()
      expect(await findByText(hasLine('  "futureField": 7'))).toBeInTheDocument()
      // The wire name of the field, not the name the generated TypeScript model gives it
      expect(await findByText(hasLine('    "exists": true,'))).toBeInTheDocument()
    })

    it("copies the stored JSON to the clipboard", async () => {
      const user = userEvent.setup()
      const { getByRole } = renderDebugInfo(debugMessage)

      await user.click(getByRole("button", { name: "copy" }))

      expect(await navigator.clipboard.readText()).toEqual(JSON.stringify(JSON.parse(storedJson), undefined, 2))
    })

    it("falls back to the stored text when it cannot be formatted", async () => {
      const { getByRole, findByText } = renderDebugInfo({ errorString: "not json at all", debugInfo })

      await userEvent.click(getByRole("button", { name: "Raw JSON" }))

      expect(await findByText("not json at all")).toBeInTheDocument()
    })
  })

  it("shows the message as text when the API returned no structured debug info", () => {
    const { getByText, queryByRole } = renderDebugInfo({ errorString: "legacy debug message" })

    getByText("legacy debug message")
    expect(queryByRole("table")).toBeNull()
    expect(queryByRole("group", { name: "Debug summary" })).toBeNull()
  })
})
