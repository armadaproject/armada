import { PodDebugInfo } from "../../../../openapi/lookout"

import { summarizeDebugInfo } from "./debugSummary"

const base: PodDebugInfo = { schemaVersion: 1, trigger: "podFailed", pod: {} }

describe("summarizeDebugInfo", () => {
  it("always reports the trigger", () => {
    expect(summarizeDebugInfo(base)).toEqual([{ label: "Trigger", value: "podFailed" }])
  })

  it("reports the pod phase, with its reason when there is one", () => {
    expect(summarizeDebugInfo({ ...base, pod: { phase: "Failed" } })).toContainEqual({
      label: "Pod phase",
      value: "Failed",
    })
    expect(summarizeDebugInfo({ ...base, pod: { phase: "Failed", reason: "Evicted" } })).toContainEqual({
      label: "Pod phase",
      value: "Failed (Evicted)",
    })
  })

  it("describes each container by its state", () => {
    const summary = summarizeDebugInfo({
      ...base,
      pod: {
        initContainers: [{ name: "setup", state: "terminated", exitCode: 0, reason: "Completed" }],
        containers: [
          { name: "main", state: "terminated", exitCode: 137, reason: "OOMKilled" },
          { name: "broken", state: "waiting", reason: "ImagePullBackOff" },
          { name: "sidecar", state: "running" },
        ],
      },
    })

    expect(summary).toContainEqual({
      label: "Containers",
      value:
        "init setup terminated, exit 0 (Completed); main terminated, exit 137 (OOMKilled); " +
        "broken waiting (ImagePullBackOff); sidecar running",
    })
  })

  it("counts warning events on the pod, and says nothing when there are none", () => {
    const warnings = (info: PodDebugInfo) => summarizeDebugInfo(info).find((item) => item.label === "Warnings")

    expect(warnings({ ...base, pod: { events: [{ type: "Normal" }] } })).toBeUndefined()
    expect(
      warnings({ ...base, pod: { events: [{ type: "Warning" }, { type: "Normal" }, { type: "Warning" }] } }),
    ).toEqual({ label: "Warnings", value: "2" })
  })

  it("does not count node events as pod warnings", () => {
    expect(
      summarizeDebugInfo({ ...base, node: { name: "node-1", events: [{ type: "Warning" }] } }).find(
        (item) => item.label === "Warnings",
      ),
    ).toBeUndefined()
  })
})
