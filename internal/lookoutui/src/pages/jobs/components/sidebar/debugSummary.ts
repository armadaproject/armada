import { DebugContainer, PodDebugInfo } from "../../../../openapi/lookout"

export interface DebugSummaryItem {
  label: string
  value: string
}

const describeContainer = (container: DebugContainer, prefix = ""): string => {
  const exit = container.exitCode == null ? "" : `, exit ${container.exitCode}`
  const reason = container.reason ? ` (${container.reason})` : ""
  return `${prefix}${container.name} ${container.state}${exit}${reason}`
}

// The facts that explain most failures, for showing above the events so that the cause can be seen at a glance.
export const summarizeDebugInfo = ({ trigger, pod }: PodDebugInfo): DebugSummaryItem[] => {
  const summary: DebugSummaryItem[] = [{ label: "Trigger", value: trigger }]

  if (pod.phase) {
    summary.push({ label: "Pod phase", value: pod.reason ? `${pod.phase} (${pod.reason})` : pod.phase })
  }

  const containers = [
    ...(pod.initContainers ?? []).map((container) => describeContainer(container, "init ")),
    ...(pod.containers ?? []).map((container) => describeContainer(container)),
  ]
  if (containers.length > 0) {
    summary.push({ label: "Containers", value: containers.join("; ") })
  }

  const warnings = (pod.events ?? []).filter((event) => event.type === "Warning").length
  if (warnings > 0) {
    summary.push({ label: "Warnings", value: String(warnings) })
  }

  return summary
}
