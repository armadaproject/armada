// cspell:ignore lookouthc
import { readFileSync } from "fs"
import { resolve } from "path"

import { load } from "js-yaml"

import { CommandSpec } from "../../../../config"
import { Job } from "../../../../models/lookoutModels"

import { getCommandText } from "./SidebarTabJobCommands"

const readCommandSpecs = (configPath: string): CommandSpec[] => {
  // Vitest runs from the UI directory, two levels below the root of the repository.
  const path = resolve(process.cwd(), "../..", configPath)
  const config = load(readFileSync(path, "utf8")) as { uiConfig: { commandSpecs: CommandSpec[] } }
  return config.uiConfig.commandSpecs
}

const job = {
  jobId: "01hcq4x7d2k9m3n5p8r0s1t2v4",
  namespace: "team-a",
  runs: [
    { cluster: "cluster-0", runId: "run-0", podName: "armada-run-0" },
    { cluster: "cluster-1", runId: "run-1", podName: "armada-run-1" },
  ],
} as unknown as Job

describe("getCommandText", () => {
  it.each([{ configPath: "config/lookout/config.yaml" }, { configPath: "config/lookouthc/config.yaml" }])(
    "renders the shipped commands of $configPath for the latest run",
    ({ configPath }) => {
      const commands = Object.fromEntries(
        readCommandSpecs(configPath).map((spec) => [spec.name, getCommandText(job, spec)]),
      )

      expect(commands).toEqual({
        Logs: "kubectl --context cluster-1 -n team-a logs armada-run-1",
        Exec: "kubectl --context cluster-1 -n team-a exec -it armada-run-1 -- /bin/sh",
      })
    },
  )
})
