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
  jobId: "job-1",
  namespace: "team-a",
  runs: [
    { cluster: "cluster-0", runId: "run-0" },
    { cluster: "cluster-1", runId: "run-1" },
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
        Logs: "kubectl --context cluster-1 -n team-a logs -l armada_job_id=job-1,armada_job_run_id=run-1 --tail=-1",
        Exec: "kubectl --context cluster-1 -n team-a exec -it $(kubectl --context cluster-1 -n team-a get pod -l armada_job_id=job-1,armada_job_run_id=run-1 -o name) -- /bin/sh",
      })
    },
  )
})
