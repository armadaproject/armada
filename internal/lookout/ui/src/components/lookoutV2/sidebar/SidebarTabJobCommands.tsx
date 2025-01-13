import { OpenInNew } from "@mui/icons-material"
import { Link, Stack } from "@mui/material"
import { template, templateSettings } from "lodash"
import { Fragment } from "react/jsx-runtime"
import validator from "validator"

import { NoRunsAlert } from "./NoRunsAlert"
import { SidebarTabHeading } from "./sidebarTabContentComponents"
import { Job } from "../../../models/lookoutV2Models"
import { SPACING } from "../../../styling/spacing"
import { CommandSpec } from "../../../utils"
import { CodeBlock } from "../../CodeBlock"

export interface SidebarTabJobCommandsProps {
  job: Job
  commandSpecs: CommandSpec[]
}

function getCommandText(job: Job, commandSpec: CommandSpec): string {
  try {
    templateSettings.interpolate = /{{([\s\S]+?)}}/g
    const compiledTemplate = template(commandSpec.template)
    return compiledTemplate(job)
  } catch (error) {
    console.error("Failed to generate command text:", error)
    return "" // Return an empty string in case of failure
  }
}

export const SidebarTabJobCommands = ({ job, commandSpecs }: SidebarTabJobCommandsProps) => {
  if ((job.runs ?? []).length === 0) {
    return <NoRunsAlert jobState={job.state} />
  }

  return (
    <>
      {commandSpecs.map((commandSpec) => {
        const { name } = commandSpec
        const commandText = getCommandText(job, commandSpec)
        return (
          <Fragment key={name}>
            <SidebarTabHeading>{name}</SidebarTabHeading>
            {validator.isURL(commandText) ? (
              <Link href={commandText} target="_blank">
                <Stack direction="row" spacing={SPACING.xs} alignItems="center">
                  <div>{commandText}</div>
                  <OpenInNew fontSize="inherit" />
                </Stack>
              </Link>
            ) : (
              <CodeBlock
                code={commandText}
                language="bash"
                downloadable={false}
                showLineNumbers={false}
                loading={false}
              />
            )}
          </Fragment>
        )
      })}
    </>
  )
}
