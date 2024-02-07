import React, { useCallback } from "react"

import { ContentCopy } from "@mui/icons-material"
import { IconButton, Link } from "@mui/material"
import { template, templateSettings } from "lodash"
import { Job } from "models/lookoutV2Models"
import validator from "validator"

import styles from "./SidebarTabJobCommands.module.css"
import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { CommandSpec } from "../../../utils"

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
  const openSnackbar = useCustomSnackbar()

  const copyCommand = useCallback(async (commandText: string) => {
    await navigator.clipboard.writeText(commandText)
    openSnackbar("Copied command to clipboard!", "info", {
      autoHideDuration: 3000,
      preventDuplicate: true,
    })
  }, [])

  return (
    <div style={{ width: "100%", height: "100%" }}>
      {job.runs?.length ? (
        <div>
          {commandSpecs.map((c, i) => (
            <>
              <div>
                {i > 0 ? <br /> : undefined}
                {c.name}
                <IconButton size="small" title="Copy to clipboard" onClick={() => copyCommand(getCommandText(job, c))}>
                  <ContentCopy />
                </IconButton>
              </div>
              {validator.isURL(getCommandText(job, c)) ? (
                <Link href={getCommandText(job, c)} target="_blank">
                  <div>{getCommandText(job, c)}</div>
                </Link>
              ) : (
                <div className={styles.commandsText}>{getCommandText(job, c)}</div>
              )}
            </>
          ))}
        </div>
      ) : undefined}
    </div>
  )
}
