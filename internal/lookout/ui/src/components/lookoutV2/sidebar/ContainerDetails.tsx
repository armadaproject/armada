import { useMemo, useState } from "react"

import { CircularProgress, ListItemButton, Typography, Collapse } from "@mui/material"

import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { useJobSpec } from "../../../hooks/useJobSpec"
import { Job } from "../../../models/lookoutV2Models"
import { IGetJobSpecService } from "../../../services/lookoutV2/GetJobSpecService"
import styles from "./ContainerDetails.module.css"
import { KeyValuePairTable } from "./KeyValuePairTable"

export interface ContainerData {
  name: string
  command: string
  args: string
  resources: Record<string, string>
}

interface ContainerDetailsProps {
  job: Job
  jobSpecService: IGetJobSpecService
}

const getContainerData = (container: any): ContainerData => {
  const details: ContainerData = {
    name: "",
    command: "",
    args: "",
    resources: {},
  }
  if (container.name) {
    details.name = container.name as string
  }
  if (container.command) {
    details.command = (container.command as string[]).join(" ")
  }
  if (container.args) {
    details.args = (container.args as string[]).join(" ")
  }
  if (container.resources && (container.resources as Record<string, unknown>).limits) {
    const limits = (container.resources as Record<string, unknown>).limits as Record<string, string>
    details.resources = limits
  }
  return details
}

export const ContainerDetails = ({ job, jobSpecService }: ContainerDetailsProps) => {
  const openSnackbar = useCustomSnackbar()
  const jobSpecState = useJobSpec(job, jobSpecService, openSnackbar)

  const containers: ContainerData[] = useMemo(() => {
    if (jobSpecState.loadState === "Loading" || jobSpecState.jobSpec === undefined) {
      return []
    }
    const containerDetails: ContainerData[] = []
    if (jobSpecState.jobSpec.podSpec && jobSpecState.jobSpec.podSpec.containers) {
      for (const container of jobSpecState.jobSpec.podSpec.containers as Record<string, unknown>[]) {
        containerDetails.push(getContainerData(container))
      }
    }
    return containerDetails
  }, [jobSpecState])

  if (jobSpecState.loadState === "Loading") {
    return (
      <div className={styles.container + " " + styles.centerContent}>
        <CircularProgress />
      </div>
    )
  }
  if (containers.length === 0) {
    return <>No containers found.</>
  }
  return (
    <div className={styles.container}>
      <Typography>Containers</Typography>
      {containers.map((c, i) => (
        <SingleContainerDetails key={i} container={c} openByDefault={i === 0} />
      ))}
    </div>
  )
}

const SingleContainerDetails = ({ container, openByDefault }: { container: ContainerData; openByDefault: boolean }) => {
  const [open, setOpen] = useState<boolean>(openByDefault)
  const entrypoint = container.command + " " + container.args

  const handleClick = () => {
    setOpen(!open)
  }

  const containerName = container.name !== "" ? container.name : "No name"

  return (
    <>
      <ListItemButton onClick={handleClick}>{containerName}</ListItemButton>
      <Collapse in={open}>
        <div className={styles.singleContainer}>
          <div className={styles.commandContainer}>
            <Typography>Command</Typography>
            <div className={styles.command}>{entrypoint}</div>
          </div>
          <div>
            <Typography>Resources</Typography>
            <KeyValuePairTable
              data={Object.entries(container.resources).map(([key, value]) => ({
                key: key,
                value: value,
              }))}
            />
          </div>
        </div>
      </Collapse>
    </>
  )
}
