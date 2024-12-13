import { useEffect, useMemo, useState } from "react"

import { CircularProgress, Collapse, ListItemButton, Typography } from "@mui/material"

import styles from "./ContainerDetails.module.css"
import { KeyValuePairTable } from "./KeyValuePairTable"
import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { Job } from "../../../models/lookoutV2Models"
import { useGetJobSpec } from "../../../services/lookoutV2/useGetJobSpec"

export interface ContainerData {
  name: string
  command: string
  image: string
  args: string
  resources: Record<string, string>
}

interface ContainerDetailsProps {
  job: Job
}

const getContainerData = (container: any): ContainerData => {
  const details: ContainerData = {
    name: "",
    command: "",
    args: "",
    image: "",
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
    details.resources = (container.resources as Record<string, unknown>).limits as Record<string, string>
  }
  if (container.image) {
    details.image = container.image as string
  }
  return details
}

export const ContainerDetails = ({ job }: ContainerDetailsProps) => {
  const openSnackbar = useCustomSnackbar()

  const getJobSpecResult = useGetJobSpec(job.jobId, Boolean(job.jobId))
  useEffect(() => {
    if (getJobSpecResult.status === "error") {
      openSnackbar(`Failed to retrieve Job spec for Job with ID: ${job.jobId}: ${getJobSpecResult.error}`, "error")
    }
  }, [getJobSpecResult.status, getJobSpecResult.error])

  const containers: ContainerData[] = useMemo(() => {
    if (getJobSpecResult.status !== "success") {
      return []
    }

    const containerDetails: ContainerData[] = []
    if (getJobSpecResult.data.podSpec?.containers) {
      for (const container of getJobSpecResult.data.podSpec.containers as Record<string, unknown>[]) {
        containerDetails.push(getContainerData(container))
      }
    }
    return containerDetails
  }, [getJobSpecResult.status, getJobSpecResult.data])

  if (getJobSpecResult.status === "pending") {
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
          <div>
            <Typography>Details</Typography>
            <KeyValuePairTable
              data={[
                {
                  key: "image",
                  value: container.image,
                  allowCopy: true,
                },
              ]}
            />
          </div>
        </div>
      </Collapse>
    </>
  )
}
