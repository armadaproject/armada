import { useEffect, useMemo } from "react"

import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Paper,
  Skeleton,
  Stack,
  styled,
  Typography,
} from "@mui/material"

import { KeyValuePairTable } from "./KeyValuePairTable"
import { SidebarTabSubheading } from "./sidebarTabContentComponents"
import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { Job } from "../../../models/lookoutModels"
import { useGetJobSpec } from "../../../services/lookout/useGetJobSpec"
import { SPACING } from "../../../styling/spacing"
import { CodeBlock } from "../../CodeBlock"

const LoadingContainerPaper = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(2),
}))

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
      <LoadingContainerPaper variant="outlined">
        <Skeleton />
      </LoadingContainerPaper>
    )
  }
  if (containers.length === 0) {
    return <Alert severity="info">No containers found.</Alert>
  }
  return (
    <div>
      {containers.map((c, i) => (
        <SingleContainerDetails key={i} container={c} openByDefault={i === 0} />
      ))}
    </div>
  )
}

const SingleContainerDetails = ({ container, openByDefault }: { container: ContainerData; openByDefault: boolean }) => {
  const entrypoint = container.command + " " + container.args
  const containerName = container.name !== "" ? container.name : "No name"

  return (
    <Accordion defaultExpanded={openByDefault}>
      <AccordionSummary>
        <SidebarTabSubheading>{containerName}</SidebarTabSubheading>
      </AccordionSummary>
      <AccordionDetails>
        <Stack spacing={SPACING.sm}>
          <div>
            <Typography>Command</Typography>
            <CodeBlock code={entrypoint} language="bash" downloadable={false} showLineNumbers={false} loading={false} />
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
        </Stack>
      </AccordionDetails>
    </Accordion>
  )
}
