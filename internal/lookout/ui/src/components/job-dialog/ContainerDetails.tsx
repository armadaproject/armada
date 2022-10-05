import React from "react"

import {
  getCommandArgumentsFromJobYaml,
  getCommandFromJobYaml,
  getCpuFromJobYaml,
  getGpuFromJobYaml,
  getMemoryFromJobYaml,
  getStorageFromJobYaml,
  JobYaml,
} from "../../services/ComputeResourcesService"
import DetailRow from "./DetailRow"

import "./Details.css"

interface ContainerDetailProps {
  jobYaml: JobYaml
}

export default function ContainerDetails(props: ContainerDetailProps) {
  const cpu = getCpuFromJobYaml(props.jobYaml)
  const gpu = getGpuFromJobYaml(props.jobYaml)
  const memory = getMemoryFromJobYaml(props.jobYaml)
  const storage = getStorageFromJobYaml(props.jobYaml)
  const command = getCommandFromJobYaml(props.jobYaml)
  const commandArgs = getCommandArgumentsFromJobYaml(props.jobYaml)

  return (
    <>
      {command &&
        command.map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"Command-" + index} name={"Command-" + index} value={values} />,
        )}
      {commandArgs &&
        commandArgs.map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"Arguments-" + index} name={"Arguments-" + index} value={values} />,
        )}
      {cpu &&
        cpu.map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"CPU-" + index} name={"CPU-" + index} value={values} />,
        )}
      {gpu &&
        gpu.map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"GPU-" + index} name={"GPU-" + index} value={values} />,
        )}
      {memory &&
        memory.map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"Memory-" + index} name={"Memory-" + index} value={values} />,
        )}
      {storage &&
        storage.map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"Storage-" + index} name={"Storage-" + index} value={values} />,
        )}
    </>
  )
}
