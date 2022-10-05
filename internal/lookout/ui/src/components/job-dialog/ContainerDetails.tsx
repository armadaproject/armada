import React from "react"

import { getContainerInfoFromYaml } from "../../services/ComputeResourcesService"
import DetailRow from "./DetailRow"

import "./Details.css"

interface ContainerDetailProps {
  jobYaml: string
}

export default function ContainerDetails(props: ContainerDetailProps) {
  const containerInfo = getContainerInfoFromYaml(props.jobYaml)

  return (
    <>
      {containerInfo.command &&
        containerInfo.command.map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"Command-" + index} name={"Command-" + index} value={values} />,
        )}
      {containerInfo.arguments &&
        containerInfo.arguments.map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"Arguments-" + index} name={"Arguments-" + index} value={values} />,
        )}
      {containerInfo.cpu &&
        containerInfo.cpu.map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"CPU-" + index} name={"CPU-" + index} value={values} />,
        )}
      {containerInfo.gpu &&
        containerInfo.gpu.map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"GPU-" + index} name={"GPU-" + index} value={values} />,
        )}
      {containerInfo.memory &&
        containerInfo.memory.map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"Memory-" + index} name={"Memory-" + index} value={values} />,
        )}
      {containerInfo["ephermal-storage"] &&
        containerInfo["ephermal-storage"].map(
          (values, index) =>
            values.length > 0 && <DetailRow key={"Storage-" + index} name={"Storage-" + index} value={values} />,
        )}
    </>
  )
}
