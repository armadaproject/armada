import React, { Fragment } from "react"

import { Table, TableBody, TableContainer, Accordion, AccordionSummary, AccordionDetails } from "@material-ui/core"
import { ExpandMore } from "@material-ui/icons"

import DetailRow from "./DetailRow"
import { getContainerInfoFromYaml } from "../../services/ComputeResourcesService"

import "./Details.css"

interface ContainerDetailProps {
  jobYaml: string
}

export default function ContainerDetails(props: ContainerDetailProps) {
  const containerInfo = getContainerInfoFromYaml(props.jobYaml)
  return (
    <>
      <div className="details-yaml-container">
        <Accordion>
          <AccordionSummary expandIcon={<ExpandMore />}>
            <h3>Container Details</h3>
          </AccordionSummary>
          <AccordionDetails>
            <TableContainer>
              <Table>
                <TableBody>
                  {containerInfo?.containerInfo &&
                    containerInfo?.containerInfo.map((container, index) => (
                      <Fragment key={(container.name + "-", index)}>
                        {container.name && <DetailRow key={"Name-" + index} name={"Name"} value={container.name} />}
                        {container.command && (
                          <DetailRow key={"Command-" + index} name={"Command"} value={container.command} />
                        )}
                        {container.args && (
                          <DetailRow key={"Argument-" + index} name={"Arguments"} value={container.args} />
                        )}
                        {container.resources.limits.cpu && (
                          <DetailRow key={"CPU-" + index} name={"CPU"} value={container.resources.limits.cpu} />
                        )}
                        {container.resources.limits.memory && (
                          <DetailRow
                            key={"Memory-" + index}
                            name={"Memory"}
                            value={container.resources.limits.memory}
                          />
                        )}
                        {container.resources.limits["nvidia.com/gpu"] && (
                          <DetailRow
                            key={"GPU-" + index}
                            name={"GPU" + index}
                            value={container.resources.limits["nvidia.com/gpu"]}
                          />
                        )}
                        {container.resources.limits["ephemeral-storage"] && (
                          <DetailRow
                            key={"Ephermeral-Storage-" + index}
                            name={"Ephermeral-Storage" + index}
                            value={container.resources.limits["ephemeral-storage"]}
                          />
                        )}
                      </Fragment>
                    ))}
                </TableBody>
              </Table>
            </TableContainer>
          </AccordionDetails>
        </Accordion>
      </div>
    </>
  )
}
