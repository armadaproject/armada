import React, { Fragment, useState } from "react"

import { List, Paper, ListItem, Collapse, ListItemText, Table, TableBody, TableContainer } from "@material-ui/core"
import { ExpandLess } from "@material-ui/icons"

import { getContainerInfoFromYaml } from "../../services/ComputeResourcesService"
import DetailRow from "./DetailRow"

import "./Details.css"

interface ContainerDetailProps {
  jobYaml: string
}

export default function ContainerDetails(props: ContainerDetailProps) {
  const containerInfo = getContainerInfoFromYaml(props.jobYaml)
  const [visible, setVisible] = useState(false)
  return (
    <>
      <h3 className="container-details-title">Container Details</h3>
      <div className="container-details">
        <List component={Paper}>
          {containerInfo?.containerInfo &&
            containerInfo?.containerInfo.map((container, index) => (
              <Fragment key={container.name}>
                <ListItem
                  key={container.name + "-0"}
                  button
                  onClick={() => {
                    setVisible(!visible)
                  }}
                >
                  <ListItemText>{container.name}</ListItemText>
                  <ExpandLess />
                </ListItem>
                <Collapse key={container.name + "-1"} in={visible} timeout="auto" unmountOnExit>
                  <div className="nested-run">
                    <TableContainer>
                      <Table>
                        <TableBody>
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
                        </TableBody>
                      </Table>
                    </TableContainer>
                  </div>
                </Collapse>
              </Fragment>
            ))}
        </List>
      </div>
    </>
  )
}
