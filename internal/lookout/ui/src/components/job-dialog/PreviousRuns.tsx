import React, { Fragment } from "react"

import { Collapse, List, ListItem, ListItemText, Paper, Table, TableBody, TableContainer } from "@material-ui/core"
import { ExpandLess, ExpandMore } from "@material-ui/icons"

import { Run } from "../../services/JobService"
import { RunDetailsRows } from "./RunDetailsRows"

import "./PreviousRuns.css"

interface SchedulingHistoryProps {
  runs: Run[]
  expandedItems: Set<string>
  onToggleExpand: (k8sId: string, isExpanded: boolean) => void
}

export function PreviousRuns(props: SchedulingHistoryProps) {
  return (
    <Fragment>
      <h3 className="previous-runs-title">Previous runs</h3>
      <div className="previous-runs">
        <List component={Paper}>
          {props.runs &&
            props.runs.map((run) => (
              <Fragment key={run.k8sId}>
                <ListItem
                  key={run.k8sId + "-0"}
                  button
                  onClick={() => {
                    if (props.expandedItems.has(run.k8sId)) {
                      props.onToggleExpand(run.k8sId, false)
                    } else {
                      props.onToggleExpand(run.k8sId, true)
                    }
                  }}
                >
                  <ListItemText>{run.cluster}</ListItemText>
                  {props.expandedItems.has(run.k8sId) ? <ExpandLess /> : <ExpandMore />}
                </ListItem>
                <Collapse key={run.k8sId + "-1"} in={props.expandedItems.has(run.k8sId)} timeout="auto" unmountOnExit>
                  <div className="nested-run">
                    <TableContainer>
                      <Table>
                        <TableBody>
                          <RunDetailsRows run={run} />
                        </TableBody>
                      </Table>
                    </TableContainer>
                  </div>
                </Collapse>
              </Fragment>
            ))}
        </List>
      </div>
    </Fragment>
  )
}
