import React from 'react'
import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@material-ui/core";
import { Table as VirtualizedTable, TableCellProps, TableCellRenderer } from 'react-virtualized'

import { JobSet } from "../../services/JobService";
import { Column, defaultTableCellRenderer } from "react-virtualized";
import LinkCell from "../LinkCell";

interface JobSetTableProps {
  height: number
  width: number
  jobSets: JobSet[]
  onJobSetClick: (jobSet: string, state: string) => void
}

function JobSetTable(props: JobSetTableProps) {
  const temp = <TableContainer component={Paper} style={{
    maxHeight: props.height,
    overflowY: "auto",
  }}>
    <Table aria-label="job-set-table" stickyHeader={true}>
      <TableHead>
        <TableRow>
          <TableCell>Job Set</TableCell>
          <TableCell align="right">Queued</TableCell>
          <TableCell align="right">Pending</TableCell>
          <TableCell align="right">Running</TableCell>
          <TableCell align="right">Succeeded</TableCell>
          <TableCell align="right">Failed</TableCell>
        </TableRow>
      </TableHead>
      <TableBody>
        {props.jobSets.map(q =>
          <TableRow key={q.jobSet}>
            <TableCell component="th" scope="row">
              {q.jobSet}
            </TableCell>
            <TableCell align="right">
              <div
                className={q.jobsQueued ? "link" : ""}
                onClick={q.jobsQueued ?
                  () => props.onJobSetClick(q.jobSet, "Queued") :
                  () => {
                  }}>
                {q.jobsQueued}
              </div>
            </TableCell>
            <TableCell align="right">
              <div
                className={q.jobsPending ? "link" : ""}
                onClick={q.jobsPending ?
                  () => props.onJobSetClick(q.jobSet, "Pending") :
                  () => {
                  }}>
                {q.jobsPending}
              </div>
            </TableCell>
            <TableCell align="right">
              <div
                className={q.jobsRunning ? "link" : ""}
                onClick={q.jobsRunning ?
                  () => props.onJobSetClick(q.jobSet, "Running") :
                  () => {
                  }}>
                {q.jobsRunning}
              </div>
            </TableCell>
            <TableCell align="right">
              <div
                className={q.jobsSucceeded ? "link" : ""}
                onClick={q.jobsSucceeded ?
                  () => props.onJobSetClick(q.jobSet, "Succeeded") :
                  () => {
                  }}>
                {q.jobsSucceeded}
              </div>
            </TableCell>
            <TableCell align="right">
              <div
                className={q.jobsFailed ? "link" : ""}
                onClick={q.jobsFailed ?
                  () => props.onJobSetClick(q.jobSet, "Failed") :
                  () => {
                  }}>
                {q.jobsFailed}
              </div>
            </TableCell>
          </TableRow>
        )}
      </TableBody>
    </Table>
  </TableContainer>
  return (
    <div style={{
      height: props.height,
      width: props.width,
    }}>
      {temp}
    </div>
  )
}

const numberCellStyle: React.CSSProperties = {
  textAlign: "right",
  paddingRight: "1em",
}

function cellRendererForState(cellProps: TableCellProps, onJobSetClick: (jobSet: string, state: string) => void, state: string) {
  if (cellProps.cellData) {
    return <LinkCell onClick={() =>
      onJobSetClick((cellProps.rowData as JobSet).jobSet, state)} {...cellProps}/>
  }
  return defaultTableCellRenderer(cellProps)
}

export default function JobSetTable2(props: JobSetTableProps) {
  return (
    <div style={{
      height: props.height,
      width: props.width,
    }}>
      <VirtualizedTable
        rowGetter={({ index }) => props.jobSets[index]}
        rowCount={props.jobSets.length}
        rowHeight={40}
        headerHeight={40}
        height={props.height}
        width={props.width}
        headerStyle={{textAlign: "center"}}>
        <Column dataKey="jobSet" width={0.25 * props.width} label="Job Set"/>
        <Column
          dataKey="jobsQueued"
          width={0.15 * props.width}
          label="Queued"
          style={numberCellStyle}
          cellRenderer={(cellProps) =>
            cellRendererForState(cellProps, props.onJobSetClick, "Queued")}/>
        <Column
          dataKey="jobsPending"
          width={0.15 * props.width}
          label="Pending"
          style={numberCellStyle}
          cellRenderer={(cellProps) =>
            cellRendererForState(cellProps, props.onJobSetClick, "Pending")}/>
        <Column
          dataKey="jobsRunning"
          width={0.15 * props.width}
          label="Running"
          style={numberCellStyle}
          cellRenderer={(cellProps) =>
            cellRendererForState(cellProps, props.onJobSetClick, "Running")}/>
        <Column
          dataKey="jobsSucceeded"
          width={0.15 * props.width}
          label="Succeeded"
          style={numberCellStyle}
          cellRenderer={(cellProps) =>
            cellRendererForState(cellProps, props.onJobSetClick, "Succeeded")}/>
        <Column
          dataKey="jobsFailed"
          width={0.15 * props.width}
          label="Failed"
          style={numberCellStyle}
          cellRenderer={(cellProps) =>
            cellRendererForState(cellProps, props.onJobSetClick, "Failed")}/>
      </VirtualizedTable>
    </div>
  )
}
