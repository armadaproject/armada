import React from "react"
import { JobSet } from "../../services/JobService";
import { Table } from "react-virtualized";


interface JobSetRuntimesProps {
  height: number
  width: number
  jobSets: JobSet[]
}

export default function JobSetRuntimes(props: JobSetRuntimesProps) {
  return <div style={{
    height: props.height,
    width: props.width,
  }}>
    <Table
      disableHeader
      headerHeight={40}
      height={props.height}
      width={props.width}
      rowCount={props.jobSets.length}
      rowGetter={({index}) => props.jobSets[index]}
      rowHeight={40}
      rowRenderer={(rowProps) => {
        console.log(rowProps.style.height)
        console.log(rowProps.style.width)
        return <div style={rowProps.style}>Stuff</div>
      }}>
    </Table>
  </div>
}
