import React, { CSSProperties } from 'react'
import { LookoutJobInfo } from "../openapi/models"
import {
  AutoSizer,
  Column,
  ColumnProps,
  InfiniteLoader,
  List,
  Table,
  TableRowProps,
  TableRowRenderer
} from "react-virtualized"

type JobTableComponentProps = {
  jobInfos: LookoutJobInfo[]
  fetchJobs: (start: number, stop: number) => Promise<LookoutJobInfo[]>
  isLoaded: (index: number) => boolean
  canLoadMore: boolean
}

const tableStyle = {
  flex: "1 1 auto",
}

export default function JobTableComponent(props: JobTableComponentProps) {
  const rowCount = props.canLoadMore ? props.jobInfos.length + 1 : props.jobInfos.length

  const rowRenderer = function rowRenderer(rowProps: TableRowProps) {
    let content = "Loading..."
    if (rowProps.index >= 0 && rowProps.index < props.jobInfos.length) {
      content = props.jobInfos[rowProps.index].job?.id || "#NO_JOB_ID#"
    }

    return (
      <div style={rowProps.style} key={rowProps.key} className={rowProps.className}>
        {content}
      </div>
    )
  }

  const rowGetter = function ({ index }: { index: number }): LookoutJobInfo {
    if (index >= 0 && index < props.jobInfos.length) {
      return props.jobInfos[index]
    } else {
      return {}
    }
  }

  return (
    <InfiniteLoader
      isRowLoaded={({ index }) => {
        return props.isLoaded(index)
      }}
      loadMoreRows={({ startIndex, stopIndex }) => {
        return props.fetchJobs(startIndex, stopIndex + 1)  // stopIndex is inclusive
      }}
      rowCount={rowCount}>
      {({ onRowsRendered, registerChild }) => (
        <div className="tableComponent" style={tableStyle}>
          <AutoSizer>
            {({ height, width }) => {
              console.log("autosizer", height, width)
              return (
                <Table
                  height={height}
                  onRowsRendered={onRowsRendered}
                  ref={registerChild}
                  rowCount={rowCount}
                  rowHeight={40}
                  rowRenderer={rowRenderer}
                  rowGetter={rowGetter}
                  headerHeight={40}
                  width={width}>
                  <Column dataKey="job.id" width={0.2 * width} label="Job Id"/>
                  <Column dataKey="job.queue" width={0.2 * width} label="Queue"/>
                </Table>
              )
            }}
          </AutoSizer>
        </div>
      )}
    </InfiniteLoader>

  )
}
