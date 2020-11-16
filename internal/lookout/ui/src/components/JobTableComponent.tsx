import React, { CSSProperties } from 'react'
import { LookoutJobInfo } from "../openapi/models"
import { AutoSizer, InfiniteLoader, List } from "react-virtualized"

type JobTableComponentProps = {
  jobInfos: LookoutJobInfo[]
  fetchJobs: (start: number, stop: number) => Promise<LookoutJobInfo[]>
  isLoaded: (index: number) => boolean
  canLoadMore: boolean
}

type RowRenderSpec = {
  key: string
  index: number
  style: CSSProperties
}


const tableStyle = {
  flex: 1,
}

export default function JobTableComponent(props: JobTableComponentProps) {
  const rowCount = props.canLoadMore ? props.jobInfos.length + 1 : props.jobInfos.length
  console.log(rowCount, props.canLoadMore)
  const rowRenderer = function rowRenderer({ key, index, style }: RowRenderSpec) {
    if (index >= props.jobInfos.length) {
      return <div style={style} key={key}>Loading...</div>
    }
    return (
      <div style={style} key={key}>
        {props.jobInfos[index].job?.id}
      </div>
    )
  }

  return (
    <div className="tableComponent" style={tableStyle}>
      <InfiniteLoader
        isRowLoaded={({ index }) => {
          return props.isLoaded(index)
        }}
        loadMoreRows={({ startIndex, stopIndex }) => {
          return props.fetchJobs(startIndex, stopIndex + 1)  // stopIndex is inclusive
        }}
        rowCount={rowCount}>
        {({ onRowsRendered, registerChild }) => (
          <AutoSizer>
            {({ height, width }: { height: number, width: number }) => {
              console.log("autosizer", height, width)
              return (
                <List
                  height={height}
                  onRowsRendered={onRowsRendered}
                  ref={registerChild}
                  rowCount={rowCount}
                  rowHeight={20}
                  rowRenderer={rowRenderer}
                  width={width}/>)
            }}
          </AutoSizer>
        )}
      </InfiniteLoader>
    </div>
  )
}
