import React from 'react'
import { LookoutJobInfo } from "../openapi/models";
import { InfiniteLoader, List } from "react-virtualized";
import { stringify } from "querystring";

type JobTableComponentProps = {
  fetchJobs: (start: number, stop: number) => Promise<LookoutJobInfo[]>
  isLoaded: (index: number) => boolean
}

const list: LookoutJobInfo[] = []

type RowRenderSpec = {
  key: string
  index: number
}

function rowRenderer({ key, index }: RowRenderSpec) {
  return (
    <div
      key={key}
    >
      {list[index].job?.id}
    </div>
  )
}

export default function JobTableComponent(props: JobTableComponentProps) {
  return (
    <InfiniteLoader
      isRowLoaded={({ index }) => {
        return props.isLoaded(index)
      }}
      loadMoreRows={({ startIndex, stopIndex }) => {
        return props.fetchJobs(startIndex, stopIndex)
      }}
      rowCount={100}
    >
      {({ onRowsRendered, registerChild }) => (
        <List
          height={200}
          onRowsRendered={onRowsRendered}
          ref={registerChild}
          rowCount={100}
          rowHeight={20}
          rowRenderer={rowRenderer}
          width={300}
        />
      )}
    </InfiniteLoader>
  )
}
