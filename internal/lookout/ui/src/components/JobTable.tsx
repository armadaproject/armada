import React from 'react'
import {
  AutoSizer,
  Column,
  InfiniteLoader,
  Table,
} from "react-virtualized"
import { JobInfoViewModel } from "../services/jobs"
import './JobTable.css'

type JobTableProps = {
  jobInfos: JobInfoViewModel[]
  fetchJobs: (start: number, stop: number) => Promise<JobInfoViewModel[]>
  isLoaded: (index: number) => boolean
  canLoadMore: boolean
}

export default function JobTable(props: JobTableProps) {
  const rowCount = props.canLoadMore ? props.jobInfos.length + 1 : props.jobInfos.length

  const rowGetter = function ({ index }: { index: number }): JobInfoViewModel {
    if (index >= 0 && index < props.jobInfos.length) {
      return props.jobInfos[index]
    } else {
      return { owner: "", jobId: "", jobSet: "", jobState: "", queue: "", submissionTime: "" }
    }
  }

  return (
    <div className="container">
      <div className="actions">
        <div className="search">Search</div>
        <button className="refresh">Refresh</button>
      </div>
      <InfiniteLoader
        isRowLoaded={({ index }) => {
          return props.isLoaded(index)
        }}
        loadMoreRows={({ startIndex, stopIndex }) => {
          return props.fetchJobs(startIndex, stopIndex + 1)  // stopIndex is inclusive
        }}
        rowCount={rowCount}>
        {({ onRowsRendered, registerChild }) => (
          <div className="job-table">
            <AutoSizer>
              {({ height, width }) => (
                <Table
                  onRowsRendered={onRowsRendered}
                  ref={registerChild}
                  rowCount={rowCount}
                  rowHeight={40}
                  rowGetter={rowGetter}
                  headerHeight={40}
                  height={height}
                  width={width}>
                  <Column dataKey="jobId" width={0.2 * width} label="Job Id"/>
                  <Column dataKey="queue" width={0.1 * width} label="Queue"/>
                  <Column dataKey="owner" width={0.1 * width} label="Owner"/>
                  <Column dataKey="jobSet" width={0.2 * width} label="Job Set"/>
                  <Column dataKey="submissionTime" width={0.2 * width} label="Submission Time"
                          defaultSortDirection="ASC"/>
                  <Column dataKey="jobState" width={0.2 * width} label="Job State"/>
                </Table>
              )
              }
            </AutoSizer>
          </div>
        )}
      </InfiniteLoader>
    </div>
  )
}
