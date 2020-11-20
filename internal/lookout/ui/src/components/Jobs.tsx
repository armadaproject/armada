import React from 'react'
import {
  AutoSizer,
  Column,
  InfiniteLoader,
  Table,
} from "react-virtualized"

import { JobInfoViewModel } from "../services/JobService"
import JobTableHeader from "./JobTableHeader";

import './Jobs.css'

type JobsProps = {
  jobInfos: JobInfoViewModel[]
  fetchJobs: (start: number, stop: number) => Promise<JobInfoViewModel[]>
  isLoaded: (index: number) => boolean
  canLoadMore: boolean
  onRefresh: () => void
  queue: string
  onQueueChange: (queue: string) => void
  newestFirst: boolean
  onOrderChange: (newestFirst: boolean) => void
}

export default class Jobs extends React.Component<JobsProps, {}> {
  infiniteLoader: React.RefObject<InfiniteLoader>

  constructor(props: JobsProps) {
    super(props)
    this.infiniteLoader = React.createRef()
    this.rowGetter = this.rowGetter.bind(this)
  }

  rowGetter({ index }: { index: number }): JobInfoViewModel {
    if (!!this.props.jobInfos[index]) {
      return this.props.jobInfos[index]
    } else {
      return { owner: "", jobId: "Loading...", jobSet: "", jobState: "", queue: "", submissionTime: "" }
    }
  }

  resetCache() {
    this.infiniteLoader.current?.resetLoadMoreRowsCache()
  }

  render() {
    const rowCount = this.props.canLoadMore ? this.props.jobInfos.length + 1 : this.props.jobInfos.length
    console.log("rowCount", rowCount, this.props.canLoadMore)

    return (
      <div className="jobs">
        <div className="job-table-header-container">
          <JobTableHeader
            queue={this.props.queue}
            newestFirst={this.props.newestFirst}
            onQueueChange={queue => {
              this.resetCache()
              this.props.onQueueChange(queue)
            }}
            onOrderChange={newestFirst => {
              this.resetCache()
              this.props.onOrderChange(newestFirst)
            }}
            onRefresh={() => {
              this.resetCache()
              this.props.onRefresh()
            }}/>
        </div>
        <div className="job-table">
          <InfiniteLoader
            ref={this.infiniteLoader}
            isRowLoaded={({ index }) => {
              console.log("isRowLoaded", index, this.props.isLoaded(index))
              return this.props.isLoaded(index)
            }}
            loadMoreRows={({ startIndex, stopIndex }) => {
              return this.props.fetchJobs(startIndex, stopIndex + 1)  // stopIndex is inclusive
            }}
            rowCount={rowCount}>
            {({ onRowsRendered, registerChild }) => (
              <AutoSizer>
                {({ height, width }) => (
                  <Table
                    onRowsRendered={onRowsRendered}
                    ref={registerChild}
                    rowCount={rowCount}
                    rowHeight={40}
                    rowGetter={this.rowGetter}
                    headerHeight={40}
                    height={height}
                    width={width}>
                    <Column dataKey="jobId" width={0.2 * width} label="Id"/>
                    <Column dataKey="owner" width={0.2 * width} label="Owner"/>
                    <Column dataKey="jobSet" width={0.2 * width} label="Job Set"/>
                    <Column dataKey="submissionTime" width={0.2 * width} label="Submission Time"/>
                    <Column dataKey="jobState" width={0.2 * width} label="State"/>
                  </Table>
                )}
              </AutoSizer>
            )}
          </InfiniteLoader>
        </div>
      </div>
    )
  }
}
