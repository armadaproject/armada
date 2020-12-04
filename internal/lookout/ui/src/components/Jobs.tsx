import React from 'react'
import { AutoSizer, Column, InfiniteLoader, Table, } from "react-virtualized"

import { JobInfoViewModel } from "../services/JobService"
import JobTableHeader from "./JobTableHeader";

import './Jobs.css'

type JobsProps = {
  jobInfos: JobInfoViewModel[]
  canLoadMore: boolean
  queue: string
  jobSet: string
  jobStates: string[]
  newestFirst: boolean
  batchSize: number
  fetchJobs: (start: number, stop: number) => Promise<JobInfoViewModel[]>
  isLoaded: (index: number) => boolean
  onQueueChange: (queue: string, callback: () => void) => void
  onJobSetChange: (jobSet: string, callback: () => void) => void
  onJobStatesChange: (jobStates: string[], callback: () => void) => void
  onOrderChange: (newestFirst: boolean, callback: () => void) => void
  onRefresh: (callback: () => void) => void
}

export default class Jobs extends React.Component<JobsProps, {}> {
  infiniteLoader: React.RefObject<InfiniteLoader>

  constructor(props: JobsProps) {
    super(props)
    this.infiniteLoader = React.createRef()
    this.rowGetter = this.rowGetter.bind(this)
    this.resetCache = this.resetCache.bind(this)
  }

  rowGetter({ index }: { index: number }): JobInfoViewModel {
    if (!!this.props.jobInfos[index]) {
      return this.props.jobInfos[index]
    } else {
      return {
        owner: "",
        jobId: "Loading...",
        jobSet: "",
        jobState: "",
        queue: "",
        submissionTime: ""
      }
    }
  }

  resetCache() {
    this.infiniteLoader.current?.resetLoadMoreRowsCache(true)
  }

  render() {
    const rowCount = this.props.canLoadMore ? this.props.jobInfos.length + this.props.batchSize : this.props.jobInfos.length

    return (
      <div className="jobs">
        <div className="job-table-header-container">
          <JobTableHeader
            queue={this.props.queue}
            jobSet={this.props.jobSet}
            newestFirst={this.props.newestFirst}
            jobStates={this.props.jobStates}
            onQueueChange={queue => {
              this.props.onQueueChange(queue, this.resetCache)
            }}
            onJobSetChange={jobSet => {
              this.props.onJobSetChange(jobSet, this.resetCache)
            }}
            onJobStatesChange={jobStates => {
              this.props.onJobStatesChange(jobStates, this.resetCache)
            }}
            onOrderChange={newestFirst => {
              this.props.onOrderChange(newestFirst, this.resetCache)
            }}
            onRefresh={() => {
              this.props.onRefresh(this.resetCache)
            }}/>
        </div>
        <div className="job-table">
          <InfiniteLoader
            ref={this.infiniteLoader}
            minimumBatchSize={this.props.batchSize}
            isRowLoaded={({ index }) => {
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
