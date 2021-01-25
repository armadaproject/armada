import React from 'react'
import { AutoSizer, Column, InfiniteLoader, Table } from "react-virtualized"

import { CancelJobsResult, Job } from "../services/JobService"
import JobTableHeader from "./JobTableHeader";
import JobRow from "./JobRow";
import HeaderRow from "./HeaderRow";
import LoadingRow from "./LoadingRow";
import JobIdCell from "./JobIdCell";
import CancelJobsModal from "./CancelJobsModal";
import { CancelJobsRequestStatus, ModalState } from "../containers/JobsContainer";

import './Jobs.css'

type JobsProps = {
  jobs: Job[]
  canLoadMore: boolean
  queue: string
  jobSet: string
  jobStates: string[]
  newestFirst: boolean
  selectedJobs: Map<string, Job>
  cancellableJobs: Job[]
  modalState: ModalState
  cancelJobsResult: CancelJobsResult
  cancelJobsRequestStatus: CancelJobsRequestStatus
  cancelJobsButtonIsEnabled: boolean
  fetchJobs: (start: number, stop: number) => Promise<Job[]>
  isLoaded: (index: number) => boolean
  onQueueChange: (queue: string) => Promise<void>
  onJobSetChange: (jobSet: string) => Promise<void>
  onJobStatesChange: (jobStates: string[]) => Promise<void>
  onOrderChange: (newestFirst: boolean) => Promise<void>
  onRefresh: () => Promise<void>
  onSelectJob: (job: Job, selected: boolean) => Promise<void>
  onSetModalState: (modal: ModalState) => void
  onCancelJobs: () => void
  onJobIdClick: (jobId: string) => void
}

export default class Jobs extends React.Component<JobsProps, {}> {
  infiniteLoader: React.RefObject<InfiniteLoader>

  constructor(props: JobsProps) {
    super(props)
    this.infiniteLoader = React.createRef()
    this.rowGetter = this.rowGetter.bind(this)
    this.resetCache = this.resetCache.bind(this)
  }

  rowGetter({ index }: { index: number }): Job {
    if (!!this.props.jobs[index]) {
      return this.props.jobs[index]
    } else {
      return {
        owner: "",
        jobId: "Loading",
        jobSet: "",
        priority: 0,
        jobState: "",
        queue: "",
        submissionTime: "",
        runs: [],
      }
    }
  }

  resetCache() {
    this.infiniteLoader.current?.resetLoadMoreRowsCache(true)
  }

  render() {
    const rowCount = this.props.canLoadMore ? this.props.jobs.length + 1 : this.props.jobs.length

    return (
      <div className="jobs">
        <div className="job-table-header-container">
          <JobTableHeader
            queue={this.props.queue}
            jobSet={this.props.jobSet}
            newestFirst={this.props.newestFirst}
            jobStates={this.props.jobStates}
            canCancel={this.props.cancelJobsButtonIsEnabled}
            onQueueChange={async queue => {
              await this.props.onQueueChange(queue)
              this.resetCache()
            }}
            onJobSetChange={async jobSet => {
              await this.props.onJobSetChange(jobSet)
              this.resetCache()
            }}
            onJobStatesChange={async jobStates => {
              await this.props.onJobStatesChange(jobStates)
              this.resetCache()
            }}
            onOrderChange={async newestFirst => {
              await this.props.onOrderChange(newestFirst)
              this.resetCache()
            }}
            onRefresh={async () => {
              await this.props.onRefresh()
              this.resetCache()
            }}
            onCancelJobsClick={() => {
              this.props.onSetModalState("CancelJobs")
            }}/>
        </div>
        <CancelJobsModal
          currentOpenModal={this.props.modalState}
          jobsToCancel={this.props.cancellableJobs}
          cancelJobsResult={this.props.cancelJobsResult}
          cancelJobsRequestStatus={this.props.cancelJobsRequestStatus}
          onCancelJobs={this.props.onCancelJobs}
          onClose={() => this.props.onSetModalState("None")}/>
        <div className="job-table">
          <InfiniteLoader
            ref={this.infiniteLoader}
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
                    rowRenderer={(tableRowProps) => {
                      if (tableRowProps.rowData.jobId === "Loading") {
                        return <LoadingRow {...tableRowProps} />
                      }

                      let selected = false
                      if (this.props.selectedJobs.has(tableRowProps.rowData.jobId)) {
                        selected = true
                      }
                      return (
                        <JobRow
                          isChecked={selected}
                          onChangeChecked={async (selected) => {
                            await this.props.onSelectJob(tableRowProps.rowData, selected)
                            this.infiniteLoader.current?.forceUpdate()
                          }}
                          tableKey={tableRowProps.key}
                          {...tableRowProps} />
                      )
                    }}
                    headerRowRenderer={(tableHeaderRowProps) => {
                      return <HeaderRow {...tableHeaderRowProps}/>
                    }}
                    headerHeight={40}
                    height={height}
                    width={width}>
                    <Column
                      dataKey="jobId"
                      width={0.2 * width}
                      label="Id"
                      cellRenderer={(cellProps) => (
                        <JobIdCell onClick={() => this.props.onJobIdClick(cellProps.cellData)} {...cellProps} />
                      )} />
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
