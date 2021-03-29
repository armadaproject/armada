import React from 'react'
import { AutoSizer, Column, InfiniteLoader, Table } from "react-virtualized"

import { Job } from "../../services/JobService"
import JobTableHeader from "./JobTableHeader";
import JobRow from "./JobRow";
import HeaderRow from "./HeaderRow";
import LoadingRow from "./LoadingRow";
import LinkCell from "../LinkCell";

import './Jobs.css'
import SearchHeaderCell from "./SearchHeaderCell";
import JobStatesHeaderCell from "./JobStatesHeaderCell";
import SubmissionTimeHeaderCell from "./SubmissionTimeHeaderCell";

type JobsProps = {
  jobs: Job[]
  canLoadMore: boolean
  queue: string
  jobSet: string
  jobStates: string[]
  newestFirst: boolean
  jobId: string
  owner: string
  selectedJobs: Map<string, Job>
  cancelJobsButtonIsEnabled: boolean
  fetchJobs: (start: number, stop: number) => Promise<Job[]>
  isLoaded: (index: number) => boolean
  onQueueChange: (queue: string) => Promise<void>
  onJobSetChange: (jobSet: string) => Promise<void>
  onJobStatesChange: (jobStates: string[]) => Promise<void>
  onOrderChange: (newestFirst: boolean) => Promise<void>
  onJobIdChange: (jobId: string) => Promise<void>
  onOwnerChange: (owner: string) => Promise<void>
  onRefresh: () => Promise<void>
  onSelectJob: (job: Job, selected: boolean) => Promise<void>
  onCancelJobsClick: () => void
  onJobIdClick: (jobIndex: number) => void
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
        jobYaml: "",
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
            jobId={this.props.jobId}
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
            onJobIdChange={async jobId => {
              await this.props.onJobIdChange(jobId)
              this.resetCache()
            }}
            onRefresh={async () => {
              await this.props.onRefresh()
              this.resetCache()
            }}
            onCancelJobsClick={this.props.onCancelJobsClick}/>
        </div>
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
                    headerHeight={60}
                    height={height - 1}
                    width={width}>
                    <Column
                      dataKey="queue"
                      width={width / 6}
                      label="Queue"
                      headerRenderer={headerProps => (
                        <SearchHeaderCell
                          headerLabel={"Queue"}
                          value={this.props.queue}
                          onChange={async queue => {
                            await this.props.onQueueChange(queue)
                            this.resetCache()
                          }}
                          {...headerProps}/>
                      )}/>
                    <Column
                      dataKey="jobId"
                      width={width / 6}
                      label="Id"
                      cellRenderer={(cellProps) => (
                        <LinkCell onClick={() => this.props.onJobIdClick(cellProps.rowIndex)} {...cellProps} />
                      )}
                      headerRenderer={headerProps => (
                        <SearchHeaderCell
                          headerLabel={"Id"}
                          value={this.props.jobId}
                          onChange={async jobId => {
                            await this.props.onJobIdChange(jobId)
                            this.resetCache()
                          }}
                          {...headerProps}/>
                      )}/>
                    <Column
                      dataKey="owner"
                      width={width / 6}
                      label="Owner"
                      headerRenderer={headerProps => (
                        <SearchHeaderCell
                          headerLabel={"Owner"}
                          value={this.props.owner}
                          onChange={async owner => {
                            await this.props.onOwnerChange(owner)
                            this.resetCache()
                          }}
                          {...headerProps}/>
                      )}/>
                    <Column
                      dataKey="jobSet"
                      width={width / 6}
                      label="Job Set"
                      headerRenderer={headerProps => (
                        <SearchHeaderCell
                          headerLabel={"Job Set"}
                          value={this.props.jobSet}
                          onChange={async jobSet => {
                            await this.props.onJobSetChange(jobSet)
                            this.resetCache()
                          }}
                          {...headerProps}/>
                      )}/>
                    <Column
                      dataKey="submissionTime"
                      width={width / 6}
                      label="Submission Time"
                      headerRenderer={headerProps => (
                        <SubmissionTimeHeaderCell
                          newestFirst={this.props.newestFirst}
                          onOrderChange={async newestFirst => {
                            await this.props.onOrderChange(newestFirst)
                            this.resetCache()
                          }}
                          {...headerProps}/>
                      )}/>
                    <Column
                      dataKey="jobState"
                      width={width / 6}
                      label="State"
                      headerRenderer={headerProps => (
                        <JobStatesHeaderCell
                          jobStates={this.props.jobStates}
                          onJobStatesChange={async jobStates => {
                            await this.props.onJobStatesChange(jobStates)
                            this.resetCache()
                          }}
                          {...headerProps}/>
                      )}/>
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
