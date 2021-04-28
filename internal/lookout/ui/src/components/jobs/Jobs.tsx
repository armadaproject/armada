import React from 'react'
import { AutoSizer, InfiniteLoader, Table } from "react-virtualized"

import { Job } from "../../services/JobService"
import JobTableHeader from "./JobTableHeader";
import CheckboxRow from "../CheckboxRow";
import CheckboxHeaderRow from "../CheckboxHeaderRow";
import LoadingRow from "./LoadingRow";
import { ColumnSpec } from "../../containers/JobsContainer";
import columnWrapper from "./ColumnWrapper";

import './Jobs.css'

type JobsProps = {
  jobs: Job[]
  canLoadMore: boolean
  defaultColumns: ColumnSpec<string | boolean | string[]>[]
  annotationColumns: ColumnSpec<string>[]
  selectedJobs: Map<string, Job>
  cancelJobsButtonIsEnabled: boolean
  fetchJobs: (start: number, stop: number) => Promise<Job[]>
  isLoaded: (index: number) => boolean
  onChangeColumn: (columnId: string, newValue: string | boolean | string[]) => Promise<void>
  onDisableColumn: (columnId: string, isDisabled: boolean) => Promise<void>
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
            defaultColumns={this.props.defaultColumns}
            annotationColumns={this.props.annotationColumns}
            canCancel={this.props.cancelJobsButtonIsEnabled}
            onRefresh={async () => {
              await this.props.onRefresh()
              this.resetCache()
            }}
            onCancelJobsClick={this.props.onCancelJobsClick}
            onDisableColumn={this.props.onDisableColumn}/>
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
                        <CheckboxRow
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
                      return <CheckboxHeaderRow {...tableHeaderRowProps}/>
                    }}
                    headerHeight={60}
                    height={height - 1}
                    width={width}>
                    {this.props.defaultColumns
                      .filter(c => !c.isDisabled)
                      .map(c => columnWrapper(
                        c,
                        width / this.props.defaultColumns.length,
                        newValue => {
                          this.props.onChangeColumn(c.id, newValue)
                        }))}
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
