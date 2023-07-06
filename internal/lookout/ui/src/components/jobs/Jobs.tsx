import React from "react"

import { AutoSizer, InfiniteLoader, Table } from "react-virtualized"

import createJobTableColumns from "./JobTableColumns"
import JobTableHeader from "./JobTableHeader"
import LoadingRow from "./LoadingRow"
import { ColumnSpec } from "../../containers/JobsContainer"
import { Job } from "../../services/JobService"
import { RequestStatus } from "../../utils"
import CheckboxHeaderRow from "../CheckboxHeaderRow"
import CheckboxRow from "../CheckboxRow"

import "./Jobs.css"

type JobsProps = {
  jobs: Job[]
  defaultColumns: ColumnSpec<string | boolean | string[]>[]
  annotationColumns: ColumnSpec<string>[]
  selectedJobs: Map<string, Job>
  autoRefresh: boolean
  cancelJobsButtonIsEnabled: boolean
  reprioritizeButtonIsEnabled: boolean
  getJobsRequestStatus: RequestStatus
  fetchJobs: (start: number, stop: number) => Promise<Job[]>
  isLoaded: (index: number) => boolean
  onChangeColumnValue: (columnId: string, newValue: string | boolean | string[]) => void
  onDisableColumn: (columnId: string, isDisabled: boolean) => void
  onDeleteColumn: (columnId: string) => void
  onAddColumn: () => void
  onChangeAnnotationColumnKey: (columnId: string, newKey: string) => void
  onRefresh: () => void
  onSelectJob: (index: number, selected: boolean) => void
  onShiftSelect: (index: number, selected: boolean) => void
  onDeselectAllClick: () => void
  onCancelJobsClick: () => void
  onReprioritizeJobsClick: () => void
  onJobIdClick: (jobIndex: number) => void
  onAutoRefreshChange: (autoRefresh: boolean) => void
  onInteract: () => void
  onRegisterResetCache: (resetCache: () => void) => void
  onClear: () => void
}

export default class Jobs extends React.Component<JobsProps, Record<string, never>> {
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
        jobStateDuration: "",
        queue: "",
        submissionTime: "",
        runs: [],
        jobYaml: "",
        annotations: {},
        namespace: "",
        containers: new Map<number, string[]>(),
      }
    }
  }

  componentDidMount() {
    this.props.onRegisterResetCache(this.resetCache)
  }

  resetCache() {
    this.infiniteLoader.current?.resetLoadMoreRowsCache(true)
  }

  render() {
    const rowCount = this.props.jobs.length
    return (
      <div className="jobs">
        <div className="job-table-header-container">
          <JobTableHeader
            defaultColumns={this.props.defaultColumns}
            annotationColumns={this.props.annotationColumns}
            autoRefresh={this.props.autoRefresh}
            canCancel={this.props.cancelJobsButtonIsEnabled}
            canReprioritize={this.props.reprioritizeButtonIsEnabled}
            isLoading={this.props.getJobsRequestStatus === "Loading"}
            onRefresh={this.props.onRefresh}
            onCancelJobsClick={this.props.onCancelJobsClick}
            onReprioritizeJobsClick={this.props.onReprioritizeJobsClick}
            onDisableColumn={this.props.onDisableColumn}
            onDeleteColumn={this.props.onDeleteColumn}
            onAddColumn={this.props.onAddColumn}
            onChangeAnnotationColumnKey={this.props.onChangeAnnotationColumnKey}
            onAutoRefreshChange={this.props.onAutoRefreshChange}
            onClear={this.props.onClear}
          />
        </div>
        <div className="job-table">
          <InfiniteLoader
            ref={this.infiniteLoader}
            isRowLoaded={({ index }) => {
              return this.props.isLoaded(index)
            }}
            loadMoreRows={async ({ startIndex, stopIndex }) => {
              await this.props.fetchJobs(startIndex, stopIndex + 1) // stopIndex is inclusive
            }}
            rowCount={rowCount}
          >
            {({ onRowsRendered, registerChild }) => (
              <AutoSizer>
                {({ height, width }) => {
                  const enabledColumns = this.props.defaultColumns
                    .concat(this.props.annotationColumns)
                    .filter((col) => !col.isDisabled)
                  return (
                    <Table
                      gridStyle={{ overflowY: "scroll" }}
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
                            onChangeChecked={(selected) => {
                              this.props.onInteract()
                              this.props.onSelectJob(tableRowProps.index, selected)
                            }}
                            onChangeCheckedShift={(selected) => {
                              this.props.onInteract()
                              this.props.onShiftSelect(tableRowProps.index, selected)
                            }}
                            tableKey={tableRowProps.key}
                            {...tableRowProps}
                          />
                        )
                      }}
                      headerRowRenderer={(tableHeaderRowProps) => {
                        const jobsAreSelected = this.props.selectedJobs.size > 0
                        return (
                          <CheckboxHeaderRow
                            checked={jobsAreSelected}
                            disabled={!jobsAreSelected}
                            onClick={() => this.props.onDeselectAllClick()}
                            {...tableHeaderRowProps}
                          />
                        )
                      }}
                      headerHeight={60}
                      height={height - 1}
                      width={width}
                      onScroll={this.props.onInteract}
                    >
                      {createJobTableColumns({
                        columns: enabledColumns,
                        totalWidth: width,
                        onChangeColumnValue: this.props.onChangeColumnValue,
                        onJobIdClick: this.props.onJobIdClick,
                      })}
                    </Table>
                  )
                }}
              </AutoSizer>
            )}
          </InfiniteLoader>
        </div>
      </div>
    )
  }
}
