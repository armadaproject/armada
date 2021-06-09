import React from "react"

import { AutoSizer, InfiniteLoader, Table } from "react-virtualized"

import { ColumnSpec } from "../../containers/JobsContainer"
import { Job } from "../../services/JobService"
import CheckboxHeaderRow from "../CheckboxHeaderRow"
import CheckboxRow from "../CheckboxRow"
import JobTableHeader from "./JobTableHeader"
import LoadingRow from "./LoadingRow"
import columnWrapper from "./columnWrapper"

import "./Jobs.css"

type JobsProps = {
  jobs: Job[]
  canLoadMore: boolean
  defaultColumns: ColumnSpec<string | boolean | string[]>[]
  annotationColumns: ColumnSpec<string>[]
  selectedJobs: Map<string, Job>
  cancelJobsButtonIsEnabled: boolean
  fetchJobs: (start: number, stop: number) => Promise<Job[]>
  isLoaded: (index: number) => boolean
  onChangeColumnValue: (columnId: string, newValue: string | boolean | string[]) => void
  onDisableColumn: (columnId: string, isDisabled: boolean) => void
  onDeleteColumn: (columnId: string) => void
  onAddColumn: () => void
  onChangeAnnotationColumnKey: (columnId: string, newKey: string) => void
  onRefresh: () => void
  onSelectJob: (job: Job, selected: boolean) => void
  onShiftSelect: (index: number, selected: boolean) => void
  onDeselectAllClick: () => void
  onCancelJobsClick: () => void
  onJobIdClick: (jobIndex: number) => void
  resetRefresh: () => void
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
        queue: "",
        submissionTime: "",
        runs: [],
        jobYaml: "",
        annotations: {},
      }
    }
  }

  resetCache() {
    this.infiniteLoader.current?.resetLoadMoreRowsCache(true)
  }

  render() {
    this.resetCache()

    const rowCount = this.props.canLoadMore ? this.props.jobs.length + 1 : this.props.jobs.length
    return (
      <div className="jobs">
        <div className="job-table-header-container">
          <JobTableHeader
            defaultColumns={this.props.defaultColumns}
            annotationColumns={this.props.annotationColumns}
            canCancel={this.props.cancelJobsButtonIsEnabled}
            onRefresh={this.props.onRefresh}
            onCancelJobsClick={this.props.onCancelJobsClick}
            onDisableColumn={this.props.onDisableColumn}
            onDeleteColumn={this.props.onDeleteColumn}
            onAddColumn={this.props.onAddColumn}
            onChangeAnnotationColumnKey={this.props.onChangeAnnotationColumnKey}
          />
        </div>
        <div className="job-table">
          <InfiniteLoader
            ref={this.infiniteLoader}
            isRowLoaded={({ index }) => {
              return this.props.isLoaded(index)
            }}
            loadMoreRows={({ startIndex, stopIndex }) => {
              return this.props.fetchJobs(startIndex, stopIndex + 1) // stopIndex is inclusive
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
                            onChangeChecked={(selected) => this.props.onSelectJob(tableRowProps.rowData, selected)}
                            onChangeCheckedShift={(selected) => this.props.onShiftSelect(tableRowProps.index, selected)}
                            tableKey={tableRowProps.key}
                            {...tableRowProps}
                          />
                        )
                      }}
                      headerRowRenderer={(tableHeaderRowProps) => {
                        return (
                          <CheckboxHeaderRow
                            deselectEnabled={this.props.selectedJobs.size > 0}
                            onDeselectAllClick={this.props.onDeselectAllClick}
                            {...tableHeaderRowProps}
                          />
                        )
                      }}
                      headerHeight={60}
                      height={height - 1}
                      width={width}
                    >
                      {enabledColumns.map((col) =>
                        columnWrapper(
                          col.id,
                          col,
                          width / enabledColumns.length,
                          (newValue: string | boolean | string[]) => {
                            this.props.onChangeColumnValue(col.id, newValue)
                          },
                          this.props.onJobIdClick,
                        ),
                      )}
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
