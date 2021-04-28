import React from 'react';
import { ColumnSpec } from "../../containers/JobsContainer";
import { Column } from "react-virtualized";
import SubmissionTimeHeaderCell from "./SubmissionTimeHeaderCell";
import JobStatesHeaderCell from "./JobStatesHeaderCell";
import SearchHeaderCell from "./SearchHeaderCell";

export default function columnWrapper(
  columnSpec: ColumnSpec<string | boolean | string[]>,
  width: number,
  onChange: (val: string | boolean | string[]) => void
) {
  let column

  if (columnSpec.id === "submissionTime") {
    column = (
      <Column
        dataKey={columnSpec.accessor}
        width={width}
        label={columnSpec.name}
        headerRenderer={headerProps => (
          <SubmissionTimeHeaderCell
            newestFirst={columnSpec.filter as boolean}
            onOrderChange={onChange}
            {...headerProps}/>
        )}/>
    )
  } else if (columnSpec.id === "jobState") {
    column = (
      <Column
        dataKey={columnSpec.accessor}
        width={width}
        label={columnSpec.name}
        headerRenderer={headerProps => (
          <JobStatesHeaderCell
            jobStates={columnSpec.filter as string[]}
            onJobStatesChange={onChange}
            {...headerProps}/>
        )}/>
    )
  } else {
    column = (
      <Column
        dataKey={columnSpec.accessor}
        width={width}
        label={columnSpec.name}
        headerRenderer={headerProps => (
          <SearchHeaderCell
            headerLabel={columnSpec.name}
            value={columnSpec.filter as string}
            onChange={onChange}
            {...headerProps}/>
        )}/>
    )
  }

  return column
}
