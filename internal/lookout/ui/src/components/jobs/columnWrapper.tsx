import React from 'react';
import { ColumnSpec } from "../../containers/JobsContainer";
import { Column } from "react-virtualized";
import SubmissionTimeHeaderCell from "./SubmissionTimeHeaderCell";
import JobStatesHeaderCell from "./JobStatesHeaderCell";
import SearchHeaderCell from "./SearchHeaderCell";
import LinkCell from "../LinkCell";

export default function columnWrapper(
  key: string,
  columnSpec: ColumnSpec<string | boolean | string[]>,
  width: number,
  onChange: (val: string | boolean | string[]) => void,
  onJobIdClick: (jobIndex: number) => void,
) {
  let column

  switch (columnSpec.id) {
    case "submissionTime": {
      column = (
        <Column
          key={key}
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
      break;
    }
    case "jobState": {
      column = (
        <Column
          key={key}
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
      break;
    }
    case "jobId": {
      column = (
        <Column
          dataKey={columnSpec.accessor}
          width={width}
          label={columnSpec.name}
          cellRenderer={(cellProps) => (
            <LinkCell onClick={() => onJobIdClick(cellProps.rowIndex)} {...cellProps}/>
          )}
          headerRenderer={headerProps => (
            <SearchHeaderCell
              headerLabel={columnSpec.name}
              value={columnSpec.filter as string}
              onChange={onChange}
              {...headerProps}/>
          )}/>
      )
      break;
    }
    default: {
      column = (
        <Column
          key={key}
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
      break;
    }
  }

  return column
}
