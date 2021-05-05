import React from 'react';
import {
  Checkbox, IconButton,
  Input,
  InputLabel, ListItem,
  ListItemText,
  MenuItem,
  MenuProps,
  Select,
  TextField
} from "@material-ui/core";
import { Add, Clear } from "@material-ui/icons";

import { ColumnSpec } from "../../containers/JobsContainer";

type ColumnSelectProps = {
  defaultColumns: ColumnSpec<string | boolean | string[]>[]
  annotationColumns: ColumnSpec<string>[]
  inputLabel: string
  addColumnText: string
  onDisableColumn: (columnId: string, isDisabled: boolean) => void
  onDeleteColumn: (columnId: string) => void
  onAddColumn: () => void
  onChangeAnnotationColumnKey: (columnId: string, newKey: string) => void
}

const ITEM_HEIGHT = 64
const menuProps: Partial<MenuProps> = {
  PaperProps: {
    style: {
      maxHeight: "90%",
      width: 300,
      overflowY: "auto",
    },
  },
  anchorOrigin: {
    vertical: "bottom",
    horizontal: "left",
  },
  transformOrigin: {
    vertical: "top",
    horizontal: "left",
  },
  getContentAnchorEl: null,
}

function countTotalSelected(defaultColumns: ColumnSpec<any>[], annotationColumns: ColumnSpec<any>[]): number {
  let count = 0

  for (let col of defaultColumns) {
    if (!col.isDisabled) {
      count++
    }
  }

  for (let col of annotationColumns) {
    if (!col.isDisabled) {
      count++
    }
  }

  return count
}

export default function ColumnSelect(props: ColumnSelectProps) {
  return (
    <div className="job-states-header-cell">
      <InputLabel shrink={true} id="job-table-state-select-label">Columns</InputLabel>
      <Select
        labelId="job-table-state-select-label"
        id="job-table-state-select"
        multiple
        input={<Input/>}
        value={[]}
        renderValue={() => `${countTotalSelected(props.defaultColumns, props.annotationColumns)} selected`}
        displayEmpty={true}
        MenuProps={menuProps}
        className="job-states-header-cell-select">
        {props.defaultColumns.map(col => (
          <ListItem key={col.id} value={col.name} style={{ height: ITEM_HEIGHT }}>
            <Checkbox
              checked={!col.isDisabled}
              onChange={event => props.onDisableColumn(col.id, !event.target.checked)}/>
            <ListItemText primary={col.name}/>
          </ListItem>
        ))}
        {props.annotationColumns.map(col => (
          <ListItem key={col.id} value={col.name} style={{ height: ITEM_HEIGHT }}>
            <Checkbox
              checked={!col.isDisabled}
              onChange={event => props.onDisableColumn(col.id, !event.target.checked)}/>
            <TextField
              onKeyDown={e => e.stopPropagation()} // For input to work in select menu
              label={props.inputLabel}
              value={col.name}
              onChange={event => {
                props.onChangeAnnotationColumnKey(col.id, event.target.value)
              }}/>
            <IconButton color="secondary" component="span" style={{
              marginLeft: "0.5em",
              height: ITEM_HEIGHT / 2,
              width: ITEM_HEIGHT / 2,
            }}
            onClick={() => props.onDeleteColumn(col.id)}>
              <Clear/>
            </IconButton>
          </ListItem>
        ))}
        <MenuItem style={{
            height: ITEM_HEIGHT,
          }}
          onClick={props.onAddColumn}>
          <div style={{
            paddingLeft: 8,
            paddingRight: 16,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}>
            <Add/>
          </div>
          <ListItemText secondary={props.addColumnText}/>
        </MenuItem>
      </Select>
    </div>
  )
}
