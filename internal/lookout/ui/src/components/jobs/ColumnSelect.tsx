import React from 'react';
import {
  Checkbox,
  Input,
  InputAdornment,
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
  onEditColumn: (columnId: string, newValue: string) => void
}

const ITEM_HEIGHT = 64
const ITEM_PADDING_TOP = 16
const menuProps: Partial<MenuProps> = {
  PaperProps: {
    style: {
      maxHeight: ITEM_HEIGHT + ITEM_PADDING_TOP,
      width: 250,
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
    if (col.isDisabled) {
      count++
    }
  }

  for (let col of annotationColumns) {
    if (col.isDisabled) {
      count++
    }
  }

  return count
}

export default function ColumnSelect(props: ColumnSelectProps) {
  if (menuProps.PaperProps && menuProps.PaperProps.style) {
    menuProps.PaperProps.style.maxHeight = ITEM_HEIGHT *
      (props.defaultColumns.length + props.annotationColumns.length + 1) +
      ITEM_PADDING_TOP
  }

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
              label={props.inputLabel}
              value={col.name}
              onChange={() => {}}
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <Clear/>
                  </InputAdornment>
                ),
              }}/>
          </ListItem>
        ))}
        <MenuItem style={{
          height: ITEM_HEIGHT,
        }}>
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
