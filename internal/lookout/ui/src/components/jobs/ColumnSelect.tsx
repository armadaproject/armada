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
import { ColumnSpec } from "../../containers/JobTableColumnActions";

type ColumnSelectProps = {
  selectedColumns: Set<string>
  defaultColumns: ColumnSpec[]
  additionalColumns: ColumnSpec[]
  inputLabel: string
  addColumnText: string
  onSelect: (id: string, selected: boolean) => void
  onDeleteColumn: (col: string) => void
  onAddColumn: () => void
  onChange: (id: string, newValue: string) => void
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

export default function ColumnSelect(props: ColumnSelectProps) {
  if (menuProps.PaperProps && menuProps.PaperProps.style) {
    menuProps.PaperProps.style.maxHeight = ITEM_HEIGHT *
      (props.defaultColumns.length + props.additionalColumns.length + 1) +
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
        renderValue={() => `${props.selectedColumns.size} selected`}
        displayEmpty={true}
        MenuProps={menuProps}
        className="job-states-header-cell-select">
        {props.defaultColumns.map(col => (
          <ListItem key={col.id} value={col.name} style={{ height: ITEM_HEIGHT }}>
            <Checkbox
              checked={props.selectedColumns.has(col.id)}
              onChange={event => props.onSelect(col.id, event.target.checked)}/>
            <ListItemText primary={col.name}/>
          </ListItem>
        ))}
        {props.additionalColumns.map(col => (
          <ListItem key={col.id} value={col.name} style={{ height: ITEM_HEIGHT }}>
            <Checkbox
              checked={props.selectedColumns.has(col.id)}
              onChange={event => props.onSelect(col.id, event.target.checked)}/>
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
