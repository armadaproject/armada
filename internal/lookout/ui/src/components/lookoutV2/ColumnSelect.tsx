import { useState } from "react"

import { Check, Delete, Edit } from "@mui/icons-material"
import {
  Button,
  Checkbox,
  Divider,
  FormControl,
  IconButton,
  InputLabel,
  ListItemText,
  MenuItem,
  OutlinedInput,
  Select,
  TextField,
  Typography,
} from "@mui/material"
import { ColumnId, getColumnMetadata, JobTableColumn, toColId } from "utils/jobsTableColumns"

import styles from "./ColumnSelect.module.css"

type ColumnSelectProps = {
  allColumns: JobTableColumn[]
  groupedColumns: ColumnId[]
  visibleColumns: ColumnId[]
  onAddAnnotation: (annotationKey: string) => void
  onToggleColumn: (columnKey: string) => void
  onRemoveAnnotation: (columnKey: string) => void
  onEditAnnotation: (columnKey: string, annotationKey: string) => void
}

export default function ColumnSelect({
  allColumns,
  groupedColumns,
  visibleColumns,
  onAddAnnotation,
  onToggleColumn,
  onRemoveAnnotation,
  onEditAnnotation,
}: ColumnSelectProps) {
  const [creatingAnnotation, setCreatingAnnotation] = useState(false)
  const [newAnnotationKey, setNewAnnotationKey] = useState("")

  const [currentlyEditing, setCurrentlyEditing] = useState(new Map<string, string>())

  function clearAddAnnotation() {
    setCreatingAnnotation(false)
    setNewAnnotationKey("")
  }

  function saveNewAnnotation() {
    onAddAnnotation(newAnnotationKey)
    clearAddAnnotation()
  }

  function edit(key: string, name: string) {
    const newCurrentlyEditing = new Map<string, string>(currentlyEditing)
    newCurrentlyEditing.set(key, name)
    setCurrentlyEditing(newCurrentlyEditing)
  }

  function stopEditing(key: string) {
    if (currentlyEditing.has(key)) {
      const newCurrentlyEditing = new Map<string, string>(currentlyEditing)
      newCurrentlyEditing.delete(key)
      setCurrentlyEditing(newCurrentlyEditing)
    }
  }

  return (
    <>
      <FormControl sx={{ m: 0, width: 200 }} focused={false}>
        <InputLabel id="checkbox-select-label">Columns</InputLabel>
        <Select
          labelId="checkbox-select-label"
          id="demo-multiple-checkbox"
          multiple
          value={visibleColumns}
          input={<OutlinedInput label="Column" />}
          renderValue={(selected) => {
            return `${selected.length} columns selected`
          }}
          size="small"
        >
          <div className={styles.columnMenu}>
            <div className={styles.columnSelect} style={{ height: "100%" }}>
              {allColumns.map((column) => {
                const colId = toColId(column.id)
                const colIsGrouped = groupedColumns.includes(colId)
                const colIsVisible = visibleColumns.includes(colId)
                const colMetadata = getColumnMetadata(column)
                const colIsAnnotation = colMetadata.isAnnotation ?? false
                return (
                  <MenuItem key={colId} value={colMetadata.displayName} disabled={colIsGrouped}>
                    <Checkbox checked={colIsVisible} onClick={() => onToggleColumn(colId)} />
                    {colIsAnnotation ? (
                      <>
                        {currentlyEditing.has(colId) ? (
                          <>
                            <TextField
                              label="Annotation Key"
                              size="small"
                              variant="standard"
                              value={currentlyEditing.get(colId)}
                              onChange={(e) => edit(colId, e.target.value)}
                              style={{
                                maxWidth: 350,
                              }}
                              fullWidth={true}
                            />
                            <IconButton
                              onClick={() => {
                                if (currentlyEditing.has(colId)) {
                                  onEditAnnotation(colId, currentlyEditing.get(colId) ?? "")
                                }
                                stopEditing(colId)
                              }}
                            >
                              <Check />
                            </IconButton>
                          </>
                        ) : (
                          <>
                            <ListItemText
                              primary={colMetadata.displayName}
                              style={{
                                maxWidth: 350,
                                overflowX: "auto",
                              }}
                            />
                            <IconButton onClick={() => edit(colId, colId)}>
                              <Edit />
                            </IconButton>
                          </>
                        )}
                        <IconButton
                          onClick={() => {
                            stopEditing(colId)
                            onRemoveAnnotation(colId)
                          }}
                        >
                          <Delete />
                        </IconButton>
                      </>
                    ) : (
                      <ListItemText
                        primary={colMetadata.displayName + (colIsGrouped ? " (Grouped)" : "")}
                        style={{
                          maxWidth: 350,
                          overflowX: "auto",
                        }}
                      />
                    )}
                  </MenuItem>
                )
              })}
            </div>
            <Divider orientation="vertical" style={{ height: "100%" }} />
            <div className={styles.annotationSelectContainer}>
              <Typography display="block" variant="caption" sx={{ width: "100%" }}>
                Click here to add an annotation column.
              </Typography>
              <Typography display="block" variant="caption" sx={{ width: "100%" }}>
                Annotations are metadata (key-value pairs) that you can add to your job.
              </Typography>
              <div className={styles.addColumnButton}>
                {creatingAnnotation ? (
                  <>
                    <TextField
                      variant="outlined"
                      label="Annotation Key"
                      size="small"
                      sx={{ width: "100%" }}
                      value={newAnnotationKey}
                      onChange={(e) => {
                        setNewAnnotationKey(e.target.value)
                      }}
                      onKeyUp={(e) => {
                        if (e.key === "Enter") {
                          saveNewAnnotation()
                        }
                      }}
                    />
                    <div className={styles.addAnnotationButtons}>
                      <div className={styles.addAnnotationAction}>
                        <Button variant="outlined" onClick={clearAddAnnotation}>
                          Cancel
                        </Button>
                      </div>
                      <div className={styles.addAnnotationAction}>
                        <Button variant="contained" onClick={saveNewAnnotation}>
                          Save
                        </Button>
                      </div>
                    </div>
                  </>
                ) : (
                  <Button variant="contained" onClick={() => setCreatingAnnotation(true)}>
                    Add column
                  </Button>
                )}
              </div>
            </div>
          </div>
        </Select>
      </FormControl>
    </>
  )
}
