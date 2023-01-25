import { useState, MouseEvent, useRef } from "react"

import { ArrowDropDown, ArrowDropUp } from "@material-ui/icons"
import { Check, Delete, Edit } from "@mui/icons-material"
import {
  Button,
  Checkbox,
  Divider,
  FormControl,
  IconButton,
  InputAdornment,
  InputLabel,
  ListItemText,
  MenuItem,
  OutlinedInput,
  Popover,
  Select,
  TextField,
  Typography,
} from "@mui/material"
import { VisibilityColumn } from "@tanstack/react-table"
import { ColumnId, getColumnMetadata, JobTableColumn, toColId } from "utils/jobsTableColumns"

import styles from "./ColumnSelect.module.css"

type ColumnSelectProps = {
  selectableColumns: JobTableColumn[]
  groupedColumns: ColumnId[]
  visibleColumns: ColumnId[]
  onAddAnnotation: (annotationKey: string) => void
  onToggleColumn: (columnId: ColumnId) => void
  onRemoveAnnotation: (columnId: ColumnId) => void
  onEditAnnotation: (columnId: ColumnId, newDisplayName: string) => void
}

export default function ColumnSelect({
  selectableColumns,
  groupedColumns,
  visibleColumns,
  onAddAnnotation,
  onToggleColumn,
  onRemoveAnnotation,
  onEditAnnotation,
}: ColumnSelectProps) {
  const labelRef = useRef<HTMLLabelElement>(null)
  const anchorEl = useRef<HTMLInputElement>(null)
  const [isOpen, setIsOpen] = useState<boolean>(false)

  const [creatingAnnotation, setCreatingAnnotation] = useState(false)
  const [newAnnotationKey, setNewAnnotationKey] = useState("")

  const [currentlyEditing, setCurrentlyEditing] = useState(new Map<string, string>())

  function clearAddAnnotation() {
    setCreatingAnnotation(false)
    setNewAnnotationKey("")
  }

  function saveNewAnnotation() {
    onAddAnnotation(newAnnotationKey.trim())
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
      <FormControl sx={{ m: 0, mt: "4px", width: 200 }} focused={false}>
        <InputLabel
          htmlFor="column-select-input"
          variant="filled"
          style={{ transform: "translate(12px, -8px) scale(0.75)" }}
        >
          Columns
        </InputLabel>
        <OutlinedInput
          id="column-select-input"
          ref={anchorEl}
          size={"small"}
          onClick={() => setIsOpen(true)}
          type={"button"}
          label={"Columns"}
          value={`${
            selectableColumns.filter((col) => (visibleColumns as string[]).includes(col.id ?? "")).length
          } columns selected`}
          endAdornment={<InputAdornment position="end">{isOpen ? <ArrowDropUp /> : <ArrowDropDown />}</InputAdornment>}
          style={{ paddingRight: 5 }}
        />
        <Popover
          open={isOpen}
          onClose={() => setIsOpen(false)}
          anchorEl={anchorEl.current}
          anchorOrigin={{
            vertical: "bottom",
            horizontal: "center",
          }}
          transformOrigin={{
            vertical: "top",
            horizontal: "center",
          }}
        >
          <div className={styles.columnMenu}>
            <div className={styles.columnSelect} style={{ height: "100%" }}>
              {selectableColumns.map((column) => {
                const colId = toColId(column.id)
                const colIsGrouped = groupedColumns.includes(colId)
                const colIsVisible = visibleColumns.includes(colId)
                const colMetadata = getColumnMetadata(column)
                const colIsAnnotation = colMetadata.annotation ?? false
                return (
                  <MenuItem
                    onClick={() => {
                      if (!colIsAnnotation) {
                        onToggleColumn(colId)
                      }
                    }}
                    key={colId}
                    value={colMetadata.displayName}
                    disabled={colIsGrouped}
                  >
                    <Checkbox checked={colIsVisible} onClick={() => onToggleColumn(colId)} />
                    {colIsAnnotation ? (
                      <>
                        {currentlyEditing.has(colId) ? (
                          <>
                            <TextField
                              label="Annotation Key"
                              size="small"
                              variant="standard"
                              autoFocus
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
                            <IconButton onClick={() => edit(colId, colMetadata.displayName)}>
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
                      autoFocus
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
          {/*</Select>*/}
        </Popover>
      </FormControl>
    </>
  )
}
