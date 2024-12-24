import {
  Alert,
  Checkbox,
  Dialog,
  DialogContent,
  DialogContentText,
  DialogTitle,
  FormControl,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Stack,
  styled,
  TextField,
} from "@mui/material"
import { SPACING } from "../../styling/spacing"
import { ColumnId, getColumnMetadata, JobTableColumn, toColId } from "../../utils/jobsTableColumns"
import { PointerEvent, useCallback, useMemo } from "react"
import { DragHandle } from "@mui/icons-material"
import { DndContext, DragEndEvent, KeyboardSensor, PointerSensor, useSensor, useSensors } from "@dnd-kit/core"
import { arrayMove, SortableContext, useSortable } from "@dnd-kit/sortable"
import { CSS } from "@dnd-kit/utilities"
import { restrictToWindowEdges, restrictToVerticalAxis } from "@dnd-kit/modifiers"

const GrabListItemIcon = styled(ListItemIcon)({
  cursor: "grab",
  touchAction: "none",
})

interface SortableColumnProps {
  column: JobTableColumn
  isGrouped: boolean
  isVisible: boolean
  onToggleVisibility: () => void
}

const SortableColumn = ({ column, isGrouped, isVisible, onToggleVisibility }: SortableColumnProps) => {
  const colId = toColId(column.id)
  const colMetadata = getColumnMetadata(column)
  const colIsAnnotation = colMetadata.annotation ?? false

  const { attributes, listeners, setNodeRef, transform, transition } = useSortable({ id: colId, disabled: isGrouped })

  return (
    <ListItem
      key={colId}
      disablePadding
      dense
      style={{ transform: CSS.Transform.toString(transform), transition }}
      ref={setNodeRef}
      {...attributes}
      {...listeners}
    >
      <GrabListItemIcon>
        <DragHandle />
      </GrabListItemIcon>
      <ListItemButton onClick={onToggleVisibility} dense disabled={isGrouped} tabIndex={2} data-no-dnd>
        <ListItemIcon>
          <Checkbox
            edge="start"
            checked={isVisible}
            tabIndex={-1}
            disableRipple
            inputProps={{ "aria-labelledby": colId }}
            size="small"
          />
        </ListItemIcon>
        <ListItemText id={colId} primary={colMetadata.displayName} secondary={isGrouped ? "Grouped" : undefined} />
      </ListItemButton>
    </ListItem>
  )
}

class ColumnListPointerSensor extends PointerSensor {
  static activators = [
    {
      eventName: "onPointerDown",
      handler: ({ nativeEvent: event }: PointerEvent) => {
        // Block DnD event propagation if element has "data-no-dnd" attribute
        let cur = event.target as HTMLElement

        while (cur) {
          if (cur.dataset && cur.dataset.noDnd) {
            return false
          }
          cur = cur.parentElement as HTMLElement
        }

        return true
      },
    },
  ] as (typeof PointerSensor)["activators"]
}

export interface ColumnConfigurationDialogProps {
  open: boolean
  onClose: () => void

  selectableColumns: JobTableColumn[]
  groupedColumns: ColumnId[]
  visibleColumns: ColumnId[]
  columnOrder: ColumnId[]

  setColumnOrder: (columnOrder: ColumnId[]) => void
  toggleColumnVisibility: (columnId: ColumnId) => void
}

export const ColumnConfigurationDialog = ({
  open,
  onClose,
  selectableColumns,
  groupedColumns,
  visibleColumns,
  columnOrder,
  setColumnOrder,
  toggleColumnVisibility,
}: ColumnConfigurationDialogProps) => {
  const orderedColumns = useMemo(() => {
    const selectableColumnsById = selectableColumns.reduce(
      (acc, column) => {
        acc[toColId(column.id)] = column
        return acc
      },
      {} as Record<ColumnId, JobTableColumn>,
    )

    const remainingColIds = new Set(Object.keys(selectableColumnsById) as ColumnId[])
    const result = [...groupedColumns, ...columnOrder].flatMap((colId) => {
      const present = remainingColIds.delete(colId)
      return present && colId in selectableColumnsById ? [selectableColumnsById[colId]] : []
    })

    if (remainingColIds.size > 0) {
      console.warn(
        `The folloiwng column IDs were not in the orderedColumns array: ${[...remainingColIds].join(", ")}. This is a bug.`,
      )
      remainingColIds.forEach((colId) => result.push(selectableColumnsById[colId]))
    }
    return result
  }, [columnOrder, selectableColumns])

  const handleDragEnd = useCallback(
    ({ active, over }: DragEndEvent) => {
      if (over && active.id !== over.id) {
        const oldIndex = columnOrder.indexOf(active.id as ColumnId)
        const newIndex = columnOrder.indexOf(over.id as ColumnId)
        setColumnOrder(arrayMove(columnOrder, oldIndex, newIndex))
      }
    },
    [columnOrder, setColumnOrder],
  )

  const pointerSensor = useSensor(ColumnListPointerSensor)
  const keyboardSensor = useSensor(KeyboardSensor)

  const sensors = useSensors(pointerSensor, keyboardSensor)

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Column configuration</DialogTitle>
      <DialogContent>
        <Stack spacing={SPACING.sm}>
          <DialogContentText component="p">
            Select which columns to view, any additional annotation columns, and the order of the columns.
          </DialogContentText>
          <Alert severity="info" variant="outlined">
            Click and drag the columns into your desired order.
          </Alert>
          <div>
            <FormControl margin="dense" fullWidth size="small">
              <TextField
                id="add-annotation-column"
                label="Add annotation column"
                helperText="Annotations are metadata (key-value pairs) that you can add to your job."
                size="small"
              />
            </FormControl>
          </div>
          <div>
            <DndContext
              onDragEnd={handleDragEnd}
              sensors={sensors}
              modifiers={[restrictToVerticalAxis, restrictToWindowEdges]}
            >
              <SortableContext items={orderedColumns.map(({ id }) => toColId(id))}>
                <List dense>
                  {orderedColumns.map((column) => {
                    const colId = toColId(column.id)
                    return (
                      <SortableColumn
                        key={toColId(column.id)}
                        column={column}
                        isGrouped={groupedColumns.includes(colId)}
                        isVisible={visibleColumns.includes(colId)}
                        onToggleVisibility={() => toggleColumnVisibility(colId)}
                      />
                    )
                  })}
                </List>
              </SortableContext>
            </DndContext>
          </div>
        </Stack>
      </DialogContent>
    </Dialog>
  )
}
