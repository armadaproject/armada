import { PointerEvent, useCallback, useMemo, useRef, useState } from "react"

import { DndContext, DragEndEvent, KeyboardSensor, PointerSensor, useSensor, useSensors } from "@dnd-kit/core"
import { restrictToWindowEdges, restrictToVerticalAxis } from "@dnd-kit/modifiers"
import { arrayMove, SortableContext } from "@dnd-kit/sortable"
import { ArrowDownward, Close } from "@mui/icons-material"
import {
  Alert,
  Chip,
  Dialog,
  DialogContent,
  DialogContentText,
  DialogTitle,
  IconButton,
  List,
  Stack,
  styled,
  Typography,
} from "@mui/material"
import { ErrorBoundary } from "react-error-boundary"

import { AddAnnotationColumnInput } from "./AddAnnotationColumnInput"
import { OrderableColumnListItem } from "./OrderableColumnListItem"
import { SPACING } from "../../../styling/spacing"
import {
  AnnotationColumnId,
  ColumnId,
  fromAnnotationColId,
  getColumnMetadata,
  JobTableColumn,
  PINNED_COLUMNS,
  toColId,
} from "../../../utils/jobsTableColumns"
import { AlertErrorFallback } from "../../AlertErrorFallback"

const ScrollToAddAnnotationColumnChip = styled(Chip)({
  zIndex: 100,

  position: "absolute",
  left: "50%",
  bottom: 10,
  transform: "translate(-50%, 0)",
})

const StyledDialogTitle = styled(DialogTitle)({
  paddingRight: 10,
})

const CloseIconButton = styled(IconButton)(({ theme }) => ({
  position: "absolute",
  top: 8,
  right: 8,
  color: theme.palette.text.secondary,
}))

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
  allColumns: JobTableColumn[]
  groupedColumnIds: ColumnId[]
  filterColumnIds: ColumnId[]
  sortColumnIds: ColumnId[]
  visibleColumnIds: ColumnId[]
  columnOrderIds: ColumnId[]
  setColumnOrder: (columnOrder: ColumnId[]) => void
  toggleColumnVisibility: (columnId: ColumnId) => void
  onAddAnnotationColumn: (annotationKey: string) => void
  onRemoveAnnotationColumn: (colId: ColumnId) => void
  onEditAnnotationColumn: (colId: ColumnId, annotationKey: string) => void
}

export const ColumnConfigurationDialog = ({
  open,
  onClose,
  allColumns,
  groupedColumnIds,
  filterColumnIds,
  sortColumnIds,
  visibleColumnIds,
  columnOrderIds,
  setColumnOrder,
  toggleColumnVisibility,
  onAddAnnotationColumn,
  onRemoveAnnotationColumn,
  onEditAnnotationColumn,
}: ColumnConfigurationDialogProps) => {
  const allColumnsById = useMemo(
    () =>
      allColumns.reduce(
        (acc, column) => {
          acc[toColId(column.id)] = column
          return acc
        },
        {} as Record<ColumnId, JobTableColumn>,
      ),
    [allColumns],
  )

  const groupedColumnsSet = useMemo(() => new Set(groupedColumnIds), [groupedColumnIds])
  const filterColumnsSet = useMemo(() => new Set(filterColumnIds), [filterColumnIds])
  const sortColumnsSet = useMemo(() => new Set(sortColumnIds), [sortColumnIds])
  const visibleColumnsSet = useMemo(() => new Set(visibleColumnIds), [visibleColumnIds])

  const orderedColumns = useMemo(() => {
    return columnOrderIds
      .filter((id) => !groupedColumnsSet.has(id) && !PINNED_COLUMNS.includes(toColId(id)) && id in allColumnsById)
      .map((id) => allColumnsById[id])
  }, [columnOrderIds, groupedColumnIds, allColumnsById, groupedColumnsSet])

  const handleDragEnd = useCallback(
    ({ active, over }: DragEndEvent) => {
      if (over && active.id !== over.id) {
        const oldIndex = columnOrderIds.indexOf(active.id as ColumnId)
        const newIndex = columnOrderIds.indexOf(over.id as ColumnId)
        setColumnOrder(arrayMove(columnOrderIds, oldIndex, newIndex))
      }
    },
    [columnOrderIds, setColumnOrder],
  )

  const annotationColumnKeysSet = useMemo(
    () =>
      new Set(
        allColumns
          .filter((column) => getColumnMetadata(column).annotation)
          .map(({ id }) => fromAnnotationColId(id as AnnotationColumnId)),
      ),
    [allColumns],
  )

  const pointerSensor = useSensor(ColumnListPointerSensor)
  const keyboardSensor = useSensor(KeyboardSensor)
  const sensors = useSensors(pointerSensor, keyboardSensor)

  const [isAddAnnotationColumnContainerVisible, setIsAddAnnotationColumnContainerVisible] = useState(false)
  const addAnnotationColumnContainerObserver = useRef<IntersectionObserver>(undefined)
  const scrollIntoViewAddAnnotationColumnContainer = useRef<() => void>(undefined)
  const addAnnotationColumnContainerRef = useCallback((node: HTMLDivElement) => {
    if (addAnnotationColumnContainerObserver.current) {
      addAnnotationColumnContainerObserver.current.disconnect()
    }

    addAnnotationColumnContainerObserver.current = new IntersectionObserver(([entry]) => {
      setIsAddAnnotationColumnContainerVisible(entry.isIntersecting)
    })

    if (node) {
      addAnnotationColumnContainerObserver.current.observe(node)
      scrollIntoViewAddAnnotationColumnContainer.current = () => {
        node.scrollIntoView({ behavior: "smooth" })
      }
    }
  }, [])

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      {!isAddAnnotationColumnContainerVisible && (
        <ScrollToAddAnnotationColumnChip
          label="Add an annotation column"
          onClick={scrollIntoViewAddAnnotationColumnContainer.current ?? undefined}
          icon={<ArrowDownward />}
          color="primary"
        />
      )}
      <StyledDialogTitle>Column configuration</StyledDialogTitle>
      <CloseIconButton aria-label="close" onClick={onClose} title="Close">
        <Close />
      </CloseIconButton>
      <DialogContent>
        <ErrorBoundary FallbackComponent={AlertErrorFallback}>
          <Stack spacing={SPACING.sm}>
            <DialogContentText component="p">
              Select which columns to view, any additional annotation columns, and the order of the columns.
            </DialogContentText>
            {groupedColumnIds.length > 0 && (
              <>
                <DialogContentText variant="body2">
                  The following columns cannot be hidden or re-ordered because they are currently grouped:
                </DialogContentText>
                <DialogContentText component="ul" variant="body2">
                  {groupedColumnIds
                    .map((id) => allColumnsById[id])
                    .map((column) => {
                      const { displayName } = getColumnMetadata(column)
                      return (
                        <Typography key={column.id} component="li" variant="body2">
                          {displayName}
                        </Typography>
                      )
                    })}
                </DialogContentText>
              </>
            )}
            <Alert severity="info" variant="outlined">
              Click and drag the columns into your desired order.
            </Alert>
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
                        <OrderableColumnListItem
                          key={colId}
                          column={column}
                          isVisible={visibleColumnsSet.has(colId)}
                          onToggleVisibility={() => toggleColumnVisibility(colId)}
                          filtered={filterColumnsSet.has(colId)}
                          sorted={sortColumnsSet.has(colId)}
                          removeAnnotationColumn={() => onRemoveAnnotationColumn(colId)}
                          editAnnotationColumn={(annotationKey) => onEditAnnotationColumn(colId, annotationKey)}
                          existingAnnotationColumnKeysSet={annotationColumnKeysSet}
                        />
                      )
                    })}
                  </List>
                </SortableContext>
              </DndContext>
            </div>
            <div ref={addAnnotationColumnContainerRef}>
              <DialogContentText component="h3">Add annotation column</DialogContentText>
              <DialogContentText variant="body2">
                Annotations are metadata (key-value pairs) that you can add to your job.
              </DialogContentText>
              <AddAnnotationColumnInput
                onCreate={onAddAnnotationColumn}
                existingAnnotationColumnKeysSet={annotationColumnKeysSet}
              />
            </div>
          </Stack>
        </ErrorBoundary>
      </DialogContent>
    </Dialog>
  )
}
