import { useState } from "react"

import { useSortable } from "@dnd-kit/sortable"
import { CSS } from "@dnd-kit/utilities"
import { Cancel, Delete, DragHandle, Edit } from "@mui/icons-material"
import {
  Checkbox,
  IconButton,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Stack,
  styled,
  Tooltip,
} from "@mui/material"

import { EditAnnotationColumnInput } from "./EditAnnotationColumnInput"
import { SPACING } from "../../../styling/spacing"
import {
  AnnotationColumnId,
  fromAnnotationColId,
  getColumnMetadata,
  JobTableColumn,
  toColId,
} from "../../../utils/jobsTableColumns"

const GrabListItemIcon = styled(ListItemIcon)({
  cursor: "grab",
  touchAction: "none",
})

const EditAnnotationColumnInputContainer = styled(Stack)({
  width: "100%",
})

const TooltipChildContainer = styled("div")({
  display: "flex",
  flexGrow: 1,
})

export interface OrderableColumnListItemProps {
  column: JobTableColumn
  isVisible: boolean
  onToggleVisibility: () => void
  filtered: boolean
  sorted: boolean
  removeAnnotationColumn: () => void
  editAnnotationColumn: (annotationKey: string) => void
  existingAnnotationColumnKeysSet: Set<string>
}

export const OrderableColumnListItem = ({
  column,
  isVisible,
  onToggleVisibility,
  filtered,
  sorted,
  removeAnnotationColumn,
  editAnnotationColumn,
  existingAnnotationColumnKeysSet,
}: OrderableColumnListItemProps) => {
  const colId = toColId(column.id)
  const colMetadata = getColumnMetadata(column)
  const colIsAnnotation = colMetadata.annotation ?? false
  const { attributes, listeners, setNodeRef, transform, transition } = useSortable({
    id: colId,
  })

  throw new Error("argh")

  const [isEditing, setIsEditing] = useState(false)

  let listItemButtonNode = (
    <ListItemButton
      onClick={onToggleVisibility}
      dense
      disabled={filtered || sorted || !column.enableHiding}
      tabIndex={2}
      data-no-dnd
    >
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
      <ListItemText
        id={colId}
        primary={colMetadata.displayName}
        secondary={colIsAnnotation ? "Annotation" : undefined}
      />
    </ListItemButton>
  )
  if (filtered || sorted) {
    const title = (() => {
      if (filtered && sorted) {
        return `The ${colMetadata.displayName} column cannot be hidden because filtering and sorting has been applied to it`
      }
      if (filtered) {
        return `The ${colMetadata.displayName} column cannot be hidden because filtering has been applied to it`
      }
      return `The ${colMetadata.displayName} column cannot be hidden because sorting has been applied to it`
    })()

    listItemButtonNode = (
      <Tooltip title={title} arrow={false} followCursor placement="top">
        <TooltipChildContainer>{listItemButtonNode}</TooltipChildContainer>
      </Tooltip>
    )
  }

  return (
    <ListItem
      key={colId}
      disablePadding
      dense
      style={{ transform: CSS.Transform.toString(transform), transition }}
      secondaryAction={
        colIsAnnotation && !isEditing ? (
          <>
            <IconButton
              data-no-dnd
              title="Edit column"
              onClick={() => {
                setIsEditing(true)
              }}
            >
              <Edit />
            </IconButton>
            <IconButton data-no-dnd edge="end" title="Delete column" onClick={removeAnnotationColumn}>
              <Delete />
            </IconButton>
          </>
        ) : undefined
      }
      ref={setNodeRef}
      {...attributes}
      {...listeners}
    >
      <GrabListItemIcon>
        <DragHandle />
      </GrabListItemIcon>
      {colIsAnnotation && isEditing ? (
        <EditAnnotationColumnInputContainer data-no-dnd spacing={SPACING.xs} direction="row">
          <EditAnnotationColumnInput
            onEdit={(annotationKey) => {
              editAnnotationColumn(annotationKey)
              setIsEditing(false)
            }}
            currentAnnotationKey={fromAnnotationColId(colId as AnnotationColumnId)}
            existingAnnotationColumnKeys={existingAnnotationColumnKeysSet}
          />
          <div>
            <IconButton onClick={() => setIsEditing(false)} title="Cancel changes">
              <Cancel />
            </IconButton>
          </div>
        </EditAnnotationColumnInputContainer>
      ) : (
        listItemButtonNode
      )}
    </ListItem>
  )
}
