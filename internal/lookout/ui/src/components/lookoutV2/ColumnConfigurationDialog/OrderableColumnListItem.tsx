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

export interface OrderableColumnListItemProps {
  column: JobTableColumn
  isVisible: boolean
  onToggleVisibility: () => void
  removeAnnotationColumn: () => void
  editAnnotationColumn: (annotationKey: string) => void
  existingAnnotationColumnKeysSet: Set<string>
}

export const OrderableColumnListItem = ({
  column,
  isVisible,
  onToggleVisibility,
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

  const [isEditing, setIsEditing] = useState(false)

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
        <ListItemButton onClick={onToggleVisibility} dense disabled={!column.enableHiding} tabIndex={2} data-no-dnd>
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
      )}
    </ListItem>
  )
}
