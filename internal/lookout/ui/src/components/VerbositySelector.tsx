import { useState } from "react"

import { Chat, ChatBubbleOutline } from "@mui/icons-material"
import { Rating, styled, Typography } from "@mui/material"

import { SPACING } from "../styling/spacing"

const OuterContainer = styled("div")(({ theme }) => ({
  display: "flex",
  flexDirection: "column",
  gap: theme.spacing(SPACING.xs),
}))

const RatingAndLabelContainer = styled("div")(({ theme }) => ({
  display: "flex",
  alignItems: "center",
  gap: theme.spacing(SPACING.sm),
}))

const StyledRating = styled(Rating)(({ theme }) => ({
  "& .MuiRating-iconFilled": {
    color: theme.palette.secondary.light,
  },
  "& .MuiRating-iconHover": {
    color: theme.palette.secondary.main,
  },
}))

export interface VerbositySelectorProps {
  name: string
  legendLabel: string
  max: number
  verbosity: number
  onChange: (verbosity: number) => void
  disabled?: boolean
}

export const VerbositySelector = ({
  name,
  legendLabel,
  max,
  verbosity,
  onChange,
  disabled,
}: VerbositySelectorProps) => {
  const [hoverValue, setHoverValue] = useState(-1)
  return (
    <OuterContainer>
      <Typography component="legend" variant="caption">
        {legendLabel}
      </Typography>
      <RatingAndLabelContainer>
        <StyledRating
          name={name}
          value={verbosity}
          onChangeActive={(_, value) => {
            setHoverValue(value)
          }}
          onChange={(_, value) => onChange(value ?? 0)}
          max={max}
          getLabelText={(value) => value.toString()}
          icon={<Chat fontSize="inherit" />}
          emptyIcon={<ChatBubbleOutline fontSize="inherit" />}
          disabled={disabled}
        />
        <Typography>Level {hoverValue !== -1 ? hoverValue.toString() : verbosity.toString()}</Typography>
      </RatingAndLabelContainer>
    </OuterContainer>
  )
}
