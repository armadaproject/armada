import { alpha, MenuItem, Select, styled, Typography } from "@mui/material"

import { AggregateType, aggregateTypes } from "../models/lookoutModels"

const StyledSelect = styled(Select)(({ theme }) => ({
  fontSize: theme.typography.body2.fontSize,
  margin: 0,
  padding: 0,
  color: alpha(theme.palette.text.primary, 0.7),
  "& .MuiSelect-select": {
    padding: "2px 24px 2px 6px",
  },
  "& .MuiOutlinedInput-notchedOutline": {
    border: "none",
  },
  "&:hover": {
    backgroundColor: alpha(theme.palette.primary.main, 0.08),
    borderRadius: theme.shape.borderRadius,
    "& .MuiOutlinedInput-notchedOutline": {
      border: "none",
    },
  },
}))

const SelectWrapper = styled("div")({
  display: "flex",
  gap: "4px",
})

const SelectorContainer = styled("div")({
  display: "flex",
  alignItems: "center",
  gap: "4px",
})

// For type-safety
const earliestAggregateType: AggregateType = "earliest"
const latestAggregateType: AggregateType = "latest"
const averageAggregateType: AggregateType = "average"

export interface LastTransitionTimeAggregateSelectorProps {
  value: AggregateType
  onChange: (value: AggregateType) => void
}

export const LastTransitionTimeAggregateSelector = ({ value, onChange }: LastTransitionTimeAggregateSelectorProps) => (
  <SelectorContainer onClick={(e) => e.stopPropagation()}>
    <Typography variant="caption" color="textSecondary">
      Show:
    </Typography>
    <SelectWrapper>
      <StyledSelect
        value={value}
        size="small"
        variant="outlined"
        onChange={({ target: { value: newValue } }) => {
          if (([...aggregateTypes] as unknown[]).includes(newValue)) {
            onChange(newValue as AggregateType)
          }
        }}
      >
        <MenuItem value={latestAggregateType}>Latest</MenuItem>
        <MenuItem value={earliestAggregateType}>Earliest</MenuItem>
        <MenuItem value={averageAggregateType}>Average</MenuItem>
      </StyledSelect>
    </SelectWrapper>
  </SelectorContainer>
)
