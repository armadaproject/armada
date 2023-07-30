import { PayloadAction, createSlice } from "@reduxjs/toolkit"

type JobDetailLogProps = {
  line: string
  timestamp: string
}

const initialState = [
  {
    line: "",
    timestamp: "",
  },
]

const jobLogSlice = createSlice({
  name: "jobLogSlice",
  initialState,
  reducers: {
    setJobLog(state, action: PayloadAction<JobDetailLogProps[]>) {
      return [...action.payload]
    },
  },
})

export const { setJobLog } = jobLogSlice.actions
export default jobLogSlice.reducer
