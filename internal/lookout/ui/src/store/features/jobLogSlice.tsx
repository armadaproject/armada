import { PayloadAction, createSlice } from "@reduxjs/toolkit"

type JobDetailLogProps = {
  jobLog: { line: string; timestamp: string }[] | []
  loginfo: {
    runId: string
    jobRun: string
    container: string
  }
}

const initialState = {
  jobLog: [
    {
      line: "",
      timestamp: "",
    },
  ],
  loginfo: {
    runId: "",
    jobRun: "",
    container: "",
  },
}

const jobLogSlice = createSlice({
  name: "jobLogSlice",
  initialState,
  reducers: {
    setJobLog(state, action: PayloadAction<JobDetailLogProps>) {
      return { jobLog: [...action.payload?.jobLog], loginfo: action.payload?.loginfo }
    },
  },
})

export const { setJobLog } = jobLogSlice.actions
export default jobLogSlice.reducer
