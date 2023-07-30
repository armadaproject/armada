import { configureStore } from "@reduxjs/toolkit"
import jobLogReducer from "./features/jobLogSlice"
import { useDispatch } from "react-redux"
// import filtersReducer from '../features/filters/filtersSlice'

export const store = configureStore({
  reducer: {
    jobLog: jobLogReducer,
  },
})

// Globally setting the Type of Redux store
export type RootState = ReturnType<typeof store.getState>

// Globally setting the Type of Redux dispatch
export type AppDispatch = typeof store.dispatch
export const useAppDispatch: () => AppDispatch = useDispatch //
