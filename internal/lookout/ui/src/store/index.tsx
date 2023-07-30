import { configureStore } from "@reduxjs/toolkit"
import jobLogReducer from "./features/jobLogSlice"
import { TypedUseSelectorHook, useDispatch, useSelector } from "react-redux"
// import filtersReducer from '../features/filters/filtersSlice'

export const store = configureStore({
  reducer: {
    jobLog: jobLogReducer,
  },
  devTools: process.env.NODE_ENV !== "production",
})

// Globally setting the Type of Redux store
export type RootState = ReturnType<typeof store.getState>

// Globally setting the Type of Redux dispatch
export type AppDispatch = typeof store.dispatch
export const useAppDispatch: () => AppDispatch = useDispatch

export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector
