import { configureStore, combineReducers } from "@reduxjs/toolkit"
import { TypedUseSelectorHook, useDispatch, useSelector } from "react-redux"
import { persistReducer, persistStore } from "redux-persist"
import storage from "redux-persist/lib/storage"

// Features
import jobLogReducer from "./features/jobLogSlice"

const persistConfig = {
  key: "root",
  version: 1,
  storage: storage,
}

const rootReducer = combineReducers({ jobLogSlice: jobLogReducer })

const persistedReducer = persistReducer(persistConfig, rootReducer)

export const store = configureStore({
  reducer: persistedReducer,
  devTools: process.env.NODE_ENV !== "production",
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: false,
    }),
})

// Globally setting the Type of Redux store
export type RootState = ReturnType<typeof persistedReducer>

// Globally setting the Type of Redux useDispatch
export type AppDispatch = typeof store.dispatch
export const useAppDispatch: () => AppDispatch = useDispatch

// Globally setting the Type of Redux useSelector
export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector

// Export persistor so State can be persisted along side the store state value
export const persistor = persistStore(store)
