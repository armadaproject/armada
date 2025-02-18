import { Dispatch, SetStateAction, useCallback, useEffect, useState } from "react"

export const booleanFromStorageValue = (storageValue: string | null): boolean | null => {
  if (storageValue === null) {
    return null
  }

  try {
    const parsedValue = JSON.parse(storageValue)
    if (typeof parsedValue !== "boolean") {
      return null
    }

    return parsedValue
  } catch {
    return null
  }
}

export const booleanToStorageValue = (value: boolean): string => JSON.stringify(value)

export const textEnumFromStorageValue = <TEnum extends string>(
  storageValue: string | null,
  enumMap: Record<TEnum, true>,
): TEnum | null => {
  if (storageValue === null) {
    return null
  }

  if (enumMap[storageValue as TEnum]) {
    return storageValue as TEnum
  }

  return null
}

export const textEnumToStorageValue = <TEnum extends string>(value: TEnum): string => value

const EVENT_TYPE = "localstorage"

export const useLocalStorageValue = <TValue>(
  key: string,
  defaultValue: TValue,
  fromStorageValue: (storageValue: string | null) => TValue | null,
  toStorageValue: (value: TValue) => string,
): [TValue, Dispatch<SetStateAction<TValue>>] => {
  const updateState = useCallback(() => {
    const storageValue = localStorage.getItem(key)
    const value = fromStorageValue(storageValue)
    if (value !== null) {
      return value
    }

    localStorage.setItem(key, toStorageValue(defaultValue))
    window.dispatchEvent(new Event(EVENT_TYPE))
    return defaultValue
  }, [key, defaultValue, fromStorageValue, toStorageValue])

  const [state, setState] = useState(updateState)

  useEffect(() => {
    localStorage.setItem(key, toStorageValue(state))
    window.dispatchEvent(new Event(EVENT_TYPE))
  }, [key, state])

  useEffect(() => {
    const listenStorageChange = () => {
      setState(updateState)
    }
    window.addEventListener(EVENT_TYPE, listenStorageChange)
    return () => window.removeEventListener(EVENT_TYPE, listenStorageChange)
  }, [updateState])

  return [state, setState]
}
