import { Component, FC } from "react"

import { Location, NavigateFunction, Params, useLocation, useNavigate, useParams } from "react-router-dom"

export type RequestStatus = "Loading" | "Idle"

export type ApiResult = "Success" | "Failure" | "Partial success"

export interface Padding {
  top: number
  bottom: number
  left: number
  right: number
}

export function inverseRecord<K extends string | number | symbol, V extends string | number | symbol>(
  record: Record<K, V>,
): Record<V, K> {
  return Object.fromEntries(Object.entries(record).map(([k, v]) => [v, k]))
}

export function debounced(fn: (...args: any[]) => Promise<any>, delay: number): (...args: any[]) => Promise<any> {
  let timerId: NodeJS.Timeout | null
  return function (...args: any[]): Promise<any> {
    return new Promise<any>((resolve) => {
      if (timerId) {
        clearTimeout(timerId)
      }
      timerId = setTimeout(() => {
        resolve(fn(...args))
        timerId = null
      }, delay)
    })
  }
}

export function setStateAsync<T>(component: Component<any, T>, state: T): Promise<void> {
  return new Promise((resolve) => {
    component.setState(state, resolve)
  })
}

export function selectItem<V>(key: string, item: V, selectedMap: Map<string, V>, isSelected: boolean) {
  if (isSelected) {
    selectedMap.set(key, item)
  } else if (selectedMap.has(key)) {
    selectedMap.delete(key)
  }
}

export async function getErrorMessage(error: any): Promise<string> {
  if (error === undefined) {
    return "Unknown error"
  }
  let basicMessage = (error.status ?? "") + " " + (error.statusText ?? "")
  if (basicMessage === " ") {
    if (error.toString() !== undefined && typeof error.toString === "function") {
      basicMessage = error.toString()
    } else {
      basicMessage = "Unknown error"
    }
  }
  try {
    const json = await error.json()
    const errorMessage = json.message
    return errorMessage ?? basicMessage
  } catch {
    return basicMessage
  }
}

export function tryParseJson(json: string): Record<string, unknown> | unknown[] | undefined {
  try {
    return JSON.parse(json) as Record<string, unknown>
  } catch (e: unknown) {
    if (e instanceof Error) {
      console.error(e.message)
    }
    return undefined
  }
}

const priorityRegex = new RegExp("^([0-9]+)$")

export function priorityIsValid(priority: string): boolean {
  return priorityRegex.test(priority) && priority.length > 0
}

export async function waitMillis(millisToWait: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, millisToWait))
}

export function removeUndefined(obj: Record<string, any>) {
  return Object.keys(obj).forEach((key) => {
    if (obj[key] === undefined) {
      delete obj[key]
    }
  })
}

export interface Router {
  location: Location
  navigate: NavigateFunction
  params: Readonly<Params>
}

export interface PropsWithRouter {
  router: Router
}

export function withRouter<T extends PropsWithRouter>(Component: FC<T>): FC<Omit<T, "router">> {
  function ComponentWithRouterProp(props: T) {
    const location = useLocation()
    const navigate = useNavigate()
    const params = useParams()
    return <Component {...props} router={{ location, navigate, params }} />
  }
  return ComponentWithRouterProp as FC<Omit<T, "router">>
}

export const PlatformCancelReason = "Platform error marked by user"
