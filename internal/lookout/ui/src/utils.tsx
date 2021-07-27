interface UIConfig {
  armadaApiBaseUrl: string
  userAnnotationPrefix: string
  binocularsEnabled: boolean
  binocularsBaseUrlPattern: string
}

export interface Padding {
  top: number
  bottom: number
  left: number
  right: number
}

export async function getUIConfig(): Promise<UIConfig> {
  try {
    const response = await fetch("/config")
    const json = await response.json()
    return {
      armadaApiBaseUrl: json.ArmadaApiBaseUrl ?? "",
      userAnnotationPrefix: json.UserAnnotationPrefix ?? "",
      binocularsEnabled: json.BinocularsEnabled ?? true,
      binocularsBaseUrlPattern: json.BinocularsBaseUrlPattern ?? "",
    }
  } catch (e) {
    console.error(e)
  }

  return {
    armadaApiBaseUrl: "",
    userAnnotationPrefix: "",
    binocularsEnabled: true,
    binocularsBaseUrlPattern: "",
  }
}

export function reverseMap<K, V>(map: Map<K, V>): Map<V, K> {
  return new Map(Array.from(map.entries()).map(([k, v]) => [v, k]))
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

export function secondsToDurationString(totalSeconds: number): string {
  totalSeconds = Math.round(totalSeconds)
  const days = Math.floor(totalSeconds / (24 * 3600))
  const hours = Math.floor(totalSeconds / 3600) % 24
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60

  const segments: string[] = []

  if (days > 0) {
    segments.push(`${days}d`)
  }
  if (hours > 0) {
    segments.push(`${hours}h`)
  }
  if (minutes > 0) {
    segments.push(`${minutes}m`)
  }
  if (seconds > 0) {
    segments.push(`${seconds}s`)
  }
  if (segments.length === 0) {
    return "0s"
  }

  return segments.join(" ")
}

export function setStateAsync<T>(component: React.Component<any, T>, state: T): Promise<void> {
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
  let basicMessage = (error?.status ?? "") + " " + (error?.statusText ?? "")
  basicMessage = basicMessage != " " ? basicMessage : "Unknown error"
  try {
    const json = await error.json()
    const errorMessage = json.message
    return errorMessage ?? basicMessage
  } catch {
    return basicMessage
  }
}

export function updateArray<T>(array: T[], newValues: T[], start: number) {
  for (let i = 0; i < newValues.length; i++) {
    const arrayIndex = start + i
    if (arrayIndex < array.length) {
      array[arrayIndex] = newValues[i]
    } else if (arrayIndex >= array.length) {
      array.push(newValues[i])
    } else {
      throw new Error("Index is bad!")
    }
  }
}
