interface UIConfig {
  armadaApiBaseUrl: string
  userAnnotationPrefix: string
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
      binocularsBaseUrlPattern: json.BinocularsBaseUrlPattern ?? "",
    }
  } catch (e) {
    console.error(e)
  }

  return {
    armadaApiBaseUrl: "",
    userAnnotationPrefix: "",
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

export function selectItem<V>(key: string, item: V, selectedMap: Map<string, V>, isSelected: boolean) {
  if (isSelected) {
    selectedMap.set(key, item)
  } else if (selectedMap.has(key)) {
    selectedMap.delete(key)
  }
}
