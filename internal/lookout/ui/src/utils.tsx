interface UIConfig {
  armadaApiBaseUrl: string
}

export async function getUIConfig(): Promise<UIConfig> {
  const config = {
    armadaApiBaseUrl: ""
  }

  try {
    const response = await fetch("/config")
    const json = await response.json()
    if (json.ArmadaApiBaseUrl) {
      config.armadaApiBaseUrl = json.ArmadaApiBaseUrl
    }
  } catch (e) {
    console.error(e)
  }

  return config
}

export function reverseMap<K, V>(map: Map<K, V>): Map<V, K> {
  return new Map(Array.from(map.entries()).map(([k, v]) => ([v, k])))
}

export function debounced(fn: (...args: any[]) => Promise<any>, delay: number): (...args: any[]) => Promise<any> {
  let timerId: NodeJS.Timeout | null;
  return function(...args: any[]): Promise<any> {
    return new Promise<any>(resolve => {
      if (timerId) {
        clearTimeout(timerId);
      }
      timerId = setTimeout(() => {
        resolve(fn(...args));
        timerId = null;
      }, delay);
    })
  }
}
