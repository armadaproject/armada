export function updateArray<T>(array: (T | null)[], newValues: (T | null)[], start: number, end: number) {
  const nValuesToAdd = Math.max(0, Math.min(newValues.length, end - start))
  if (nValuesToAdd === 0) {
    return
  }

  end = start + nValuesToAdd
  if (end >= array.length) {
    const padding = new Array(end - array.length).fill(null)
    array.push(...padding)
  }
  array.splice(start, nValuesToAdd, ...newValues)
}

export function getOrDefault<T>(value: T | undefined | null, defaultValue: T): T {
  if (value) {
    return value
  }
  return defaultValue
}
