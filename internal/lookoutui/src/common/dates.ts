export const validDateFromNullableIsoString = (isoString: string | null) => {
  if (!isoString) {
    return null
  }
  const date = new Date(isoString)
  if (isNaN(date.valueOf())) {
    return null
  }
  return date
}
