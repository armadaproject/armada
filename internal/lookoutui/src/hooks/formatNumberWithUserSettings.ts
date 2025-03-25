import { useCallback } from "react"

import { formatNumber } from "../common/formatNumber"
import { useFormatNumberLocale, useFormatNumberNotation, useFormatNumberShouldFormat } from "../userSettings"

export const useFormatNumberWithUserSettings = () => {
  const [shouldFormatNumber] = useFormatNumberShouldFormat()
  const [locale] = useFormatNumberLocale()
  const [notation] = useFormatNumberNotation()

  return useCallback(
    (n: number) => (shouldFormatNumber ? formatNumber(n, { locale, notation }) : n.toString()),
    [shouldFormatNumber, locale, notation],
  )
}
