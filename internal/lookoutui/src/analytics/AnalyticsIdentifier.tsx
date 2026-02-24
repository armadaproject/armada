import { useIdentifyUserForAnalytics } from "./hooks"

export const AnalyticsIdentifier = () => {
  useIdentifyUserForAnalytics()
  return null
}
