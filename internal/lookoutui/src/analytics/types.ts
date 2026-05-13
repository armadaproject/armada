export const ANALYTICS_EVENTS = {
  CANCEL_JOBS_CLICKED: "Cancel Jobs Clicked",
  PREEMPT_JOBS_CLICKED: "Preempt Jobs Clicked",
  REPRIORITIZE_JOBS_CLICKED: "Reprioritize Jobs Clicked",
  SIDEBAR_TAB_CLICKED: "Sidebar Tab Clicked",
  DOWNLOAD_LOGS_CLICKED: "Download Logs Clicked",
  MARK_NODE_UNSCHEDULABLE_CONFIRMED: "Mark Node Unschedulable Confirmed",
  CANCEL_JOB_SETS_CLICKED: "Cancel Job Sets Clicked",
  REPRIORITIZE_JOB_SETS_CLICKED: "Reprioritize Job Sets Clicked",
  SYSTEM_COLOR_MODE_TOGGLED: "System Color Mode Toggled",
  LIGHT_MODE_SELECTED: "Light Mode Selected",
  DARK_MODE_SELECTED: "Dark Mode Selected",
  COLUMN_CONFIGURATION_DIALOG_CLOSED: "Column Configuration Dialog Closed",
} as const

export type AnalyticsEventName = (typeof ANALYTICS_EVENTS)[keyof typeof ANALYTICS_EVENTS]
