import { getConfig } from "../config"
import { initialiseSentry } from "./sentry"

const { sentry } = getConfig().errorMonitoring

if (sentry) {
  initialiseSentry(sentry)
}
