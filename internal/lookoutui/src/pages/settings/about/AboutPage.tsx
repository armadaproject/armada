import { Warning } from "@mui/icons-material"
import { Alert, Link, Stack, Tooltip, Typography } from "@mui/material"

import { SPACING } from "../../../common/spacing"
import { useGetBackendVersion, UNKNOWN_VERSION_INFO, VersionInfo } from "../../../services/lookout/useGetBackendVersion"

const DEV = "dev"
const UNKNOWN = "unknown"

const REPO_URL = "https://github.com/armadaproject/armada"

const isReleaseVersion = (version: string) => version !== "" && version !== DEV && version !== UNKNOWN

const releaseUrl = (version: string) => `${REPO_URL}/releases/tag/${version}`

const getFrontendVersionInfo = (): VersionInfo => ({
  version: import.meta.env.VITE_APP_VERSION || DEV,
  commit: import.meta.env.VITE_APP_COMMIT || UNKNOWN,
  buildTime: import.meta.env.VITE_APP_BUILD_TIME || UNKNOWN,
})

interface VersionRowProps {
  label: string
  info: VersionInfo
}

const VersionRow = ({ label, info }: VersionRowProps) => (
  <div>
    <Typography variant="overline" color="text.secondary">
      {label}
    </Typography>
    <Typography variant="h5" component="div">
      {isReleaseVersion(info.version) ? (
        <Link href={releaseUrl(info.version)} target="_blank" rel="noopener noreferrer">
          {info.version}
        </Link>
      ) : (
        info.version
      )}
    </Typography>
    <Typography variant="body2" color="text.secondary">
      commit {info.commit} &middot; built {info.buildTime}
    </Typography>
  </div>
)

export const AboutPage = () => {
  const { data: backendVersion, isLoading, isError } = useGetBackendVersion()

  const frontendVersionInfo = getFrontendVersionInfo()
  const resolvedBackendVersion: VersionInfo = backendVersion ?? UNKNOWN_VERSION_INFO

  const showLoadingPlaceholder = isLoading
  const showFetchErrorAlert = isError

  const versionsKnownAndDiffer =
    !showLoadingPlaceholder &&
    isReleaseVersion(frontendVersionInfo.version) &&
    isReleaseVersion(resolvedBackendVersion.version) &&
    frontendVersionInfo.version !== resolvedBackendVersion.version

  return (
    <Stack spacing={SPACING.md}>
      <div>
        <Typography variant="h4">About</Typography>
      </div>
      <div>
        <Typography>Release information for this Lookout deployment.</Typography>
      </div>
      {showFetchErrorAlert && (
        <Alert severity="warning">Could not load the backend version. The frontend version is shown below.</Alert>
      )}
      <VersionRow label="Frontend" info={frontendVersionInfo} />
      <Stack direction="row" spacing={SPACING.sm} alignItems="flex-start">
        <VersionRow label="Backend" info={showLoadingPlaceholder ? UNKNOWN_VERSION_INFO : resolvedBackendVersion} />
        {versionsKnownAndDiffer && (
          <Tooltip title="The frontend and backend versions differ. A rolling deploy may be in progress." arrow>
            <Warning role="img" aria-label="Frontend and backend versions differ" fontSize="small" color="warning" />
          </Tooltip>
        )}
      </Stack>
    </Stack>
  )
}
