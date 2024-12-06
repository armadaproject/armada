import { Dispatch, ReactNode, SetStateAction, useEffect, useState } from "react"

import { ThemeProvider, createTheme } from "@mui/material"
import { JobsTableContainer } from "containers/lookoutV2/JobsTableContainer"
import { SnackbarProvider } from "notistack"
import { UserManager, WebStorageStateStore, UserManagerSettings, User } from "oidc-client-ts"
import { BrowserRouter, Navigate, Route, Routes, useNavigate } from "react-router-dom"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { IGroupJobsService } from "services/lookoutV2/GroupJobsService"
import { UpdateJobSetsService } from "services/lookoutV2/UpdateJobSetsService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import { withRouter } from "utils"

import NavBar from "./components/NavBar"
import JobSetsContainer from "./containers/JobSetsContainer"
import { UserManagerContext, useUserManager } from "./oidc"
import { ICordonService } from "./services/lookoutV2/CordonService"
import { IGetJobInfoService } from "./services/lookoutV2/GetJobInfoService"
import { IGetRunInfoService } from "./services/lookoutV2/GetRunInfoService"
import { ILogService } from "./services/lookoutV2/LogService"
import { CommandSpec } from "./utils"
import { OidcConfig } from "./utils"

import "./App.css"

const theme = createTheme({
  palette: {
    primary: {
      main: "#00aae1",
      contrastText: "#fff",
    },
  },
  typography: {
    fontFamily: [
      "-apple-system",
      "BlinkMacSystemFont",
      "'Segoe UI'",
      "'Roboto'",
      "'Oxygen'",
      "'Ubuntu'",
      "'Cantarell'",
      "'Fira Sans'",
      "'Droid Sans'",
      "'Helvetica Neue'",
      "sans-serif",
    ].join(","),
  },
})

type AppProps = {
  customTitle: string
  oidcConfig?: OidcConfig
  v2GetJobsService: IGetJobsService
  v2GroupJobsService: IGroupJobsService
  v2RunInfoService: IGetRunInfoService
  v2JobSpecService: IGetJobInfoService
  v2LogService: ILogService
  v2UpdateJobsService: UpdateJobsService
  v2UpdateJobSetsService: UpdateJobSetsService
  v2CordonService: ICordonService
  jobSetsAutoRefreshMs: number | undefined
  jobsAutoRefreshMs: number | undefined
  debugEnabled: boolean
  commandSpecs: CommandSpec[]
}

// Handling authentication on page opening

interface AuthWrapperProps {
  children: ReactNode
  userManager: UserManager | undefined
  isAuthenticated: boolean
}

interface OidcCallbackProps {
  setIsAuthenticated: Dispatch<SetStateAction<boolean>>
}

export const UserManagerProvider = UserManagerContext.Provider

function OidcCallback({ setIsAuthenticated }: OidcCallbackProps): JSX.Element {
  const navigate = useNavigate()
  const userManager = useUserManager()
  const [error, setError] = useState<string | undefined>()

  useEffect(() => {
    if (!userManager) return
    userManager
      .signinRedirectCallback()
      .then(() => {
        setIsAuthenticated(true)
        navigate("/")
      })
      .catch((e) => {
        setError(`${e}`)
        console.error(e)
      })
  }, [navigate, userManager, setIsAuthenticated])

  if (error) return <p>Something went wrong; more details are available in the console.</p>
  return <p>Authenticating...</p>
}

function AuthWrapper({ children, userManager, isAuthenticated }: AuthWrapperProps) {
  useEffect(() => {
    if (!userManager || isAuthenticated) return // Skip if userManager is not available or user is already authenticated

    const handleAuthentication = async () => {
      try {
        const user = await userManager.getUser()
        if (!user || user.expired) {
          await userManager.signinRedirect()
        }
      } catch (error) {
        console.error("Error during authentication:", error)
      }
    }

    ;(async () => {
      await handleAuthentication()
    })()
  }, [userManager, isAuthenticated])

  return <>{children}</>
}

export function createUserManager(config: OidcConfig): UserManager {
  const userManagerSettings: UserManagerSettings = {
    authority: config.authority,
    client_id: config.clientId,
    redirect_uri: `${window.location.origin}/oidc`,
    scope: config.scope,
    userStore: new WebStorageStateStore({ store: window.localStorage }),
    loadUserInfo: true,
  }

  return new UserManager(userManagerSettings)
}

// Version 2 of the Lookout UI used to be hosted under /v2, so we try our best
// to redirect users to the new location while preserving the rest of the URL.
const V2Redirect = withRouter(({ router }) => <Navigate to={{ ...router.location, pathname: "/" }} />)

export function App(props: AppProps): JSX.Element {
  const [userManager, setUserManager] = useState<UserManager | undefined>(undefined)
  const [isAuthenticated, setIsAuthenticated] = useState(false)
  const [username, setUsername] = useState<string | undefined>(undefined)

  useEffect(() => {
    if (!userManager && props.oidcConfig) {
      const userManagerInstance = createUserManager(props.oidcConfig)
      setUserManager(userManagerInstance)

      userManagerInstance.getUser().then((user: User | null) => {
        if (user) {
          setUsername(user.profile.sub)
        }
      })
    }
  }, [props.oidcConfig])

  useEffect(() => {
    if (props.customTitle) {
      document.title = `${props.customTitle} - Armada Lookout`
    }
  }, [props.customTitle])

  const result = (
    <ThemeProvider theme={theme}>
      <SnackbarProvider anchorOrigin={{ horizontal: "right", vertical: "bottom" }} autoHideDuration={8000} maxSnack={3}>
        <BrowserRouter>
          <UserManagerProvider value={userManager}>
            <AuthWrapper userManager={userManager} isAuthenticated={isAuthenticated}>
              <div className="app-container">
                <NavBar customTitle={props.customTitle} username={username} />
                <div className="app-content">
                  <Routes>
                    <Route
                      path="/"
                      element={
                        <JobsTableContainer
                          getJobsService={props.v2GetJobsService}
                          groupJobsService={props.v2GroupJobsService}
                          updateJobsService={props.v2UpdateJobsService}
                          runInfoService={props.v2RunInfoService}
                          jobSpecService={props.v2JobSpecService}
                          logService={props.v2LogService}
                          cordonService={props.v2CordonService}
                          debug={props.debugEnabled}
                          autoRefreshMs={props.jobsAutoRefreshMs}
                          commandSpecs={props.commandSpecs}
                        />
                      }
                    />
                    <Route path="/job-sets" element={<JobSetsContainer {...props} />} />
                    <Route path="/oidc" element={<OidcCallback setIsAuthenticated={setIsAuthenticated} />} />
                    <Route path="/v2" element={<V2Redirect />} />
                    <Route
                      path="*"
                      element={
                        // This wildcard route ensures that users who follow old
                        // links to /job-sets or /jobs see something other than
                        // a blank page.
                        <Navigate to="/" />
                      }
                    />
                  </Routes>
                </div>
              </div>
            </AuthWrapper>
          </UserManagerProvider>
        </BrowserRouter>
      </SnackbarProvider>
    </ThemeProvider>
  )

  return result
}
