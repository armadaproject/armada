import { Refresh, Logout } from "@mui/icons-material"
import { Alert, Button, Stack, Typography } from "@mui/material"

import { SPACING } from "../../../common/spacing"
import { getConfig } from "../../../config"
import { useUserManager, useUsername } from "../../../oidcAuth"

const config = getConfig()

export const AccountPage = () => {
  const username = useUsername()
  const userManager = useUserManager()
  return (
    <Stack spacing={SPACING.md}>
      <div>
        <Typography variant="h4">Your account</Typography>
      </div>
      <div>
        <Alert severity="info">
          <Typography component="p" gutterBottom>
            {username === null ? (
              <>You are currently using Armada Lookout anonymously and are not signed in.</>
            ) : (
              <>
                You are currently logged in as <strong>{username}</strong>.
              </>
            )}
          </Typography>
          {config.oidc?.authority && (
            <Typography component="p" variant="body2">
              Your account is authenticated via <em>{config.oidc.authority}</em>.
            </Typography>
          )}
        </Alert>
      </div>
      {userManager && (
        <div>
          <Stack spacing={SPACING.sm} direction="row">
            <div>
              <Button
                startIcon={<Refresh />}
                variant="outlined"
                color="success"
                onClick={() => userManager.signinRedirect()}
              >
                Refresh credentials
              </Button>
            </div>
            <div>
              <Button
                startIcon={<Logout />}
                variant="outlined"
                color="error"
                onClick={() => userManager.signoutRedirect()}
              >
                Sign out {username}
              </Button>
            </div>
          </Stack>
        </div>
      )}
    </Stack>
  )
}
