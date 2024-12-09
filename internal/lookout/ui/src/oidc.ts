import { createContext, useContext } from "react"

import { UserManager } from "oidc-client-ts"

export const UserManagerContext = createContext<UserManager | undefined>(undefined)

export function useUserManager(): UserManager | undefined {
  return useContext(UserManagerContext)
}

export async function getAccessToken(userManager: UserManager): Promise<string> {
  const user = await userManager.getUser()

  if (user !== null && !user.expired) {
    return user.access_token
  }

  try {
    await userManager.signinRedirect()
  } catch (err) {
    console.error("Error during sign-in redirect:", err)
    throw err
  }

  const redirectedUser = await userManager.getUser()
  if (redirectedUser !== null) {
    return redirectedUser.access_token
  }

  throw new Error("Failed to obtain access token after sign-in redirect")
}

export function getAuthorizationHeaders(accessToken: string): Headers {
  const headers = new Headers()
  headers.append("Authorization", `Bearer ${accessToken}`)
  return headers
}
