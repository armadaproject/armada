import React from "react"

import { UserManager } from "oidc-client-ts"

export const UserManagerContext = React.createContext<UserManager | undefined>(undefined)

export function useUserManager(): UserManager | undefined {
  return React.useContext(UserManagerContext)
}

export async function getAccessToken(userManager: UserManager): Promise<string> {
  const user = await userManager.getUser()
  if (user !== null && !user.expired) return user.access_token
  return (await userManager.signinPopup()).access_token
}

export function getAuthorizationHeaders(accessToken: string): Headers {
  const headers = new Headers()
  headers.append("Authorization", `Bearer ${accessToken}`)
  return headers
}
