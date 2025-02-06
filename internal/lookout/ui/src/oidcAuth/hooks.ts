import { useCallback, useContext, useEffect, useState } from "react"

import { UserManager } from "oidc-client-ts"

import { OidcAuthContext } from "./OidcAuthContext"

export const useUserManager = (): UserManager | undefined => useContext(OidcAuthContext)?.userManager

export const useUsername = (): string | null => {
  const userManager = useUserManager()
  const [username, setUsername] = useState<string | null>(null)
  useEffect(() => {
    if (!userManager) {
      return
    }

    ;(async () => {
      const user = await userManager.getUser()
      if (!user) {
        return
      }

      setUsername(user.profile.sub)
    })()
  }, [userManager])

  return username
}

export const useGetAccessToken = () => {
  const userManager = useUserManager()

  return useCallback(async () => {
    if (!userManager) {
      return undefined
    }

    const user = await userManager.getUser()
    if (!user || user.expired) {
      await userManager.signinRedirect()
      return undefined
    }

    return user.access_token
  }, [userManager])
}
