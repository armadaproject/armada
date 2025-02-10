import { createContext } from "react"

import { UserManager } from "oidc-client-ts"

export interface OidcAuthContextProps {
  userManager: UserManager | undefined
}

export const OidcAuthContext = createContext<OidcAuthContextProps | undefined>(undefined)
