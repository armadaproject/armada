import { useQuery } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { useAuthenticatedFetch } from "../../oidcAuth"

export interface VersionInfo {
  version: string
  commit: string
  buildTime: string
}

export const UNKNOWN_VERSION_INFO: VersionInfo = {
  version: "unknown",
  commit: "unknown",
  buildTime: "unknown",
}

export const useGetBackendVersion = () => {
  const authenticatedFetch = useAuthenticatedFetch()

  return useQuery<VersionInfo, string>({
    queryKey: ["getVersion"],
    queryFn: async ({ signal }) => {
      try {
        const response = await authenticatedFetch("/api/v1/version", { signal })
        const json = (await response.json()) as Partial<VersionInfo>
        return {
          version: json.version ?? "unknown",
          commit: json.commit ?? "unknown",
          buildTime: json.buildTime ?? "unknown",
        }
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    refetchOnMount: false,
    staleTime: Infinity,
  })
}
