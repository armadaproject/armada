import { getConfig } from "../config"

// resolveMirrorOrigin returns the origin to mirror Lookout API requests to, or
// undefined if mirroring is disabled or misconfigured. The target must be
// HTTPS: mirrored requests carry the user's Authorization header, which must
// never be sent to an untrusted or plaintext origin.
const resolveMirrorOrigin = (): string | undefined => {
  const requestMirror = getConfig().requestMirror
  if (!requestMirror?.enabled || !requestMirror.targetUrl) {
    return undefined
  }
  try {
    const url = new URL(requestMirror.targetUrl)
    return url.protocol === "https:" ? url.origin : undefined
  } catch {
    return undefined
  }
}

const mirrorOrigin = resolveMirrorOrigin()

// isLookoutApiRequest reports whether a request targets the Lookout server's
// own REST API. The Lookout API is served same-origin under /api/; requests to
// the Armada server (config.armadaApiBaseUrl) and Binoculars
// (config.binocularsBaseUrlPattern) use absolute URLs to other origins and are
// deliberately excluded, since only Lookout API load is being evaluated.
const isLookoutApiRequest = (url: URL): boolean =>
  url.origin === window.location.origin && url.pathname.startsWith("/api/")

// mirrorLookoutApiRequest duplicates an outgoing Lookout API request to the
// configured mirror backend (e.g. an experimental deployment under performance
// evaluation), so it can observe real production query patterns without
// affecting the user. Requests that are not Lookout API requests are ignored.
// It is fire-and-forget: the mirrored response is discarded and failures are
// silently ignored.
export const mirrorLookoutApiRequest = (input: RequestInfo | URL, init: RequestInit | undefined): void => {
  if (!mirrorOrigin) {
    return
  }

  try {
    const requestUrl = new URL(input instanceof Request ? input.url : input.toString(), window.location.href)
    if (!isLookoutApiRequest(requestUrl)) {
      return
    }

    const targetUrl = `${mirrorOrigin}${requestUrl.pathname}${requestUrl.search}`

    fetch(targetUrl, {
      ...init,
      credentials: "include",
    }).catch(() => undefined)
  } catch {
    // Silently ignore mirror failures
  }
}
