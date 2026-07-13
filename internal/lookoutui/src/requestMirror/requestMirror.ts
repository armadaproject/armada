import { getConfig } from "../config"

const requestMirror = getConfig().requestMirror

// mirrorRequest duplicates an outgoing API request to the configured mirror
// backend (e.g. an experimental deployment under performance evaluation), so
// it can observe real production query patterns without affecting the user.
// It is fire-and-forget: the mirrored response is discarded and failures are
// silently ignored.
export const mirrorRequest = (input: RequestInfo | URL, init: RequestInit | undefined): void => {
  if (!requestMirror?.enabled || !requestMirror.targetUrl) {
    return
  }

  try {
    const originalUrl = new URL(input instanceof Request ? input.url : input.toString(), window.location.href)
    const mirrorOrigin = new URL(requestMirror.targetUrl).origin
    const targetUrl = `${mirrorOrigin}${originalUrl.pathname}${originalUrl.search}`

    const headers = new Headers(init?.headers)
    headers.set("X-Mirrored-Request", "true")

    fetch(targetUrl, {
      ...init,
      headers,
      credentials: "include",
      keepalive: true,
    }).catch(() => undefined)
  } catch {
    // Silently ignore mirror failures
  }
}
