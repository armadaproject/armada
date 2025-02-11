export const appendAuthorizationHeaders = (headers: Headers, accessToken: string) => {
  headers.append("Authorization", `Bearer ${accessToken}`)
  return headers
}
