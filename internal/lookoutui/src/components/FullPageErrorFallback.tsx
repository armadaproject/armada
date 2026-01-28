import { FallbackProps } from "react-error-boundary"

import { ErrorPage } from "./ErrorPage"

export const FullPageErrorFallback = ({ error }: FallbackProps) => (
  <ErrorPage
    error={error}
    errorTitle="Something went wrong on this page"
    errorContextMessage="This is an unexpected error. Please reach out to the maintainers of Armada for help with this problem."
  />
)
