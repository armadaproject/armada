import { createRoot } from "react-dom/client"
import { ErrorBoundary } from "react-error-boundary"

import { App } from "./app/App"
import { FullPageErrorFallback } from "./components/FullPageErrorFallback"
import { reactErrorHandlers } from "./errorMonitoring"

import "./index.css"

const container = document.getElementById("root")

if (container === null) {
  throw new Error('DOM element with ID "root" was not found')
}

createRoot(container, { ...reactErrorHandlers }).render(
  <ErrorBoundary FallbackComponent={FullPageErrorFallback}>
    <App />
  </ErrorBoundary>,
)
