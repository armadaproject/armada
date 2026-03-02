import { useEffect } from "react"

import { analyticsConfig } from "../config/types"

interface analyticsProps {
  config: analyticsConfig | undefined
}

/**
 * Component that dynamically injects analytics scripts into the document head
 * based on the configuration provided.
 */
export const AnalyticsScript = ({ config }: analyticsProps) => {
  useEffect(() => {
    if (!config?.scripts || config.scripts.length === 0) {
      return
    }

    const scriptElements: HTMLScriptElement[] = []

    config.scripts.forEach((scriptTag) => {
      const script = document.createElement("script")

      if (scriptTag.content) {
        script.textContent = scriptTag.content
      }

      if (scriptTag.attributes) {
        Object.entries(scriptTag.attributes).forEach(([key, value]) => {
          script.setAttribute(key, value)
        })
      }

      document.head.appendChild(script)
      scriptElements.push(script)
    })

    // Cleanup function to remove scripts when component unmounts
    return () => {
      scriptElements.forEach((script) => {
        if (script.parentNode) {
          document.head.removeChild(script)
        }
      })
    }
  }, [config])

  return null
}
