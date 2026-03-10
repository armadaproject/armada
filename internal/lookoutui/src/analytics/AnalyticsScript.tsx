import { useEffect } from "react"

import { AnalyticsConfig } from "../config/types"

interface AnalyticsScriptProps {
  config: AnalyticsConfig | undefined
  onReady: () => void
}

/**
 * Component that dynamically injects analytics scripts into the document head
 * based on the configuration provided. While this dynamically creates and injects script elements,
 * the script content and attributes come from server-controlled configuration (not user input)
 * making it safe to inject these scripts into the document head. Ensure that any analytics scripts
 * added to the configuration are from trusted sources and do not contain malicious code.
 */
export const AnalyticsScript = ({ config, onReady }: AnalyticsScriptProps) => {
  useEffect(() => {
    if (!config?.scripts || config.scripts.length === 0) {
      onReady()
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

    let cancelled = false

    const loadPromises = scriptElements
      .filter((s) => s.src)
      .map((script) => {
        return new Promise((resolve) => {
          script.addEventListener("load", resolve, { once: true })
          script.addEventListener("error", resolve, { once: true })
        })
      })

    Promise.allSettled(loadPromises).then(() => {
      if (!cancelled) {
        onReady()
      }
    })

    // Cleanup function to remove scripts when component unmounts
    return () => {
      cancelled = true
      scriptElements.forEach((script) => {
        if (script.parentNode) {
          document.head.removeChild(script)
        }
      })
    }
  }, [config, onReady])

  return null
}
