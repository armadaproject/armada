import { useEffect } from "react"

import { AnalyticsConfig } from "../config/types"

interface AnalyticsScriptProps {
  config: AnalyticsConfig | undefined
  onReady: () => void
}

// Module-level state to prevent re-injection during React Strict Mode remounts
let scriptsInjected = false
let scriptsReadyPromise: Promise<void> | null = null

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

    if (scriptsInjected) {
      if (scriptsReadyPromise) {
        scriptsReadyPromise.then(onReady)
      } else {
        onReady()
      }
      return
    }

    scriptsInjected = true

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

    scriptsReadyPromise = Promise.allSettled(loadPromises).then(() => undefined)

    scriptsReadyPromise.then(() => {
      if (!cancelled) {
        onReady()
      }
    })

    // Cleanup: cancel pending callbacks but don't remove scripts
    // Scripts persist for the page lifetime to avoid re-initialization during Strict Mode remounts
    return () => {
      cancelled = true
    }
  }, [config, onReady])

  return null
}
