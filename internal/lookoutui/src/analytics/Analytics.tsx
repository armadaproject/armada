import { ComponentPropsWithRef, ElementType, forwardRef, ReactNode, JSX } from "react"

import { getConfig, analyticsConfig } from "../config"

type AnalyticsProps<C extends ElementType> = {
  component: C
  eventName: string
  eventData?: Record<string, string>
  children?: ReactNode
} & ComponentPropsWithRef<C>

function buildAnalyticsAttributes(
  eventName: string,
  analyticsConfig: analyticsConfig,
  eventData?: Record<string, string>,
): Record<string, string> {
  const method = analyticsConfig.method || "attribute"

  if (method === "class") {
    const classes: string[] = []

    if (analyticsConfig.eventAttribute) {
      const eventValue = eventName.replace(/\s/g, "+")
      classes.push(`${analyticsConfig.eventAttribute}=${eventValue}`)
    }

    if (analyticsConfig.dataAttribute && eventData) {
      Object.entries(eventData).forEach(([key, value]) => {
        const propValue = value.replace(/\s/g, "+")
        classes.push(`${analyticsConfig.dataAttribute}-${key.toLowerCase()}=${propValue}`)
      })
    }

    return { className: classes.join(" ") }
  } else {
    const analyticsAttributes: Record<string, string> = {}
    if (analyticsConfig.eventAttribute) {
      analyticsAttributes[analyticsConfig.eventAttribute] = eventName
    }
    if (analyticsConfig.dataAttribute && eventData) {
      Object.entries(eventData).forEach(([key, value]) => {
        analyticsAttributes[`${analyticsConfig.dataAttribute}-${key.toLowerCase()}`] = value
      })
    }

    return analyticsAttributes
  }
}

/**
 * Component that wraps any element and injects properties for analytics
 *
 * @param {C} component - The component to render with analytics attributes
 * @param {string} eventName - The event name to track when the component is interacted with
 * @param {Record<string, string>} [eventData] - Optional event data to include with the analytics event
 * @param {ReactNode} [children] - Optional children for the component
 *
 * @example
 * <Analytics component="button" eventName="click_submit" eventData={{ form: "login" }}>
 *   Submit
 * </Analytics>
 */
export const Analytics = forwardRef(
  <C extends ElementType>(
    { component, eventName, eventData, children, className, ...props }: AnalyticsProps<C>,
    ref: ComponentPropsWithRef<C>["ref"],
  ) => {
    const Component = component
    const config = getConfig()
    const analyticsConfig = config.analytics

    const analyticsAttributes = analyticsConfig ? buildAnalyticsAttributes(eventName, analyticsConfig, eventData) : {}

    // Merge className if using class-based analytics
    const mergedClassName =
      analyticsAttributes.className && className
        ? `${className} ${analyticsAttributes.className}`
        : analyticsAttributes.className || className

    return (
      <Component ref={ref} {...props} {...analyticsAttributes} className={mergedClassName}>
        {children}
      </Component>
    )
  },
) as <C extends ElementType>(props: AnalyticsProps<C>) => JSX.Element
