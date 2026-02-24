import { ComponentPropsWithRef, ElementType, forwardRef, MouseEvent, ReactNode, useCallback, JSX } from "react"

import { getConfig } from "../config"

import { buildAnalyticsAttributes } from "./utils"

type AnalyticsProps<C extends ElementType> = {
  /**
   * The component to render with analytics attributes
   */
  component: C
  /**
   * The event name to track when the component is interacted with
   */
  eventName: string
  /**
   * Optional event data to include with the analytics event
   */
  eventData?: Record<string, string>
  /**
   * Optional children for the component
   */
  children?: ReactNode
} & ComponentPropsWithRef<C>

/**
 * A universal analytics component that adds analytics attributes to any component
 * based on the configured analytics provider.
 *
 * Supports:
 * - Attribute-based analytics (e.g., Umami): adds HTML attributes like data-umami-event="Event Name"
 * - Class-based analytics (e.g., Plausible): adds CSS classes like plausible-event-name=Event+Name
 *
 * Usage:
 * ```tsx
 * import { Button, Tab } from "@mui/material"
 *
 * // With Button
 * <Analytics component={Button} eventName="Cancel Job" eventData={{ jobId: job.id }}>
 *   Cancel
 * </Analytics>
 *
 * // With Tab
 * <Analytics
 *   component={Tab}
 *   label="Details"
 *   value="details"
 *   eventName="Sidebar Tab View"
 *   eventData={{ tab: "details" }}
 * />
 *
 * // With Link
 * <Analytics component={Link} href="/jobs" eventName="Navigate to Jobs">
 *   View Jobs
 * </Analytics>
 * ```
 */
export const Analytics = forwardRef(
  <C extends ElementType>(
    { component, eventName, eventData, children, className, onClick, ...props }: AnalyticsProps<C>,
    ref: ComponentPropsWithRef<C>["ref"],
  ) => {
    const Component = component
    const config = getConfig()
    const analyticsConfig = config.analyticsScript

    const analyticsAttributes = analyticsConfig ? buildAnalyticsAttributes(eventName, analyticsConfig, eventData) : {}

    // Merge className if using class-based analytics
    const mergedClassName =
      analyticsAttributes.className && className
        ? `${className} ${analyticsAttributes.className}`
        : analyticsAttributes.className || className

    // Wrap onClick to dispatch custom event for analytics scripts that need it
    const handleClick = useCallback(
      (event: MouseEvent<HTMLElement>) => {
        // Dispatch custom event for analytics scripts that need it
        if (analyticsConfig) {
          const customEvent = new CustomEvent("analytics", { detail: analyticsAttributes })
          window.dispatchEvent(customEvent)
        }

        // Call original onClick if provided
        if (onClick) {
          onClick(event as never)
        }
      },
      [onClick, analyticsAttributes, analyticsConfig],
    )

    return (
      <Component ref={ref} {...props} {...analyticsAttributes} className={mergedClassName} onClick={handleClick}>
        {children}
      </Component>
    )
  },
) as <C extends ElementType>(props: AnalyticsProps<C>) => JSX.Element
