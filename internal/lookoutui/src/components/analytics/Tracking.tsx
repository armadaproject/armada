import { ComponentPropsWithRef, ElementType, forwardRef, MouseEvent, ReactNode, useCallback, JSX } from "react"

import { buildTrackingAttributes } from "../../common/trackingAttributes"
import { getConfig } from "../../config"

type TrackingProps<C extends ElementType> = {
  /**
   * The component to render with tracking attributes
   */
  component: C
  /**
   * The event name to track when the component is interacted with
   */
  eventName: string
  /**
   * Optional event data to include with the tracking event
   */
  eventData?: Record<string, string>
  /**
   * Optional children for the component
   */
  children?: ReactNode
} & ComponentPropsWithRef<C>

/**
 * A universal tracking component that adds tracking attributes to any component
 * based on the configured analytics provider.
 *
 * Supports:
 * - Attribute-based tracking (e.g., Umami): adds HTML attributes like data-umami-event="Event Name"
 * - Class-based tracking (e.g., Plausible): adds CSS classes like plausible-event-name=Event+Name
 *
 * Usage:
 * ```tsx
 * import { Button, Tab } from "@mui/material"
 *
 * // With Button
 * <Tracking component={Button} eventName="Cancel Job" eventData={{ jobId: job.id }}>
 *   Cancel
 * </Tracking>
 *
 * // With Tab
 * <Tracking
 *   component={Tab}
 *   label="Details"
 *   value="details"
 *   eventName="Sidebar Tab View"
 *   eventData={{ tab: "details" }}
 * />
 *
 * // With Link
 * <Tracking component={Link} href="/jobs" eventName="Navigate to Jobs">
 *   View Jobs
 * </Tracking>
 * ```
 */
export const Tracking = forwardRef(
  <C extends ElementType>(
    { component, eventName, eventData, children, className, onClick, ...props }: TrackingProps<C>,
    ref: ComponentPropsWithRef<C>["ref"],
  ) => {
    const Component = component
    const config = getConfig()
    const trackingConfig = config.trackingScript

    const trackingAttributes = trackingConfig ? buildTrackingAttributes(eventName, trackingConfig, eventData) : {}

    // Merge className if using class-based tracking
    const mergedClassName =
      trackingAttributes.className && className
        ? `${className} ${trackingAttributes.className}`
        : trackingAttributes.className || className

    // Wrap onClick to dispatch custom event for tracking scripts that need it
    const handleClick = useCallback(
      (event: MouseEvent<HTMLElement>) => {
        // Dispatch custom event for tracking scripts that need it
        if (trackingConfig) {
          const customEvent = new CustomEvent("tracking", { detail: trackingAttributes })
          window.dispatchEvent(customEvent)
        }

        // Call original onClick if provided
        if (onClick) {
          onClick(event as never)
        }
      },
      [onClick, trackingAttributes, trackingConfig],
    )

    return (
      <Component ref={ref} {...props} {...trackingAttributes} className={mergedClassName} onClick={handleClick}>
        {children}
      </Component>
    )
  },
) as <C extends ElementType>(props: TrackingProps<C>) => JSX.Element
