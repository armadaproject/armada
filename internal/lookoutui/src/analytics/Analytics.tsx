import { ComponentPropsWithRef, ElementType, forwardRef, ReactNode, JSX } from "react"

import { getConfig } from "../config"

import { trackAnalyticsEvent } from "./trackAnalyticsEvent"
import { AnalyticsEventName } from "./types"

const config = getConfig()

type AnalyticsProps<C extends ElementType> = {
  component: C
  eventName: AnalyticsEventName
  eventData?: Record<string, string>
  children?: ReactNode
} & ComponentPropsWithRef<C>

/**
 * Component that wraps any element and injects properties for analytics
 *
 * @param {C} component - The component to render with analytics tracking
 * @param {AnalyticsEventName} eventName - The event name to track when the component is interacted with
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
    const analyticsConfig = config.analytics

    if (!analyticsConfig) {
      return (
        <Component ref={ref} {...props} className={className}>
          {children}
        </Component>
      )
    }

    const { onClick: existingOnClick, ...restProps } = props as any
    const handleClick = (event: any) => {
      trackAnalyticsEvent(eventName, eventData)

      if (existingOnClick) {
        existingOnClick(event)
      }
    }

    return (
      <Component ref={ref} {...restProps} onClick={handleClick} className={className}>
        {children}
      </Component>
    )
  },
) as <C extends ElementType>(props: AnalyticsProps<C>) => JSX.Element
