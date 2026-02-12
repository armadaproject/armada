# Example Lookout UI Configuration with Tracking Script

To enable a tracking script (like Umami, Google Analytics, Plausible, etc.) in the Lookout UI, add the `trackingScript` configuration to your Lookout configuration YAML file:

```yaml
uiConfig:
  # ... other UI configuration ...

  trackingScript:
    scripts:
      - content: |
          console.log("Inline script example");
        attributes:
          type: "text/javascript"
      - attributes:
          src: "http://localhost:3000/script.js"
          data-website-id: "b92c8526-afb0-46c3-85d4-6878632efb01"
          defer: "true"
    eventAttribute: "data-foo-event" # add css attribute for tracking events
    dataAttribute: "data-foo-event" # add css attribute for tracking event payloads
    trackedEvents: # list of eventNames to track
      - "Something Clicked"
```

## Examples

### Umami Analytics

```yaml
uiConfig:
  trackingScript:
    scripts:
      - attributes:
          src: "https://analytics.yourdomain.com/script.js"
          data-website-id: "your-website-id"
          defer: "true"
    eventAttribute: "data-umami-event"
    dataAttribute: "data-umami-event"
    trackedEvents:
      - "Cancel Jobs Clicked"
      - "Reprioritize Jobs Clicked"
```

### OpenPanel

```yaml
uiConfig:
  trackingScript:
    scripts:
      - content: |
          window.op=window.op||function(){var n=[];return new Proxy(function(){arguments.length&&n.push([].slice.call(arguments))},{get:function(t,r){return"q"===r?n:function(){n.push([r].concat([].slice.call(arguments)))}} ,has:function(t,r){return"q"===r}}) }();
          window.op('init', {
            apiUrl: 'https://your-domain.com/api',
            clientId: 'YOUR_CLIENT_ID',
            trackScreenViews: true,
            trackOutgoingLinks: true,
            trackAttributes: true,
          });
        attributes:
          type: "text/javascript"
      - attributes:
          src: "https://openpanel.dev/op1.js"
          defer: "true"
          async: "true"
    eventAttribute: "data-track"
    dataAttribute: "data"
    trackedEvents:
      - "Cancel Jobs Clicked"
      - "Reprioritize Jobs Clicked"
```

This will result in a tracking script being added to the `<head>` element and tracking attributes being added to elements

```html
<head>
  ...
  <script src="https://analytics.yourdomain.com/script.js" defer="true" data-website-id="your-website-id"></script>
</head>
...
<button type="button" data-umami-event="Something Clicked" data-umami-event-yourevent="1">Something</button>
```

Follow [Umami docs](https://umami.is/docs) on how to run and set up an instance of Umami. The `src` and `data-website-id` are in the <b>Tracking code</b> section.

## TrackingButton Component

This component wraps MUI Button and adds tracking attributes

```tsx
import { TrackingButton } from "src/components/analytics/TrackingButton.tsx"

// Simple usage
<TrackingButton eventName="Something Clicked">
  Something
</TrackingButton>

// With event data
<TrackingButton
  eventName="Submit Job"
  eventData={{ foo: "bar", baz: "5" }}
  ...
>
  Submit
</TrackingButton>
```

## TackingTab Component

This component wraps the MUI Tab and adds tracking attributes, the eventName can be shared between multiple tabs and eventData can be used to track which tab was accessed

```tsx
import { TrackingTab } from "/analytics/TrackingTab"

<TabsContainer>
  <SidebarTabs>
    <TrackingTab>
      eventName="Sidebar Tab"
      eventData={{ tab: "Info Page" }}
      ...
      />
    <TrackingTab>
      eventName="Sidebar Tab"
      eventData={{ tab: "Contact Page" }}
      ...
      />
  </SidebarTab>
</TabsContainer>
```

## Notes

- Script attributes like `defer` and `async` should be specified in the `attributes` map
- Scripts with `content` are inline scripts; scripts with `src` in attributes are external scripts
- The script is injected dynamically when the app loads
- If no `trackingScript` configuration is provided, no tracking script will be loaded
- All attributes in the `attributes` map will be added to the `<script>` tag as HTML attributes
