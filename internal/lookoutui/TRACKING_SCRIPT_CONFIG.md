# Example Lookout UI Configuration with Tracking Script

To enable a tracking script (like Umami, Google Analytics, Plausible, etc.) in the Lookout UI, add the `trackingScript` configuration to your Lookout configuration YAML file. Many analytics solutions operate by inserting `<script>` tags inside the `<head>` and adding information to other tags through HTML attributes or css classes for event tracking.

## trackingScript Schema

```yaml
uiConfig:
  # ... other UI configuration ...

  trackingScript:
    scripts: # list of <script> that will be added to <head>
      - content: | # content of <script>
          console.log("Inline script example");
        attributes: # HTML attributes of <script>
          src: "https://analytics.yourdomain.com/script.js"
          type: "text/javascript"
          defer: "true"
    method: attribute | class # specify if analytic solution uses HTML attributes or css class
    eventAttribute: "data-foo-event" # analytic solution base identifier for events
    dataAttribute: "data-foo-data" # analytic solution base identifier for data associated with an event
```

This will result in a tracking script being added to the `<head>` element

```html
<head>
  ...
  <script src="https://analytics.yourdomain.com/script.js" defer="true" type="text/javascript">
    console.log("Inline script example")
  </script>
</head>
```

Event tracking where `eventName=Something Clicked` and `eventData={yourEvent: 1}`

```html
<!-- HTML Attribute method -->
<button type="button" data-foo-event="Something Clicked" data-foo-data-yourevent="1">Something</button>

<!-- CSS class method -->
<button type="button" class="data-foo-event=Something+Clicked data-foo-data-yourevent=1">Something</button>
```

### CSS Method

Css based event tracking

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
    method: "attribute"
    eventAttribute: "data-umami-event"
    dataAttribute: "data-umami-event"
```

Follow [Umami docs](https://umami.is/docs) on how to run and set up an instance of Umami. Details for `<script>` tag are in the [Tracking code](https://umami.is/docs/collect-data) section.

### Plausible

```yaml
uiConfig:
  trackingScript:
    scripts:
      - attributes:
           src: "https://analytics.yourdomain.com/js/script_name.js"
           async: "true"
           defer: "true"
       - content: |
           window.plausible=window.plausible||function(){(plausible.q=plausible.q||[]).push(arguments)},plausible.init=plausible.init||function(i){plausible.o=i||{}};
           plausible.init({
           endpoint: "https://analytics.yourdomain.com/api/event",
           });
     method: "class"
     eventAttribute: "plausible-event-name"
     dataAttribute: "plausible-event"
```

Follow [Plausible docs](https://plausible.io/docs) on how to run and set up an instance of Plausible Community Edition. Details to get the snippet with the `<script>` tags are [here](https://plausible.io/docs/plausible-script).

## Tracking Component

This is a universal component that adds tracking attributes to any component based on the configured analytics provider.

```tsx
import { Tracking } from "src/components/analytics/Tracking"
import { Button, Tab, Link } from "@mui/material"

// With Button
<Tracking
  component={Button}
  eventName="Cancel Job"
  eventData={{ jobId: job.id }}
  variant="contained"
>
  Cancel
</Tracking>

// With Tab - eventName can be shared between tabs, eventData differentiates them
<Tracking
  component={Tab}
  label="Details"
  value="details"
  eventName="Sidebar Tab View"
  eventData={{ tab: "details" }}
/>

// With Link
<Tracking component={Link} href="/jobs" eventName="Navigate to Jobs">
  View Jobs
</Tracking>

// With any MUI or HTML component
<Tracking component="div" eventName="Custom Action" eventData={{ foo: "bar" }}>
  Any component
</Tracking>
```

The `Tracking` component:

- Takes a `component` prop specifying what component to render
- Automatically adds the correct tracking attributes (data attributes or CSS classes) based on your configuration
- Wraps the onClick handler to dispatch tracking events
- Supports full TypeScript type safety for the component's props
- Merges classNames for class-based tracking systems

## Notes

- Script attributes like `defer` and `async` should be specified in the `attributes` map
- Scripts with `content` are inline scripts; scripts with `src` in attributes are external scripts
- The script is injected dynamically when the app loads
- If no `trackingScript` configuration is provided, no tracking script will be loaded
- All attributes in the `attributes` map will be added to the `<script>` tag as HTML attributes
