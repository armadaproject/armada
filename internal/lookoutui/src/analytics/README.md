# Example Lookout UI Configuration with Analytics Script

To enable an analytics script (like Umami, Plausible, etc.) in the Lookout UI, add the `analytics` configuration to your Lookout configuration YAML file. This works for analytics providers that operate by inserting `<script>` tags inside the `<head>`.

## analytics Schema

```yaml
uiConfig:
  # ... other UI configuration ...

  analytics:
    scripts: # list of <script> that will be added to <head>
      - content: | # content of <script>
          console.log("Inline script example");
        attributes: # HTML attributes of <script>
          defer: "true"
          type: "text/javascript"
      - attributes:
          src: "https://analytics.yourdomain.com/script.js"
          type: "text/javascript"
          defer: "true"
    provider: foo # name of analytics providers in browsers window
    userIdentify:
      trackUsers: true # track users based on their oidc.profile.sub property
      identifyParam: distinctID # required parameter for analytics providers which use object for input
    customEventFunction: capture # name of function for custom events (defaults to track)
    dataWrapper: props # wraps eventData in a nested object (e.g., {props: eventData})
```

### Head Tag

This will result in a analytics script being added to the `<head>` element

```html
<head>
  ...
  <script defer="true" type="text/javascript">
    console.log("Inline script example")
  </script>
  <script src="https://analytics.yourdomain.com/script.js" defer="true" type="text/javascript"></script>
</head>
```

### Event Analytics

Event analytics where `eventName=Something Clicked` and `eventData={yourEvent: 1}`

```js
// where provider=foo and everything else is default
foo.track("Something Clicked", { yourEvent: 1 })

// where provider=foo, customEventFunction=capture
foo.capture("Something Clicked", { yourEvent: 1 })

// where provider=foo and is a function not an object and dataWrapper=props
foo("Something Clicked", { props: { yourEvent: 1 } })
```

### User Identification

Identify script execution

```js
// where provider=foo
foo.identify("user123")

// where provider=foo and identifyParam=distinctID
foo.identify({
  distinctID: "user123",
})
```

## Example Implementations

### Umami Analytics

```yaml
uiConfig:
  analytics:
    scripts:
      - attributes:
          src: "https://analytics.yourdomain.com/script.js"
          data-website-id: "your-website-id"
          defer: "true"
    provider: umami
    userIdentify:
      trackUsers: true
```

Follow [Umami docs](https://umami.is/docs) on how to run and set up an instance of Umami. Details for `<script>` tag are in the [Analytics code](https://umami.is/docs/collect-data) section.

### Plausible

```yaml
uiConfig:
  analytics:
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
    provider: plausible
    dataWrapper: props
```

Follow [Plausible docs](https://plausible.io/docs) on how to run and set up an instance of Plausible Community Edition. Details to get the snippet with the `<script>` tags are [here](https://plausible.io/docs/plausible-script).

## Analytics Component

This component sends analytics to the provider when the component is clicked.

```tsx
import { Analytics } from "src/analytics/Analytics"
import { Button, Tab, Link } from "@mui/material"

// Button
<Analytics
  component={Button}
  eventName="Cancel Job"
  eventData={{ jobId: job.id }}
  variant="contained"
>
  Cancel
</Analytics>

// Tab - eventName can be shared between tabs, eventData differentiates them
<Analytics
  component={Tab}
  label="Details"
  value="details"
  eventName="Sidebar Tab View"
  eventData={{ tab: "details" }}
/>

// Link
<Analytics component={Link} href="/jobs" eventName="Navigate to Jobs">
  View Jobs
</Analytics>

// With any MUI or HTML component
<Analytics component="div" eventName="Custom Action" eventData={{ foo: "bar" }}>
  Any component
</Analytics>
```

The `Analytics` component:

- Takes a `component` prop specifying what component to render (any HTML element or React component)
- Calls a function to send analytics information back to the provider
- Supports all props of the wrapped component with full TypeScript type safety
- Forwards refs to the underlying component

### Props

- `component` (required): The component or HTML element to render
- `eventName` (required): The event name to track
- `eventData` (optional): Additional key-value pairs to include with the event
- All other props are passed through to the underlying component

## Custom View Events

Custom view actions emit enriched metadata describing the view's configuration at the time of the action.

### Event Names

| Event | Trigger |
|-------|---------|
| `CUSTOM_VIEW_CREATED` | User saves current table configuration as a named view |
| `CUSTOM_VIEW_LOADED` | User loads a previously saved view |
| `CUSTOM_VIEW_DELETED` | User deletes a saved view |

### Event Data Schema

| Field | Type | Description |
|-------|------|-------------|
| `viewName` | string | Name of the custom view (empty string for "current" on create) |
| `columnCount` | string | Number of currently visible columns |
| `columns` | string | Delta from default columns: added columns listed by ID, removed defaults prefixed with `-` (e.g. `"Node,Priority,-Queue"`). Annotation columns always included. Empty = no customization. |
| `filterCount` | string | Number of active column filters |
| `filteredColumns` | string | Comma-joined column IDs that have active filters |
| `groupedColumns` | string | Comma-joined column IDs used for grouping |
| `groupCount` | string | Number of grouped columns |
| `sortColumn` | string | Current sort column ID |
| `sortDirection` | string | `"ASC"` or `"DESC"` |
| `pageSize` | string | Rows per page |
| `hasAnnotations` | string | `"true"` if any annotation columns exist |
| `annotationCount` | string | Number of annotation columns |
| `autoRefresh` | string | `"true"`, `"false"`, or `"unset"` |
| `activeJobSets` | string | `"true"`, `"false"`, or `"unset"` |
| `filterDetails` | string | JSON-stringified array of active filter objects. Each entry: `{ column, matchType, value }`. `matchType` is one of `"exact"` (default), `"startsWith"`, `"contains"`, `"greaterThan"`, `"lessThan"`, `"greaterThanOrEqualTo"`, `"lessThanOrEqualTo"`, `"anyOf"`, `"exists"`. Empty array `[]` when no filters are active. |

### Columns Delta Format

The `columns` field shows how the view differs from defaults:
- Columns visible in the view but hidden by default appear as-is (e.g. `Node`)
- Default columns hidden in the view appear prefixed with `-` (e.g. `-State`)
- Annotation columns (never in defaults) always appear (e.g. `annotation_team`)
- An empty string means no column customization from defaults

## Notes

- The script is injected dynamically when the app loads
- If no `analytics` configuration is provided, no analytics script will be loaded
