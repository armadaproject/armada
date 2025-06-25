# Armada UI
- [Armada UI](#armada-ui)
  - [Local development](#local-development)
    - [Using fake data](#using-fake-data)
    - [Enabling the debug view](#enabling-the-debug-view)
    - [Enabling query parameters](#enabling-query-parameters)

Armada bundles a web UI referred to as Lookout. To understand its components [see the Relationships Diagram](../design/relationships_diagram.md).

Lookout consists of two components:

* Lookout API: a Go service that provides an API to the Armada backend
* Lookout UI: a React application that provides a web interface to the Lookout API

After running `mage localdev full` and `mage ui`, you can access the Lookout UI through your browser at `http://localhost:8089`.

You can also rebuild a production build of the UI by running `mage ui` in the root of the repo.

## Local development

To start developing on the UI locally without having to build:

1. Ensure you have Docker, Node and Yarn installed for your platform.
2. Run the following commands:

```bash
cd ./internal/lookoutui
yarn
yarn openapi
yarn dev
```

This starts a live development server on [http://localhost:3000](http://localhost:3000).

For this to work, the Lookout API and other Armada components need to run as well. However, you can use fake data to develop locally without requiring any other component to run.

### Using fake data

To use fake data, go to the V2 tab, then specify `fakeData=` in the query parameters.
Alternatively, you can go to [http://localhost:3000/v2?fakeData=](http://localhost:3000/v2?fakeData=).

### Enabling the debug view

To enable the debug view, add the `debug=` query parameter to the URL.
Alternatively, you can go to [http://localhost:3000/v2?debug=](http://localhost:3000/v2?debug=).

This displays the TanStack table state as a JSON string, at the bottom of the page.
Make sure you scroll down to see it.

### Enabling query parameters

To ensure both the `fakeData` and `debug` query parameters are enabled at the same time, go to [http://localhost:3000/v2?fakeData=&debug=](http://localhost:3000/v2?fakeData=&debug=).

To learn more about UI development, see the [README for the Lookout UI](../../internal/lookoutui/README.md) for more information about UI development.
