# Armada UI

Armada bundles a web UI referred to as Lookout. To understand its components see the [Relationships Diagram](../design/relationships_diagram.md)

In short, Lookout is made of two components:

* Lookout API: a Go service that provides an API to the Armada backend
* Lookout UI: a React application that provides a web interface to the Lookout API

After running `mage localdev full` and `mage ui`, the Lookout UI should be accessible through your browser at `http://localhost:8089`

You can also re-build a production build of the UI by running `mage ui` in the root of the repo.

## Local Development

To quickly get started developing on the UI locally without having to build, ensure you have Docker, NPM and Yarn installed for your platform, then run:

```bash
cd ./internal/lookout/ui
yarn install
yarn run openapi
yarn start
```

This will start a live development server on [http://localhost:3000](http://localhost:3000).

Note that this will require the Lookout V2 API and other Armada components to run as well in order to work.
However, you can use fake data to develop locally without requiring any other component to run.

In order to use fake data, navigate to the V2 tab, then specify `fakeData=` in the query parameters.
Or, navigate to [http://localhost:3000/v2?fakeData=](http://localhost:3000/v2?fakeData=)

You can also enable the debug view by adding the `debug=` query parameter to the URL.
Equivalently, navigate to [http://localhost:3000/v2?debug=](http://localhost:3000/v2?debug=)

This displays the TanStack table state as a JSON string, at the bottom of the page.
Make sure you scroll at the bottom to see it.

Note that you can have both `fakeData` and `debug` query parameters enabled at the same time: [http://localhost:3000/v2?fakeData=&debug=](http://localhost:3000/v2?fakeData=&debug=)
