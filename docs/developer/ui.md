# Armada UI

Armada bundles a web UI referred to as Lookout. To understand its components see the [Relationships Diagram](../design/relationships_diagram.md)

In short, Lookout is made of two components:

* Lookout API: a Go service that provides an API to the Armada backend
* Lookout UI: a React application that provides a web interface to the Lookout API

After running `mage localdev full` and `mage ui`, the Lookout UI should be accessible through your browser at `http://localhost:8089`

For UI development, you can also use the React development server and skip the build step. Note that the Lookout API service will
still have to be running for this to work. Browse to `http://localhost:3000` with this.
```bash
cd ./internal/lookout/ui
yarn run start
```

You can also re-build a production build of the UI by running `mage ui` in the root of the repo.