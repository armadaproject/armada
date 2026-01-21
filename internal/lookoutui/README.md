# Lookout UI

The Lookout UI is a single-page web application written in
[TypeScript](https://www.typescriptlang.org/) with [React](https://react.dev/),
and is built using [Vite](https://vitejs.dev/).

## Development

### Pre-requisites

Developing the Lookout UI requires the following to be installed on your
machine:

1. [Node.js](https://nodejs.org/en/download/package-manager)
1. [Yarn](https://yarnpkg.com/getting-started/install) - this should usually
   just involve running `corepack enable`
1. [Docker](https://www.docker.com/)

#### Yarn and registry mirrors

If you need to use a mirror of the Yarn registry instead of
_registry.yarnpkg.com_, you can set the `ARMADA_NPM_REGISTRY` environment
variable and use `scripts/yarn-registry-mirror.sh` in place of the `yarn`
command. You may find it helpful to create an alias in your shell config, for
example:

```bash
alias yarn='~/armada/scripts/yarn-registry-mirror.sh'
```

There are more details about this in the bash file.

### Installing dependencies and generating OpenAPI client code

First, install all packages depended on by this web app using Yarn. In this
directory, run:

```bash
yarn
```

Generate the OpenAPI client code from the OpenAPI specification. This step is
required to be run before the first time the application is run, and any time
the OpenAPI specification is updated.

```bash
yarn openapi
```

### Live development server

You can run a Vite development server to see your changes in the browser in
real-time. This serves the web app on
[http://localhost:3000](http://localhost:3000). It proxies API requests to the
target defined by your `PROXY_TARGET` environment variable, or otherwise your
locally-running instance of the Lookout server at `http://localhost:10000`
(please see [the main developer docs](https://armadaproject.io/developer-guide) for details
of how to set this up).

```bash
# Proxy requests to your locally-running Lookout server
yarn dev

# Proxy API requests to your staging environment
PROXY_TARGET=https://your-lookout-staging-environment.com yarn dev
```

You should ensure the following for the instance of the Lookout server to which
you are proxying API requests:

- if OIDC authentication is enabled, the OIDC client allows redirects to
  `http://localhost:3000/oidc`
- the configured endpoints for the following services allow requests from the
  `http://localhost:3000` origin in their responses' CORS headers (set in the
  `applicationConfig.corsAllowedOrigins` path in their config file):
  - Armada API
  - Armada Binoculars

### Run unit tests

Unit tests are run using [Vitest](https://vitest.dev/).

```bash
yarn test --run
```

If you are actively changing unit tests or code covered by unit tests, you may
find it useful to omit `--run` to continuously run affected tests.

### Lint

Formatting and linting is done using [Prettier](https://prettier.io/) and
[ESLint](https://eslint.org/).

```bash
yarn lint
```

You can add the flag, `--fix` to automatically fix issues where possible.

## Building the application for production

```bash
yarn build
```

This builds the app for production to the `build` folder. It correctly bundles
React in production mode and optimises the build for the best performance.

You can then run a server to serve this production bundle locally on
[http://localhost:4173](http://localhost:4173):

```bash
yarn serve
```

In the same way as for `yarn dev`, you may supply a `PROXY_TARGET` environment
variable. The same requirements apply for the Lookout instance to which requests
are proxied (for `localhost:4173` instead of `localhost:3000`).

## Directory structure

### `public/`

`public/` contains static files to be served, which are referenced by the UI.

### `src/`

`src/` contains the source code for the React application.

Inside `src/`:

- `app/` defines the application frame
- `common/` contains logic and utilities relevant for multiple pages across the
  app
- `components/` contains React components for use in multiple pages across the
  app
- `models/` defines data models used in the app
- `openapi/` contains generated OpenAPI client code
- `pages/` contains the pages for the app.
  - Each page directory contains a `SomethingPage.tsx` file which is the React
    component for the page, and a `components/` directory containing components
    used on that page which are specific to it.
- `services/` contains functions and React hooks for making API calls. We use
  [TanStack Query](https://tanstack.com/query/latest/docs/framework/react/overview)
  for fetching, updating and caching back end requests to our APIs.
- `theme/` defines the MUI theme for the application, which determines elements
  of its overall visual look.
- `index.tsx` is the entrypoint into the React application.

#### About `index.ts(x)` files

Directories which primarily export a piece of logic or a particular component
should contain an `index.ts(x)` file which re-exports things intended to be
consumed outside of it.

Consider the following example:

```text
bar/
  - baz.ts
foo/
  - logging/
      - auth.ts
      - formatting.ts
      - index.ts
      - loggers.ts
      - types.ts
```

`foo/logging/index.ts`:

```ts
export { ConsoleLogger as Logger } from "./logging.ts"
export { type LogLine } from "./types.ts"
```

We want to import the logger defined within `foo/logging/` in `bar/baz.ts`.

This is okay:

```ts
import { Logger, type LogLine } from "../foo/logging"
```

This is discouraged:

```ts
import { authenticateLogger } from "../foo/logging/auth" // authenticateLogger is not intended to be used outside of foo/logging
import { Logger } from "../foo/logging/loggers" // should import from '../foo/logging' instead
```
