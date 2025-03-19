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
variable and use `developer/yarn-registry-mirror/yarn.sh` in place of the `yarn`
command. You may find it helpful to create an alias in your shell config, for
example:

```bash
alias yarn='~/armada/developer/yarn-registry-mirror/yarn.sh'
```

Please see [its README](../../developer/yarn-registry-mirror/README.md) for more
details.

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
(please see [the main developer docs](../../../docs/developer/ui.md) for details
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

### Run typechecker

Ensure that the types in TypeScript source and test files are correct.

```bash
yarn typecheck
```

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
React in production mode and optimizes the build for the best performance.

You can then run a server to serve this production bundle locally on
[http://localhost:4173](http://localhost:4173):

```bash
yarn serve
```

In the same way as for `yarn dev`, you may supply a `PROXY_TARGET` environment
variable. The same requirements apply for the Lookout instance to which requests
are proxied (for `localhost:4173` instead of `localhost:3000`).
