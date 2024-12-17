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
[http://localhost:3000](http://localhost:3000), and proxies API requests to a
locally-running instance of the Lookout API. Please see
[the main developer docs](../../../docs/developer/ui.md) for details of how to
set this up.

```bash
yarn dev
```

### Run unit tests

Unit tests are run using [Vitest](https://vitest.dev/).

```bash
yarn test --watch=false
```

If you are actively changing unit tests or code covered by unit tests, you may
find it useful to omit `--watch=false` to continuously run affected tests.

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
