# Armada - documentation website

This is a [Next.js](https://nextjs.org) project, based on the [Fumadocs](https://fumadocs.dev) framework, bootstrapped
using:

- [`npx create-next-app@latest`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).
- [`yarn create fumadocs-app`](https://github.com/fuma-nama/fumadocs).

## Requirements

- Node.js >= 20.x
- Yarn ~1.22.22

## Installation

```shell
yarn install
```

## Local Development

```bash
yarn dev
```

Open http://localhost:3000 on your browser to see the result.

## Build and Preview

To build the project for production and preview it, run:

```bash
yarn build
# then
yarn preview
```

The preview server will start on http://localhost:3000 by default. It also supports base path configuration to mimic
the GitHub Pages environment. Check the `.env.example` file to see how to set it up.

## Format, Lint Content and Lint Code

Please make sure to format and lint your code before committing:

```bash
yarn content:check
yarn format:fix
yarn lint:fix
```

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

To learn more about Fumadocs, take a look at the following resources:

- [Fumadocs](https://fumadocs.dev/) - learn about Fumadocs
