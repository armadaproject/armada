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

## Format, Lint Content, Lint Code and Spell Check

Please make sure to format and lint your code before committing:

```bash
yarn content:check
yarn spell:check
yarn format:fix
yarn lint:fix
```

## How the site works

Every page on the site is an `.mdx` file under `content/`. MDX is Markdown that
can also use React components. You write normal Markdown, and you can drop in
components (cards, callouts, custom JSX) where you need them.

The homepage is `content/index.mdx`. It is a content page like any other — there
is no separate "landing page" route. See "The homepage is a docs page" below for
the one way it is treated specially.

### The left sidebar nav comes from `meta.json`

The left-hand navigation is the **page tree**, built by the Fumadocs source
loader from `meta.json` files inside `content/`.

- The root `content/meta.json` defines the top-level nav and has `"root": true`.
- A `meta.json` inside a folder controls that folder's title, order, and which
  pages appear.
- A folder with **no** `meta.json` is invisible to the nav — the pages exist but
  are not listed. (Handy to know if a page you created isn't showing up: check
  whether its folder is declared in a `meta.json`.)

So to change what appears in the sidebar — order, grouping, labels — you edit
`meta.json`, not the page files.

### The right "On this page" TOC comes from Markdown headings

The table of contents on the right is generated automatically from the Markdown
headings in each page — the `##` and `###` lines. Nothing else feeds it.

This has one important consequence when you write custom layouts:

- A real Markdown heading (`## Features` on its own line) **becomes a TOC entry.**
- A heading-looking `<span>` or `<p>` you styled to look like a heading does
  **not** appear in the TOC; it's just text to Fumadocs.



## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

To learn more about Fumadocs, take a look at the following resources:

- [Fumadocs](https://fumadocs.dev/) - learn about Fumadocs
