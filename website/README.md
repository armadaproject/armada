# Armada Website Readme

The Armada documentation site is built with [Next.js](https://nextjs.org) and [Fumadocs](https://fumadocs.dev). All content is written in MDX, Markdown that can include React components.

This README owns website authoring, preview, checks and publication instructions. For contribution policy, see [CONTRIBUTING.md](../CONTRIBUTING.md). Published content lives in [`content/`](content/); repository `docs/` files are not automatically copied into the site. The published developer setup page is [`content/docs/developer-guide.mdx`](content/docs/developer-guide.mdx), a concise entrypoint that must agree with the [detailed local-development reference](../docs/developer_guide.md).

## Requirements

Before you start, make sure you have the following installed:

- **Node.js** >= 20.x — [Download](https://nodejs.org)
- **Yarn** ~1.22.22 — install with `npm install -g yarn` if you don't have it

## Getting started

All commands should be run from inside the `website/` folder. If you are at the root of the `armada` repository, navigate there first:

```bash
cd website
```

### Install dependencies

```bash
yarn install
```

### Start the local dev server

```bash
yarn dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser. The page will hot-reload as you edit files — you do not need to restart the server after making changes to content or components.

### GitHub Pages base path

The normal armadaproject.io deployment and local development use an empty base path. The workflow sets `NEXT_PUBLIC_BASE_PATH=/<repository-name>` for forks and repositories outside the `armadaproject` organization. To check links, images and assets with that prefix, copy `.env.example` to `.env.local`:

```bash
cp .env.example .env.local
```

Set `NEXT_PUBLIC_BASE_PATH` to the repository prefix (for example `/armada`, without a trailing slash). The example already sets `/armada`; remove it or set it to an empty string to return to the normal root-path preview. Use the same environment for the build and static preview, and open the prefixed URL, for example `http://localhost:3000/armada/`.

## How the site works

Every page on the site is an `.mdx` file under `content/`. MDX is Markdown that can also use React components. You write normal Markdown and drop in components (cards, callouts, custom JSX) where you need them.

### Content structure

content/
├── index.mdx # Homepage / landing page
├── getting-started.mdx # Quickstart guide
├── meta.json # Root nav configuration
├── docs/ # All documentation pages
│ ├── meta.json
│ ├── core-concepts.mdx
│ ├── developer-guide.mdx
│ └── ...
└── contribute/ # Contributing section
├── meta.json
└── ...

### How the left sidebar nav works

The left sidebar is driven entirely by `meta.json` files — Fumadocs reads them at build time and constructs the page tree from them.

**Root nav — `content/meta.json`**

**Section nav — `content/docs/meta.json`**

A `meta.json` inside a folder controls that section's title, page order, and which pages appear.

**To add a page to the nav:**

1. Create the `.mdx` file in the right folder
2. Add the filename (without `.mdx`) to the relevant `meta.json` pages array

**To remove a page from the nav:**
Remove it from `meta.json`.

**To reorder pages:**
Change the order in the `meta.json` pages array. Top to bottom = top to bottom in the sidebar.

**Important:** A folder with no `meta.json` is completely invisible to the nav. The pages exist and are routable URLs but won't appear in the sidebar. If a page you created isn't showing up, check whether its folder has a `meta.json` and whether that file is listed in it.

### Right TOC

Generated automatically from `##` and `###` Markdown headings only. JSX elements and styled `<div>`/`<span>` tags do not appear in the TOC regardless of how they look visually.

### The homepage

The homepage is `content/index.mdx` — a content page like any other, served by the `[[...slug]]` catch-all route. The `_(home)` route group in `src/app/` handles the root `/` path and renders `index.mdx` directly.

### The `not-prose` rule

Fumadocs applies typography styles to all MDX content by default. Any custom JSX layout block — a hero section, a card grid, a CTA — needs the `not-prose` class on its outermost element, otherwise Fumadocs' prose styles will override your custom styles:

```mdx
<div className='not-prose flex flex-col items-center'>
  {/* your custom layout here */}
</div>
```

### Markdown links inside JSX

Markdown link syntax (`[text](url)`) does not render inside JSX elements. Use anchor tags instead:

```mdx
{/* This won't work inside a JSX div */}
[CNCF](https://cncf.io)

{/* Use this instead */}

<a href='https://cncf.io'>CNCF</a>
```

## Format, Lint Content, Lint Code and Spell Check

Run the same checks as the website workflow before committing:

```bash
yarn content:check
yarn spell:check
yarn format:check
yarn lint:check
yarn build
```

Use `yarn format:fix` or `yarn lint:fix` to apply fixes, then review the diff and rerun the checks. These commands cover the website directory; avoid including unrelated formatting changes.

After `yarn build`, run `yarn preview` to serve the static export from `out/` on port 3000. Follow changed pages, navigation links and assets in the browser, including the contributor and developer entrypoints. Content checks do not validate route-shaped links or external URLs, so check those separately.

## Publication

The [website workflow](../.github/workflows/website.yml) validates website pull requests targeting `master`. For matching pushes to `master`, it builds the static export in `website/out/` and uploads and deploys it through GitHub Actions when GitHub Pages is enabled for the repository. The workflow can also be run manually.

Edit sources on your contribution branch and submit a pull request; do not edit a `gh-pages` branch. To remove a published page, remove its source and update the relevant navigation and incoming links in the same change. Removing only its `meta.json` entry hides it from navigation but leaves its URL available.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

To learn more about Fumadocs, take a look at the following resources:

- [Fumadocs](https://fumadocs.dev/) - learn about Fumadocs
