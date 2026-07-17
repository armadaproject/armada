# Armada Website Readme

The Armada documentation site is built with [Next.js](https://nextjs.org) and [Fumadocs](https://fumadocs.dev). All content is written in MDX, Markdown that can include React components. 

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

The live site is deployed to GitHub Pages under a base path. If your changes involve links, images, or assets and you want to make sure they resolve correctly in that environment, copy `.env.example` to `.env.local` and set the base path before running the preview:

```bash
cp .env.example .env.local
```
Then open `.env.local` and follow the instructions inside. You do not need this for most content changes: only if you are working on routing, assets, or the Next.js config itself.

## How the site works

Every page on the site is an `.mdx` file under `content/`. MDX is Markdown that can also use React components. You write normal Markdown and drop in components (cards, callouts, custom JSX) where you need them.

### Content structure

content/
├── index.mdx           # Homepage / landing page
├── getting-started.mdx # Quickstart guide
├── meta.json           # Root nav configuration
├── docs/               # All documentation pages
│   ├── meta.json
│   ├── core-concepts.mdx
│   ├── developer-guide.mdx
│   └── ...
└── contribute/         # Contributing section
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

Please make sure to format and lint your code before committing:

```bash
yarn content:check
yarn spell:check
yarn format:fix
yarn lint:fix
```

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

To learn more about Fumadocs, take a look at the following resources:

- [Fumadocs](https://fumadocs.dev/) - learn about Fumadocs
