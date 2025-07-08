# Docs README
- [Docs README](#docs-readme)
  - [Adding complex pages with assets](#adding-complex-pages-with-assets)
  - [Removing pages](#removing-pages)

Pages added to this directory are automatically copied into [armadaproject.io](https://armadaproject.io). To learn more, [see the repo README](https://github.com/armadaproject/armada/blob/gh-pages/README.txt).

For example, let's say you wanted to document bananas. If you added `bananas.md` and committed it to `master`, the page would be published at `https://armadaproject.io/bananas/`.

## Adding complex pages with assets

When adding more complex pages (for example, with images or other linked assets), make sure that any links will work both for both GitHub and armadaproject.io users.

The easiest way to accomplish this is by using page bundles. Let's say `quickstart/index.md` contains the actual content, with links to
various images using relative pathing, for example, `./my-image.png`. This is considered a page bundle by Jekyll (GitHub pages), and is rendered as a
single page at `quickstart/`.

To get this page bundle pushed to `gh-pages` branch, you'll need to adjust the GitHub workflow in `.github/workflows/pages.yml` to add your
new page bundle as well.

## Removing pages

If you push a commit to remove a page, you also need to commit the `gh-pages` branch to ensure page removal.
