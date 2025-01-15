# Docs Readme

## For Developers

See [website.md](./developer/website.md)

## Overview
Docs added to this directory are automatically copied into armadaproject.io.

For example, if you wanted to document bananas, and you added `bananas.md`,
once committed to master that would be published at
`https://armadaproject.io/bananas/`.

## Complex pages with assets
If you'd like to add a more complex page, such as one with images or other
linked assets, you have to be very careful to ensure links will work both
for people viewing in github and for those viewing via armadaproject.io.

The easiest way to accomplish this is by using page bundles. See quickstart
as as example: quickstart/index.md is the actual content, with links to
various images using relative pathing; e.g. `./my-image.png`. This is
considered a page bundle by jekyll (github pages) and are rendered as a
single page at `quickstart/`.

In order to get this page bundle pushed to gh-pages branch, you'll need
to adjust the github workflow in `.github/workflows/pages.yml` to add your
new page bundle as well.

## Removing pages
If you put a commit here to remove a page, you will need to also commit
to the gh-pages branch to remove that page.
