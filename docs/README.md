# Docs

This folder contains the documentation for the Armada project. The documentation is written in markdown and is rendered
as webpages on [armadaproject.io](https://armadaproject.io). It's accessible from the IDE, GitHub, and the website.

## For Developers

See [website.md](./developer/website.md)

## Overview

Docs added to this the `docs/` folder are automatically copied into [armadaproject.io](https://armadaproject.io).

For example, if you wanted to document bananas, and you added `bananas.md`,
once committed to master that would be published at
`https://armadaproject.io/bananas/`.

> [!NOTE]  
> All files in `docs/` folder are rendered as webpage except this `README.md` file.

## Pages with assets

If you'd like to add a more complex page, such as one with images or other
linked assets, you have to be careful to ensure links will work both
for people viewing in GitHub and for those viewing via [armadaproject.io](https://armadaproject.io).

The easiest way to accomplish this is by using page bundles. Assets should be located inside the `docs/` folder and
used in the markdown file with relative paths.

## Removing pages

Any page that is removed from the `docs/` folder will be removed from the website automatically. The `docs/` folder is
the source of truth for the website's content.
