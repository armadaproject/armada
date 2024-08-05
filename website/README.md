# Website

This folder contains the Jekyll static website source code for [armadaproject.io](https://armadaproject.io). This
document contains tips and conventions about how to develop the website interactively.

# Conventions

## Build and deployment

The website is built and deployed using GitHub Actions. The workflow defined in `.github/workflows/deploy-website.yml`
is triggered on every push to the `master` branch that contains changes to the website source code or the documentation.
The workflow copies the `docs/` folder, builds the website using Jekyll, and deploys the generated files to GitHub
Pages.

## Interesting Files/Directories

- All markdown files in the `docs/` folder are rendered as webpages (except `docs/README.md`), by
  default at the path `/blah` for a given file `blah.md` (but this behavior can be overridden by Front-Matter headers).
- To add a new page, create a new markdown file in the `docs/` folder.
- `_pages/` is the pages collection, as defined in `_config.yml` -- generally speaking, to add a new page you should
  create a new markdown file in the `docs/` folder, but you can also add a new page here if you have good reasons to do
  so.
- `_layouts/` contains layouts which can be used to format markdown files.
    - `default.html` is the default layout, used by almost all pages.
    - `redirect.html` is a layout designed to 301 redirect based on headers.
- `_includes/` includes various partials used to build the website, such as
  headers, footers, and similar.
- `_data/settings.yml` is an additional settings file, which contains the
  tree used to render the navigation bar.

# Testing

[armadaproject.io](https://armadaproject.io) is a Jekyll generated site, using GitHub Pages. You have two
choices for running it locally for testing.

## Local testing with Docker

Using Docker is the simplest way to render the site and preview locally:

- Run `./copy.sh` to copy documentation files to the project root.
- Run `docker run -p 4000:4000 -v $(pwd):/site bretfisher/jekyll-serve`.
- Navigate to `http://localhost:4000`.

## Local testing (native)

The below is a summary of how to test locally. The canonical documentation
for doing this is in GitHub docs:
https://docs.github.com/en/pages/setting-up-a-github-pages-site-with-jekyll/testing-your-github-pages-site-locally-with-jekyll.

### Prerequisites

- Ensure you have Ruby and Bundler available.
- Access to a RubyGems repository.

### Initial configuration

- Run `./copy.sh` to copy documentation files to the project root.
- Run `bundle install` from the root of this repository.
- Run `bundle exec jekyll serve` to serve the website locally (defaults to
  127.0.0.1:4000). This will update as files served beneath it update.

### Notes/Maintenance

It's necessary from time to time to upgrade the `github-pages` gem in the
bundle. The latest `github-pages` gem is always used to render by GitHub,
so we should always test with the latest when possible.

- `bundle update github-pages`
- Commit the resulting updates to `Gemfile`, if needed.

Note: Never commit `Gemfile.lock`. The specific versions of gems used may
vary by platform. The important part is that we're installing the gems via
`github-pages`.

## Testing via GitHub

If you push the contents of the `gh-pages` branch to your fork's `gh-pages` branch,
the page will be published at the following URL:
`https://{ghusername}.github.io/armada`.

> Make sure to use relative URLs when developing, to allow the website to work on forks.
