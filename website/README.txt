This branch is the github pages repo for armadaproject.io. This document
contains tips and conventions about how to develop the website interactively.


Conventions
-----------

Integration with master branch
==============================
The Armada master branch contains github workflows that will copy over some
files when committed to that branch. Ensure no changes you are making here
will be overwritten on the next master push, and make changes in the master
branch when appropriate.

Interesting Files/Directories
=============================
- All markdown files in the root of the branch are rendered as webpages, by
  default at the path `/blah` for a given file `blah.md` (but this behavior
  can be overridden by jekyll headers).
- `_pages/` is the pages collection, as defined in `_config.yml` -- generally
  speaking, it's best to leave the markdown files in the root directory
  instead unless you have a specific reason. 
- `_layouts/` contains layouts which can be used to format markdown files.
  - `default.html` is the default layout, used by almost all pages.
  - `redirect.html` is a layout designed to 301 redirect based on headers.
- `_includes/` includes various files included to build the page. such as
  headers, footers, and similar.
- `_data/settings.yml` is an additional settings file, which contains the
  tree used to render the navigation bar.


Testing
-------
armadaproject.io is a jekyll generated site, using github pages. You have two
choices for running it locally for testing.

Local testing with Docker
========================

Using Docker is the simplest way to render the site and preview locally:

- run `docker run -p 4000:4000 -v $(pwd):/site bretfisher/jekyll-serve`
- navigate to `http://localhost:4000`

Local testing (native)
======================

The below is a summary of how to test locally. The canonical documentation
for doing this is in Github docs:
https://docs.github.com/en/pages/setting-up-a-github-pages-site-with-jekyll/testing-your-github-pages-site-locally-with-jekyll.

Prerequisites
^^^^^^^^^^^^^
- Ensure you have ruby and bundler available.
- Access to a rubygem repository.

Initial configuration
^^^^^^^^^^^^^^^^^^^^^
- Run `bundle install` from the root of this repository.
- Run `bundle exec jekyll serve` to serve the website locally (defaults to
  127.0.0.1:4000). This will update as files served beneath it update.

Notes/Maintenance
^^^^^^^^^^^^^^^^^
It's neccessary from time to time to upgrade the github-pages gem in the
bundle. The latest github-pages gem is always used to render by github,
so we should always test with the latest when possible.

- `bundle update github-pages`
- Commit the resulting updates to `Gemfile`, if needed.

Note: Never commit `Gemfile.lock`. The specific versions of gems used may
vary by platform. The important part is that we're installing the gems via
github-pages.

Testing via github
==================
If you push the contents of gh-pages branch to your fork's gh-pages branch,
the page will be published at the following URL:
https://{ghusername}.github.io/armada. 
