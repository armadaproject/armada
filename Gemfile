source "https://rubygems.org"

# Use GitHub Pages locally to match the GitHub Pages server environment.
# To upgrade, run `bundle update github-pages`.
gem "github-pages", "~> 232", group: :jekyll_plugins
# See: https://pages.github.com/versions/ for available Jekyll plugins.

# Windows and JRuby does not include zoneinfo files, so bundle the tzinfo-data gem
# and associated library.
platforms :mingw, :x64_mingw, :mswin, :jruby do
  gem "tzinfo", "~> 1.2"
  gem "tzinfo-data"
end

# Performance-booster for watching directories on Windows
gem "wdm", "~> 0.1.1", :platforms => [:mingw, :x64_mingw, :mswin]

# Requiring this version of webrick permits this to work under ruby 3.x.
gem "webrick", "~> 1.9"
