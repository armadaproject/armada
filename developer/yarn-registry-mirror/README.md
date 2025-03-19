# Yarn registry mirror

It may be necessary to use a mirror of the Yarn registry when you are
developing. `yarn.sh` in this directory provides a wrapper around the `yarn`
command to support this, while ensuring `yarn.lock` files in the repository keep
using the public Yarn registry.

It sets Yarn to use your desired registry mirror from your `ARMADA_NPM_REGISTRY`
environment variable, then in `yarn.lock` files, replaces this mirror with
`https://registry.yarnpkg.com`.

You can use this script in place of the `yarn` command - for example,
`developer/yarn-registry-mirror/yarn.sh add is-thirteen`.
