# Magefiles Developer Setup - Motivation

## Current Setup

We currently have these methods for running armada:

- [docs/dev](https://github.com/armadaproject/armada/tree/master/docs/dev) - which is depreciated and replaced with localdev
- [localdev](https://github.com/armadaproject/armada/tree/master/localdev) - with its own configs and docker compose file
- [docs/local](https://github.com/armadaproject/armada/tree/master/docs/local) - which uses the helm charts
- The Operator https://github.com/armadaproject/armada-operator
- [Magefiles](https://github.com/armadaproject/armada/tree/master/magefiles) - with usage specified in [docs/development_guide.md](https://github.com/armadaproject/armada/blob/master/docs/development_guide.md)
- CI: which uses a set of commands based on mage that could deviate from what is written in [docs/development_guide.md](https://github.com/armadaproject/armada/blob/master/docs/development_guide.md)

## Why Magefiles are the way forward

Taken from https://magefile.org/

> Makefiles are hard to read and hard to write. Mostly because makefiles are essentially fancy bash scripts with significant white space and additional make-related syntax.

> Mage lets you have multiple magefiles, name your magefiles whatever you want, and they’re easy to customize for multiple operating systems. Mage has no dependencies (aside from go) and runs just fine on all major operating systems, whereas make generally uses bash which is not well supported on Windows. Go is superior to bash for any non-trivial task involving branching, looping, anything that’s not just straight line execution of commands. And if your project is written in Go, why introduce another language as idiosyncratic as bash? Why not use the language your contributors are already comfortable with?

## Ideal Setup

The aim should be to get to this:

- The Operator
- Helm Charts
- A shared simplified Magefile command that can be shared by users and the CI.

This would allow for us to have one set of configs and a single compose file in the repo, as well as get our docs, scripts and magefiles to match as much as physically possible.

This should definitely be possible, since as of https://github.com/armadaproject/armada/pull/2098 localdev can run in local and remote enviroments with no config changes required. This will include github codespaces and https://coder.com/.

## What is missing

Currently, the mage file docs and scripts are only really setup to run the server and executor, and debugging is mentioned in [docs/development_guide.md](https://github.com/armadaproject/armada/blob/master/docs/development_guide.md)

I think this should be extended to do what localdev does in the following ways (Since https://github.com/armadaproject/armada/pull/2098):

- Support optionally building the lookout UI
- Support running all the armada components

I think the current magefile approach of using the armada images makes sense for the following reasons:

- We should encourage users to use debugging methods for all development (See the next section)
- It will greatly speed up the build time since no compiling needs to happen (Especially on systems with low CPU - github codespaces gets maxed on 100% for over 10 minutes trying to compile)

## Debugging Support Implementaition

We should, from mage commands, support tearing down and starting individual components to be replaced with debugging components.

We would have example configuration on different debugging methods for different IDE's in a docs folder.

For example in docs/debugging

- /vscode/guide.md
- /jetbrains/guide.md
- etc...

This means by default a full armada instance is created, and then users have the choice to teardown individual components that can then be debugged by a method of the users chosing.

Depending on how easy it is, we could also potentially support one of these methods by default for automatic switching, potentially https://github.com/go-delve/delve using a feature flag

```bash
mage teardown executor # For normal teardown to be replaced normally

mage debug executor # Debug with Delve and switch automatically
```

## What to remove after these changes

After this is implemented, The following files / folders can be replaced with new and improved docs.

- [docs/dev](https://github.com/armadaproject/armada/tree/master/docs/dev) - Supported with this new setup
- [localdev](https://github.com/armadaproject/armada/tree/master/localdev) - Supported with this new setup

- [docs/aws-ec2](https://github.com/armadaproject/armada/tree/master/docs/aws-ec2) - Should be replaced with a more general "Cloud Guide" that covers remote development more widely.
- [docs/developer.md](https://github.com/armadaproject/armada/blob/master/docs/developer.md) and [docs/developer_guide.md](https://github.com/armadaproject/armada/blob/master/docs/development_guide.md) should both be replaced with a docs/developer folder that includes debugging guides, magefiles setup and CI info.
- [docs/local](https://github.com/armadaproject/armada/tree/master/docs/local) - Should be moved to within [docs/quickstart](https://github.com/armadaproject/armada/tree/master/docs/quickstart) since it is a part of that.

