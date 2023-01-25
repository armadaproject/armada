# Contributing to the Armada Python Client

Armada Project welcomes contributions from the opensource community.

This client is written primarily targeting supported versions of python 3 on Linux, Mac, and WSL on Windows. Please file
an [issue](https://github.com/armadaproject/armada/issues/new) if you need support for another platform.

## First time contribution using a fork

Armada encourages using the [fork then PR](https://gist.github.com/Chaser324/ce0505fbed06b947d962)
workflow for contributing. First time contributors can follow the guide below to get started.

### Prerequisites
- A working knowledge of git and python development.
- Supported versions of `python`, `pip`, and `tox` are installed.
- A working docker client, or docker-compatible client available under `docker`.
- Network access to fetch docker images and go dependencies.

### Fork and clone repository
1) In Github interface click on the `Fork` button from the [Armada repository](https://github.com/armadaproject/armada).
2) Once forked, clone your copy of armada repository: `git clone github.com:Your_GitHub_Username/armada.git`.
3) Enter the directory your repository was cloned into: `cd armada`
4) Add upstream remote to your repository: `git remote add upstream https://github.com/armadaproject/armada.git`.

### Preparing repository for development
Unlike most python projects, the Armada python client contains a large quantity of generated code. This code must be
generated in order to compile and develop against the client.

From the root of the repository, run `make python`. This will generate python code needed to build
and use the client. This command needs to be re-run anytime an API change is committed (e.g. a change to a `*.proto`
file).

### Run tests to ensure environment is prepared
Unit tests, auto-formatting, and formatting checks are all run using `tox` as an orchestration tool. Tox manages a set
of virtual environments which allow testing under multiple python versions and install conditions.

The following commands should be run, and passing, at this point:
- `tox -e py39` will run unit tests with your default python 3.9 environment
- `tox -e format` will ensure formatting is compliant with linting tools and code formatters in use.

### Create a branch and work on your contribution
Code changes should be created in a branch, to isolate them from other work being performed and to allow you to later
request those changes be pulled into the primary repository.

As a general guideline, branch names should be descriptive and should include the issue number if one exists for the
code change you are working on.

To create, then switch to a new branch, run: `git checkout -b NewFeature12345` (replacing NewFeature12345 with an
appropriate branch name).

Now that you have a branch, you can begin making changes to the code. At any point, you can re-run the tests from above
to ensure you have not regressed any existing behavior. Also, as you make changes, be sure to run `tox -e format-code`
periodically to ensure the code is formatted correctly -- this not only runs linting tools, but it also runs a code
formatting tool, `black`, which will edit your code to ensure it is formatted correctly.

Once your code is complete, and passing tests, you're ready to commit it and push it back to github.

### Push your code to Github and create a pull request
Your code is in the repository, but is not yet been added to git. Adding a file to git tells it you want a new or
changed file to be committed into your branch. You can add files individually, with `git add filename`, or you can
add all files in a directory with `git add .`. For new contributors, it's suggested you add your changes a file at a 
time to ensure you do not accidentally add unintended files to the repository.

Once your files are staged for commit, which you can validate with `git status`, you need to commit the changes. This
is as simple as running `git commit`. It will then launch your default editor for you to provide a commit message. A
good commit message is a concise summary of the changes you are committing, that generally consists of a 50 character or
less title, an empty line, then a longer description wrapped at 70 characters per line.

Once your change is committed, it exists in your local copy of the repository, but we need to push it to your fork on
Github. This is as simple as running `git push origin NewFeature12345` (again, replacing NewFeature12345 with the name
chosen above).

### Creating and maintaining a pull request
There are two ways to create a pull request (PR). The simplest, and quickest way, is to use the link returned on the command
line the first time you push a branch to Github. This brings you to the PR creation screen directly.

Alternatively, you can create a PR manually following the
[instructions from Github](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

Once your PR is created, you will have to wait for a contributor to review your change. This can take anywhere from a
few minutes to a week or more, depending on the change and how busy the development team is. When your PR is reviewed
by a contributor, they will either "Approve" the change, indicating it is OK to merge (a reviewer may also merge your
change at this point), or they will "Request Changes" and put comments to indicate they would like to make changes to
the code. You can respond to these comments to answer or ask questions, or start a discussion about the code.

If you've determined the requested change needs to be made, you'll have to return to your repository and revise the PR
with additional commits until it's gotten to a state where it can be merged.

## Releasing the client
Armada-client releases are automated via Github Actions, for contributors with sufficient access to run them.

1) Commit and merge a change to `client/python/pyproject.toml` raising the version number the appropriate amount. We are 
   using [semver](https://semver.org/) for versioning.
2) Navigate to the [python client release workflow](https://github.com/armadaproject/armada/actions/workflows/python-client-release-to-pypi.yml)
   in Github workflows, click the "Run Workflow" button on the right side, and choose "master" as the branch to use the
   workflow from.
3) Once the workflow has completed running, verify the new version of Armada client has been uploaded to
   [PyPI](https://pypi.org/project/armada-client/).
