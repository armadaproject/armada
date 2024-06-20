# Contributing

We want everyone to feel that they can contribute to the Armada Project.  Whether you have an idea to share, a feature to add, an issue to report, or a pull-request of the finest, most scintillating code ever, we want you to participate!

In the sections below, you'll find some easy way to connect with other Armada developers as well as some useful informational links on our license and community policies.

Looking forward to building Armada with you!

## Github

The main project fork lives here in Github:

* [https://github.com/armadaproject/armada](https://github.com/armadaproject/armada)

To work with [Armada website](https://armadaproject.io/), please checkout this branch:

* [https://github.com/armadaproject/armada/tree/gh-pages](https://github.com/armadaproject/armada/tree/gh-pages)

If you want to brainstorm a potential new feature, hop on over to the Discussions page, listed [below](#discussions).

## Contributing Guide

### Setup

Setup everything you’ll need to get started running and developing Armada:

* [Developer setup](https://armadaproject.io/developer)

### Issues

If you spot a bug, then please raise an issue in our main GitHub project:

* [https://github.com/armadaproject/armada/issues](https://github.com/armadaproject/armada/issues)

### Pull Requests

Likewise, if you have developed a cool new feature or improvement, then send us a pull request.
Please try and make sure that this is linked to an [issue](https://github.com/armadaproject/armada/issues).

**Please keep all pull requests on a separate branch with proper name!**

### Branches

#### Squashing fix-ups

Please try and squash any small commits to make the repo a bit cleaner. See a guide for doing this here:

* [https://thoughtbot.com/blog/autosquashing-git-commits](https://thoughtbot.com/blog/autosquashing-git-commits)

#### Branch Naming Scheme

Note the names of the branch must follow proper docker names:

>A tag name must be valid ASCII and may contain lowercase and uppercase letters, digits, underscores, periods and dashes. A tag name may not start with a period or a dash and may contain a maximum of 128 characters.

#### Signing Off Commits

To enhance the integrity of contributions to the Armada repository, we've adopted the use of the DCO (Developer Certificate of Origin) plug-in. This means that for every commit you contribute via Pull Requests, you'll need to sign off your commits to certify that you have the right to submit it under the open source license used by this project.

**Every commit in your PRs must have a "Signed-Off" attribute.**

When committing to the repository, ensure you use the `--signoff` option with `git commit`. This will append a sign-off message at the end of the commit log to indicate that the commit has your signature.

You sign-off by adding the following to your commit messages:

```
Author: Your Name <your.name@example.com>
Date:   Thu Feb 2 11:41:15 2018 -0800

    This is my commit message

    Signed-off-by: Your Name <your.name@example.com>
```

Notice the `Author` and `Signed-off-by` lines match. If they don't, the PR will
be rejected by the automated DCO check.

Git has a `-s` command line option to do this automatically:

    git commit -s -m 'This is my commit message'

If you forgot to do this and have not yet pushed your changes to the remote
repository, you can amend your commit with the sign-off by running 

    git commit --amend -s
    
This command will modify the latest commit and add the required sign-off.    

For more details checkout [DCO](https://github.com/apps/dco)


## Chat & Discussions

Sometimes, it's good to hash things out in real time.

Armada uses GH Discussions for long form communication and design discussions. To join the conversation there, go to Discussions:
* [https://github.com/armadaproject/armada/discussions](https://github.com/armadaproject/armada/discussions)

Real-time interactions between Armada developers and users occurs primarily in CNCF Slack. To join us there:
* If you already have an account on CNCF Slack, join #armada on [https://cloud-native.slack.com](https://cloud-native.slack.com).
* If you need an inviation to CNCF Slack, you can get one at [https://slack.cncf.io](https://slack.cncf.io).

## Finding Issues to Work On
If you're new to the project and looking for a place to start, we recommend checking out the issues tagged with "good first issues". These issues are specifically curated for newcomers to the project, providing an opportunity to get familiar with the codebase and make meaningful contributions.

You can view the list of [good first issues](https://github.com/armadaproject/armada/labels/good%20first%20issue) issues.

## Security

Armada developers appreciate and encourage coordinated disclosure of security vulnerabilities. If you believe you have a vulnerability to report, please contact the security team at [security@gr-oss.io](mailto:security@gr-oss.io) for triage.

## License

Armada is licensed with the Apache 2.0 license.  You can find it published here:

* [https://github.com/armadaproject/armada/blob/master/LICENSE](https://github.com/armadaproject/armada/blob/master/LICENSE)
