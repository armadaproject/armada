How to develop on this python client project 
=
<hr />

Armada Project welcomes contributions from the opensource community.
### This client is written primarily targeting supported versions PYTHON 3 on Linux, Mac, and WSL on Windows.

### Setting up your own fork of this repository on Linux, Mac, or WSL OS.
<hr />

1) On github interface click on the `Fork` button.
2) Clone your forked of armada repository on your personal git repository by running `git clone github.com:Your_GitHub_Username/armada.git`.
3) Enter the directory `cd armada`
4) Add upstream repository `git remote add upstream https://github.com/G-Research/armada.git`.

## Install Pip
<hr />

1) Install [Pip](https://pypi.org/project/pip/) on your local machine by following the instructions on [python pip documentation](https://pip.pypa.io/en/stable/).

## Setting up your own environment
<hr />

1) Run `make python` then `cd client/python` and run `pip install .` to install the client and all dependencies. You need to re-run the `pip install` command as you make changes.

## Run test to ensure everything is working
<hr />

Run all the unit test int the python client by following the instructions [here](https://tox.wiki/en/latest/).

## Create a new branch to work on your contribution
<hr />

Run git checkout -b `Your_Branch_Name`.

## Push your contributions to your forked repository
<hr />

Run `git push origin Your_Branch_Name`.

## Making a pull request
<hr />

1) On github interface, click on `Pull Request` button.
2) Next click on `Create new pull request` button.
3) Wait for review on your Pull Request.

