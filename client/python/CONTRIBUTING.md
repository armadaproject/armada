How to develop on this python client project 
=
<hr />

Armada Project welcomes contributions from the opensource community.
### Note that you need PYTHON 3+ to get this project working.

## Setting up your own fork of this repository on linux, Mac, and WSL OS.
<hr />

1) On github interface click on the `Fork` button.
2) Clone your forked of armada repository on your personal git repository by running git clone `github.com:Your_Git_Username/armada.git`.
3) Enter the directory `cd armada`
4) Add upstream repository [Armada Repo](https://github.com/G-Research/armada.git)

## Install Pyenv/Python and Pip
<hr />

2) Install [Pyenv](https://github.com/pyenv/pyenv) by following the instructions [here](https://realpython.com/intro-to-pyenv/).
3) Install [Pip](https://pypi.org/project/pip/) on your local machine by following the instructions on [python pip documentation](https://pip.pypa.io/en/stable/).

## Setting up your own environment
<hr />

1) Run `make python` then `cd client/python` and run `pip install .` to pull all necessary dependencies.

## Run test to ensure everything is working
<hr />

From the root folder run python -m pytest

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

