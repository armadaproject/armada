# Running Armada for Developers

We have two ways in this folder for running armada.  

## Running Armada with docker-compose
1) Run `make armada-dev` which will build the dev containers you need for docker-compose.
2) `docs/dev/setup.sh` 
3) Follow the command in that file to launch docker-compose with a fake executor.

## Running Armada without docker-compose
This is not recommended if developing for only Armada.

We suggest using this if you want to integrate armada with an external service like airflow.

1) Run `docs/dev/setup_local_host.sh`
2) Run each of those commands in separate terminals