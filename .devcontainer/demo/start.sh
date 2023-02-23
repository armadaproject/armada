#!/bin/bash

cd ../../internal/lookout/ui
yarn install
yarn run openapi
yarn run build
cd localdev
./scripts/kind-start.sh
cd ..
./localdev/run.sh
cd localdev
docker-compose logs -f --tail=10
