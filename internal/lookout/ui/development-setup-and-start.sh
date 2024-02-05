#!/bin/bash


if [ "$1" ]; then
	echo "Replacing registry.yarnpkg.com in yarn.lock with alternative registry: ${1}"
	sed -i "s|https://registry.yarnpkg.com|${1}|g" yarn.lock
fi

yarn install
yarn openapi
DANGEROUSLY_DISABLE_HOST_CHECK=true yarn start
