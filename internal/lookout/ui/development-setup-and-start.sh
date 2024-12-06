#!/bin/bash


if [ "$1" ]; then
	echo "Replacing registry.yarnpkg.com in yarn.lock with alternative registry: ${1}"
	sed -i "s|https://registry.yarnpkg.com|${1}|g" yarn.lock
fi

yarn install

if [ "$1" ]; then
	echo "Reverting changes to yarn.lock"
	sed -i "s|${1}|https://registry.yarnpkg.com|g" yarn.lock
fi

yarn openapi
DANGEROUSLY_DISABLE_HOST_CHECK=true yarn start
