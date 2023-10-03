#!/bin/bash

if ! source "$(dirname "$0")/common.sh"; then
    echo "::error ::failed to source common.sh"
    exit 1
fi

docker_tag=""
output_dir="."

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--tag)
            docker_tag=$2
            shift
            shift
            ;;
        -o|--output)
            output_dir=$2
            shift
            shift
            ;;
    esac
done

# validate that docker_tag is provided
if [ -z "$docker_tag" ]; then
    echo "::error ::docker tag is must be provided with -t|--tag option"
    exit 1
fi

# Check if output directory exists, if not create it
if [[ ! -d $output_dir ]]; then
    if ! mkdir -p "$output_dir"; then
        echo "::error ::failed to create output directory $output_dir"
        exit 1
    fi
fi

DOCKER_SAVE="docker save"
for image_name in "${image_names[@]}"; do
    DOCKER_SAVE+=" $docker_registry/$image_name:$docker_tag"
done
DOCKER_SAVE+=" | gzip > $output_dir/armada.tar.gz"

$DOCKER_SAVE
