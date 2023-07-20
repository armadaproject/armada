#!/bin/bash

docker_registry="gresearch"
docker_tag=""
image_names=(
    "armada-bundle"
    "armada-lookout-bundle"
    "armada-full-bundle"
    "armada-server"
    "armada-executor"
    "armada-fakeexecutor"
    "armada-lookout-ingester"
    "armada-lookout-ingester-v2"
    "armada-lookout"
    "armada-lookout-v2"
    "armada-event-ingester"
    "armada-scheduler"
    "armada-scheduler-ingester"
    "armada-binoculars"
    "armada-jobservice"
    "armadactl"
)

print_usage() {
    echo "Usage: $0 [-t|--tag <tag>] [-r|--registry <registry>]"
    echo ""
    echo "Options:"
    echo "    -t|--tag       Docker tag (required)"
    echo "    -r|--registry  Docker registry (default: 'gresearch')"
    echo "    -h|--help      Display this help message"
}

# parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--tag)
            docker_tag=$2
            shift
            shift
            ;;
        -r|--registry)
            docker_registry=$2
            shift
            shift
            ;;
        -h|--help)
            print_usage
            shift
            ;;
        *)
            echo "::error ::invalid argument $1" >&2
            exit 1
            ;;
    esac
done

# validate that docker_tag is provided
if [ -z "$docker_tag" ]; then
    echo "::error ::docker tag is must be provided with -t|--tag option"
    exit 1
fi

# iterate over image names, check existence and push them
for image_name in "${image_names[@]}"; do
    full_image_name="${docker_registry}/${image_name}:${docker_tag}"
    echo "checking existence of $full_image_name..."
    # Check if the image with the tag exists
    if ! docker image inspect "$full_image_name" > /dev/null 2>&1; then
        echo "::error ::image $full_image_name does not exist locally"
        exit 1
    fi
done

echo "pushing Armada images to $docker_registry with tag $docker_tag..."

# iterate over image names and push them
for image_name in "${image_names[@]}"; do
    full_image_name="${docker_registry}/${image_name}:${docker_tag}"
    echo "pushing $full_image_name..."
    docker push $full_image_name
    if [ $? -ne 0 ]; then
        echo "::error ::failed to push $full_image_name"
        exit 1
    fi
done
