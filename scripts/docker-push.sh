#!/bin/bash


if ! source "$(dirname "$0")/common.sh"; then
    echo "::error ::failed to source common.sh"
    exit 1
fi

docker_tag=""
use_tarballs=false

print_usage() {
    echo "Usage: $0 [-t|--tag <tag>] [-r|--registry <registry> -i|--images <images>]"
    echo ""
    echo "Options:"
    echo "    -u|--use-tarballs  Directory with image tarballs to push"
    echo "    -i|--images-dir  Directory with image tarballs to push"
    echo "    -t|--tag         Docker tag (required)"
    echo "    -r|--registry    Docker registry (default: '$docker_registry')"
    echo "    -h|--help        Display this help message"
}

# parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--images-dir)
            images_dir=$2
            images_dir=${images_dir%/}
            shift
            shift
            ;;
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
        -u|--use-tarballs)
            use_tarballs=$2
            shift
            shift
            ;;
        -h|--help)
            print_usage
            exit 0
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

if [ "$use_tarballs" = true ]; then
    if [ -z "$images_dir" ]; then
        echo "::error ::tarball images dir must be provided with -i|--images-dir option"
        exit 1
    fi
fi

# iterate over image names, check existence and push them
for image_name in "${image_names[@]}"; do
    echo "::group::validating $image_name..."
    if [ "$use_tarballs" = true ]; then
        tarball_image="${images_dir}/${image_name}.tar"
        if [ ! -f "$tarball_image" ]; then
            echo "::error ::image $tarball_image does not exist"
            exit 1
        fi
    else
        full_image_name="${docker_registry}/${image_name}:${docker_tag}"
        echo "checking existence of $full_image_name..."
        # Check if the image with the tag exists
        if ! docker image inspect "$full_image_name" > /dev/null 2>&1; then
            echo "::error ::image $full_image_name does not exist locally"
            exit 1
        fi
        echo "::endgroup::"
    fi
done

echo "pushing Armada images to $docker_registry with tag $docker_tag..."

# iterate over image names and push them
for image_name in "${image_names[@]}"; do
    echo "::group::pushing $image_name..."
    full_image_name="${docker_registry}/${image_name}:${docker_tag}"
    if [ "$use_tarballs" = true ]; then
        tarball_image="${images_dir}/${image_name}.tar"
        echo "loading tarball image $tarball_image..."
        docker load --input "$tarball_image"
    fi
    echo "pushing $full_image_name..."
    docker push $full_image_name
    if [ $? -ne 0 ]; then
        echo "::error ::failed to push $full_image_name"
        exit 1
    fi
    echo "::endgroup::"
done
