IMAGE=$1
RETRY_COUNT=${2:-5}

# Try to pull image, return success on pull succeed
for ((i=1; i<=$RETRY_COUNT; i++))
do
  if docker pull $IMAGE ; then
    exit 0
  fi
  echo Failed to pull $IMAGE - attempt $i
  sleep 3
done

# Return failure if all pull attempts fail
exit 1
