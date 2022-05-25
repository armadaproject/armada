IMAGE=$1
RETRY_COUNT=${2:-5}

for ((i=1; i<=$RETRY_COUNT; i++))
do
  if docker pull $IMAGE ; then
    # Return 0 on pull success
    exit 0
  fi
  echo Failed to pull $IMAGE - attempt $i
  sleep 60
done

# Return failure if all pull attempts fail
exit 1
