REPO_ROOT=$(git rev-parse --show-toplevel)
while [ ! -f .${REPO_ROOT}/done.txt ]
do
  sleep 1
done