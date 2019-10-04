REPO_ROOT=$(git rev-parse --show-toplevel)

echo ${REPO_ROOT}
ls
pwd

while [ ! -f ./done.txt ]
do
  sleep 1
done

echo ${REPO_ROOT}
ls
pwd