REPO_ROOT=$(git rev-parse --show-toplevel)
echo ${REPO_ROOT}
ls
pwd
sleep 10
echo hello > ${REPO_ROOT}/done.txt

echo ${REPO_ROOT}
ls
pwd