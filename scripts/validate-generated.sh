#!/bin/sh

compare_generated_files() {
  expected_repo_path="$1"
  actual_repo_path="$2"

  if [ -z "$expected_repo_path" ] || [ -z "$actual_repo_path" ]; then
    echo "Please specify paths to compare generated files"
    exit 1
  fi

  rm -f generated.txt

  { find pkg/api \( -iname '*.pb.go' -o -iname '*.pb.gw.go' -o -iname '*.swagger.go' \);
    find client/DotNet/Armada.Client -iname '*Generated.cs'; } >> generated.txt

  result=0
  while IFS='' read -r relative_path || [ -n "$relative_path" ]; do
    echo "Comparing: $expected_repo_path/$relative_path and $actual_repo_path/$relative_path"

    if ! diff "$expected_repo_path/$relative_path" "$actual_repo_path/$relative_path"; then
      result=1
    fi
  done < generated.txt

  rm generated.txt

  exit $result
}

mkdir -p ../expected && cp -R ./ ../expected

cd ../expected || exit 1
make proto
cd ../armada || exit 1

compare_generated_files ../expected .
compare_result="$?"

rm -rf ../expected

exit $compare_result
