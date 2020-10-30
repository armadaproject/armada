#!/bin/sh

make proto

dirs_to_check='pkg/api|client/DotNet/Armada.Client' # Pipe separated

git status -s -uno | grep -E "$dirs_to_check"
compare_result="$(git status -s -uno | grep -c -E "$dirs_to_check")"

exit "$compare_result"
