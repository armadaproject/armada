#!/bin/bash

INTERNAL_COVERAGE_LIST=$($GO_TEST_CMD go list ./internal... | grep -v /testsuite | grep -v /statik | grep -v /metrics | grep -v model | grep -v gen)
$GO_TEST_CMD go test -coverprofile internal_coverage.xml $INTERNAL_COVERAGE_LIST 2>&1 | tee test_reports/internal.txt
$GO_TEST_CMD go test -coverprofile pkg_coverage.xml -v ./pkg... 2>&1 | tee test_reports/pkg.txt
$GO_TEST_CMD go test -coverprofile cmd_coverage.xml -v ./cmd... 2>&1 | tee test_reports/cmd.txt
