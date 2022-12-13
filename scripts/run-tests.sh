#!/bin/bash

$GO_TEST_CMD go test -coverprofile internal_coverage.xml -v ./internal... 2>&1 | tee test_reports/internal.txt
$GO_TEST_CMD go test -coverprofile pkg_coverage.xml -v ./pkg... 2>&1 | tee test_reports/pkg.txt
$GO_TEST_CMD go test -coverprofile cmd_coverage.xml -v ./cmd... 2>&1 | tee test_reports/cmd.txt
