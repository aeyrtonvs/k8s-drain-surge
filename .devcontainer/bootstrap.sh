#!/usr/bin/env bash
# Runs once after the devcontainer is created. Pulls deps, then runs the
# same make targets CI runs (tidy, vet, test, build) so the workspace is
# verified end-to-end before the user starts editing.
set -euo pipefail

echo "==> go mod download"
go mod download

echo "==> make tidy"
make tidy

echo "==> make vet"
make vet

echo "==> make test"
make test

echo "==> make build"
make build

echo "==> bootstrap complete"
