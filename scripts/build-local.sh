#!/usr/bin/env bash
# Safe development build: never installs services, requires sudo, or touches production.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "$(uname -s)" != "Linux" ]]; then
  echo "Go plugins in this project are supported on Linux only." >&2
  exit 1
fi
mkdir -p dist
echo "Building queue worker..."
go build -buildvcs=false -o dist/queue-work-go .
echo "Building standalone example plugin..."
go build -buildmode=plugin -o examples/hello/main.so ./examples/hello
echo "Build complete: dist/queue-work-go and examples/hello/main.so"
echo "No systemd services were installed or started."
