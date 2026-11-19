#!/usr/bin/env bash
# Build only. Installation and service management must be explicit operations.
set -euo pipefail
cd "$(dirname "$0")"
exec bash scripts/build-local.sh
