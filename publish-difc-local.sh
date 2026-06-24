#!/usr/bin/env bash
# Publish DIFC-patched Kafka 4.0.0 modules to ~/.m2 for workflow Maven builds.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "${ROOT}"
GRADLE_USER_HOME="${GRADLE_USER_HOME:-${HOME}/.gradle}"
exec ./gradlew \
  :clients:publishToMavenLocal \
  :streams:publishToMavenLocal \
  :metadata:publishToMavenLocal \
  :core:publishToMavenLocal \
  -x test \
  -PskipSigning=true \
  "${@}"
