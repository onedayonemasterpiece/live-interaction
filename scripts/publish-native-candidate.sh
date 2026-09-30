#!/usr/bin/env bash
set -euo pipefail
# Explicit immutable prerelease command; no credential reads or runtime deployment.
expected="${1:?exact committed SHA required}"
[[ "$expected" =~ ^[0-9a-f]{40}$ ]]
[[ "$(git rev-parse HEAD)" == "$expected" ]]
[[ -z "$(git status --porcelain --untracked-files=no)" ]]
version="$(node -p 'JSON.parse(require("fs").readFileSync("package.json","utf8")).version')"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+-rc\.[0-9]+$ ]]
tag="v${version}"
repo=onedayonemasterpiece/live-interaction
if gh release view "$tag" --repo "$repo" >/dev/null 2>&1; then
  gh release view "$tag" --repo "$repo" --json tagName,targetCommitish,isPrerelease,url
else
  gh release create "$tag" --repo "$repo" --target "$expected" --prerelease --latest=false \
    --title "Live interaction ${version} - Python and native WSS" \
    --notes 'Integration candidate. Shared Python WSS and native Java binding. Includes current accepted provider fixes and browser heartbeat. Real consumer acceptance is recorded separately in docs/native-wss.md and consumer receipts.'
fi
