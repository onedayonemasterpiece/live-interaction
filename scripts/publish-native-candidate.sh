#!/usr/bin/env bash
set -euo pipefail
# Explicit release command, never executed by an import/build. No credential reads.
expected="${1:?exact committed SHA required}"
[[ "$expected" =~ ^[0-9a-f]{40}$ ]]
[[ "$(git rev-parse HEAD)" == "$expected" ]]
[[ -z "$(git status --porcelain --untracked-files=no)" ]]
tag=v0.3.6-rc.1
repo=onedayonemasterpiece/live-interaction
if gh release view "$tag" --repo "$repo" >/dev/null 2>&1; then
  gh release view "$tag" --repo "$repo" --json tagName,targetCommitish,isPrerelease,url
else
  gh release create "$tag" --repo "$repo" --target "$expected" --prerelease --latest=false \
    --title 'Live interaction 0.3.6 RC1 - Python and native WSS' \
    --notes 'Integration candidate. Shared Python WSS and native Java binding; not a production voice acceptance claim. See docs/native-wss.md and consumer acceptance receipts.'
fi
