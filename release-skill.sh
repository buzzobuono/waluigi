#!/usr/bin/env bash
# Usage: ./release-skill.sh <version> ["commit message"]
# Example: ./release-skill.sh 1.2.0 "feat: add new chart type"
set -euo pipefail

VERSION=${1:-}
MESSAGE=${2:-"release: waluigi-developer skill v${VERSION}"}

if [[ -z "$VERSION" ]]; then
  echo "Usage: $0 <version> [\"commit message\"]"
  exit 1
fi

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Error: version must be semver (e.g. 1.2.0)"
  exit 1
fi

REPO_ROOT=$(git rev-parse --show-toplevel)
MANIFEST="$REPO_ROOT/plugins/waluigi-developer/manifest.json"
MARKETPLACE="$REPO_ROOT/.claude-plugin/marketplace.json"

echo "→ Updating version to $VERSION"
sed -i "s/\"version\": \".*\"/\"version\": \"$VERSION\"/" "$MANIFEST"
sed -i "s/\"version\": \".*\"/\"version\": \"$VERSION\"/" "$MARKETPLACE"

echo "→ Staging manifest files"
git add "$MANIFEST" "$MARKETPLACE"

# Stage all changes: modified tracked files + new untracked files in the repo
git add -u
git add "$REPO_ROOT/waluigi" "$REPO_ROOT/doc" "$REPO_ROOT/descriptors" \
        "$REPO_ROOT/plugins" "$REPO_ROOT/.claude-plugin" 2>/dev/null || true

if git diff --cached --quiet; then
  echo "Nothing to commit."
else
  echo "→ Committing"
  git commit -m "$MESSAGE"
fi

echo "→ Pushing main"
git push origin main

echo "→ Tagging v$VERSION"
if git rev-parse "v$VERSION" >/dev/null 2>&1; then
  echo "Warning: tag v$VERSION already exists, skipping tag"
else
  git tag "v$VERSION"
  git push origin "v$VERSION"
  echo "→ Tag v$VERSION pushed"
fi

echo "Done — v$VERSION released."
