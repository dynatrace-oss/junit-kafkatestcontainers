#!/bin/bash
set  -eo pipefail

if [ -z "$TAG" ]; then
  echo "NO TAG set, aborting."
  exit 1
fi

VERSION="${TAG:1}"

# Get version defined in build.gradle
GRADLE_VERSION=$(grep -oP 'version\s*=\s*"\K[^"]+' build.gradle.kts)

if [ "$VERSION" != "$GRADLE_VERSION" ]; then
  echo "TAG and GRADLE_VERSION are incompatible, aborting."
  exit 1
fi

# Fetch current branch
BRANCH=$(git branch -r --contains HEAD | grep -v HEAD | head -1 | sed 's|origin/||' | xargs)

if [ -z "$BRANCH" ]; then
  echo "Could not determine branch, aborting."
  exit 1
fi

#If BRANCH  has no "release/" BRANCH_VERSION will be the same as BRANCH
BRANCH_VERSION="${BRANCH#release/}"

if [ -z "$BRANCH_VERSION" ] || [ "$BRANCH_VERSION" = "$BRANCH" ]; then
  echo "Branch has wrong name format, aborting."
  exit 1;
fi

BRANCH_MAJOR_MINOR_VERSION="${VERSION%.*}"
GRADLE_MAJOR_MINOR_VERSION="${GRADLE_VERSION%.*}"

if [ "$BRANCH_MAJOR_MINOR_VERSION" != "$GRADLE_MAJOR_MINOR_VERSION" ]; then
  echo "Major and Minor version differ, aborting."
  exit  1;
fi

if [[ "$GRADLE_VERSION" =~ -(alpha|beta|M|RC)[0-9]+ ]]; then
  PRERELEASE=true
else
  PRERELEASE=false
fi

TITLE="JUnit Kafka Testcontainers $GRADLE_VERSION"

echo "Versions matching."
echo "version=$GRADLE_VERSION" >> "$GITHUB_OUTPUT"
echo "prerelease=$PRERELEASE" >> "$GITHUB_OUTPUT"
echo "title=$TITLE" >> "$GITHUB_OUTPUT"
exit 0
