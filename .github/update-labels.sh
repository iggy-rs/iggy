#!/bin/bash

REPO_NAME="iggy-rs/iggy"
API_URL="https://api.github.com/repos/$REPO_NAME/labels"

create_label() {
  local name="$1"
  local color="$2"
  local description="$3"

  curl -X POST -H "Authorization: token $GITHUB_TOKEN" -H "Accept: application/vnd.github.v3+json" \
    -d '{"name": "'"$name"'", "color": "'"$color"'", "description": "'"$description"'"}' \
    "$API_URL"
}

create_label "CI/CD" "93ecdb" "Pull requests bring CI/CD improvements."
create_label "docs" "0075ca" "Improvements or additions to documentation."
create_label "duplicate" "cfd3d7" "This issue or pull request already exists."
create_label "maintenance" "ed4d86" "Maintenance work."
create_label "manifest" "D93F0B" "Label for pull request that brings a patch change."
create_label "semver:major" "8151c5" "Label for pull request that brings a major (BREAKING) change."
create_label "semver:minor" "8af85b" "Label for pull request that brings a minor change."
create_label "semver:patch" "fef2c0" "Label for pull request that brings a patch change."
