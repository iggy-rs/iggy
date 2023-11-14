---
title: Failure of {{ env.GITHUB_BOT_CONTEXT_STRING }} on {{ date | date('YYYY-MM-DD') }}
labels: bug
---

## Job failure report

- **Workflow Name**: {{ env.GITHUB_WORKFLOW }}
- **Run ID**: [{{ env.GITHUB_RUN_ID }}](<https://github.com/{{ env.GITHUB_REPOSITORY }}/actions/runs/{{ env.GITHUB_RUN_ID }}>)

### Description

A failure occurred in the scheduled run of our {{ env.GITHUB_BOT_CONTEXT_STRING }}.
Please investigate the cause of the failure and take the necessary steps to resolve it.
