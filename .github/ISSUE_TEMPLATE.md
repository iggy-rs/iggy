---
title: QEMU aarch64/arm nightly test failure on {{ date | date('YYYY-MM-DD') }}
labels: bug
---

## Test Failure Report

- **Workflow Name**: {{ env.GITHUB_WORKFLOW }}
- **Run ID**: [{{ env.GITHUB_RUN_ID }}](https://github.com/{{ env.GITHUB_REPOSITORY }}/actions/runs/{{ env.GITHUB_RUN_ID }})

### Description

A test failure occurred in the scheduled run of our nightly QEMU test suite. 
Please investigate the cause of the failure and take the necessary steps to resolve it.

### Additional Context

Please fix the issue as soon as possible to ensure the stability of our project.
