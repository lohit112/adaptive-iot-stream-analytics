---
description: Run one numbered phase from docs/PLAN.md (usage: /phase 3)
---
1. Read GEMINI.md, docs/SPEC_SUMMARY.md, docs/ARCHITECTURE_NOTES.md (if it exists) and the requested phase in docs/PLAN.md and docs/prompts/.
2. Produce an implementation plan (files, interfaces, tests, assumptions). Wait for approval.
3. Implement only that phase. Do not touch cmix/aloa/maso/earm logic.
4. Run `pytest -q`; paste real output.
5. Report: files changed, run commands, limitations. Stop.
