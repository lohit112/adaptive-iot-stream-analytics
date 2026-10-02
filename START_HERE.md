# How to use this with Antigravity
1. Unzip and open this folder as the workspace (File > Open Folder).
2. GEMINI.md / AGENTS.md / .agent/rules are loaded as project rules automatically. Check they appear in Antigravity's rules/customizations panel.
3. Optional: paste your original full spec into docs/SPEC_FULL.md.
4. New conversation, Planning mode. Paste Phase 0 from docs/prompts/PHASES.md. Review the plan, approve, check the output.
5. Repeat one phase per conversation. Or use the workflow: type `/phase 1`.
6. After each phase run `pytest -q` yourself and `git commit`.
Setup: python3.12 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt pytest
