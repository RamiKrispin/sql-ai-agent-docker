# Concrete Improvements for sql_ai_agent-docker

Overview
- [ ] Provide actionable, low-ceremony improvements to hardening, documenting, and evolving the repo for reliability and usability.

Key Observations
- [ ] Minimal documentation; onboarding is unclear.
- [ ] A broken Python package init (caused an invalid file name) previously blocked imports.
- [ ] Secrets are embedded in configs (e.g., docker-compose) and should be externalized.
- [ ] Code quality is approachable but could benefit from tests and error handling.

Concrete Improvements
- [ ] Fix Package Init
  - [x] Rename `sql_ai_agent/__init__,py` to `sql_ai_agent/__init__.py`.
  - [ ] Add a minimal `__init__` that exposes commonly used submodules (prompt_handler, parse_query).
- [ ] Documentation & Onboarding
  - [ ] Extend `README.md` with Quickstart, Architecture, and how to swap LLM/provider.
  - [ ] Add a template `.env.example` and README guidance on secret handling.
- [ ] Configuration Hygiene
  - [ ] Move secrets (POSTGRES_PASSWORD, DMR_API_KEY) to environment variables or secret managers; avoid hard-coding.
  - [ ] Centralize LLM/config in a single file and document its loading path.
- [ ] Code Quality & Safety
  - [ ] Review OpenAI client usage; use a robust import style and error handling.
  - [ ] Add type hints and replace `print` with a logging framework.
  - [ ] Harden `parse_query.py` with robust regex and safe fallbacks; ensure consistent return types.
- [ ] Tests & Validation
  - [ ] Add unit tests for `parse_query` utilities and simple `SqlAgent` flows with mocks.
  - [ ] Introduce a minimal test suite (pytest) and a small integration test plan.
- [ ] CI / Linting
  - [ ] Add GitHub Actions workflow to run `ruff` lint, tests, and optional type checks.
- [ ] Docker & Dev Experience
  - [ ] Add healthchecks, a wait-for-postgres script, and pinned dependencies to guarantee reproducibility.
  - [ ] Provide a streamlined dev docker-compose with environment-friendly defaults.
- [ ] Security & Best Practices
  - [ ] Ensure secrets are not committed and update `.gitignore` accordingly.
  - [ ] Review data handling for local data (e.g., avoid bundling large datasets in images).

Next Steps
- [ ] Pick one or more improvements to implement first (e.g., fix package init, extend README, and add initial tests). I can scaffold files, add tests, and draft CI configs.
