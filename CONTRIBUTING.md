# Contributing to Public Data Gateway

Thank you for your interest in contributing to **Public Data Gateway**! All contributions—new connectors, bug fixes, dbt models, tests, and documentation—are welcome under the [MIT License](LICENSE).

## Development Setup

```bash
git clone https://github.com/jhsfhtls/public-data-gateway.git
cd public-data-gateway

python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows PowerShell: .\.venv\Scripts\Activate.ps1

pip install -r requirements.txt -r requirements-dev.txt
pip install -e .
```

Copy `.env.example` to `.env` if you need optional credentials (`CGU_API_TOKEN`, `DATAJUD_PUBLIC_API_KEY`, or BigQuery billing settings). Never commit `.env`, API tokens, or local data artifacts.

## Branch & Commit Conventions

This repository validates branch names and commit messages in CI (`pr-lint.yml`):

- **Branch prefixes:** `feat/`, `fix/`, `docs/`, `refactor/`, `chore/`, `ci/`, `test/`, `hotfix/` followed by lowercase `kebab-case` (max 50 characters, e.g., `feat/add-new-connector` or `fix/csv-encoding`).
- **Commit messages:** Follow [Conventional Commits](https://www.conventionalcommits.org/) (`<type>(<optional-scope>): <lowercase imperative description>`).

## Quality & Architecture Standards

1. **Encoding:** All source files, SQL models, YAML contracts, and Markdown documents must use **UTF-8 without BOM** and **LF (`\n`)** line endings.
2. **ODCS v3.1 & 11-Gate Medallion Parity ([ADR-004](docs/adr/ADR-004-connector-medallion-parity-policy.md)):** Every API/connector integrated into the gateway must follow the exact same 11-Gate standard: catalog registration (`CATALOG.md`), `BaseConnector` + `contract.yaml`, isolated `data/<connector>/{bronze,silver,gold,quarantine}/` materialization, complete dbt layers (`staging` -> `intradominio` -> `cross_domain`), offline + fail-hard live tests, and 4-interface parity (SDK, FastAPI, CLI, MCP).
3. **dbt Documentation Coverage:** Every dbt model and 100% of its columns must have descriptions in `_models.yml`.
4. **Error Handling:** Never swallow exceptions silently (`except: pass`). Always log explicit context and use context managers for file I/O.
5. **Security & Privacy:** Respect `Client Path Shield` isolation (never persist external user datasets inside the gateway's `data/` lakehouse directory), preserve LGPD masking, and apply CSV formula neutralization.

## Pre-PR Verification Checklist

Run these checks locally before opening a Pull Request:

```bash
ruff check .
python scripts/render_catalog.py --check
python scripts/render_progress.py --check
python scripts/lint_dbt_docs.py
pytest -m "not live" -q
```
