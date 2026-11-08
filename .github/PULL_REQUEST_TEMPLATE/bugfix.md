## Summary
- **Affected Connector / Module**: ``
- **Root Cause**: 

## Fix & Resilience Improvements
- 

## Verification Checklist
- [ ] Regression test added in `tests/` reproducing the edge case offline
- [ ] Zero silent exception swallowing (`error_handling_resilience.md`)
- [ ] UTF-8 LF without BOM verified (`windows_encoding_safety.md`)
- [ ] `ruff check .` and `pytest -m "not live"` passing
- [ ] `JORNADA_IMPLEMENTACAO.md` updated if connector state changed

## Suggested CHANGELOG Entry
```markdown
### Fixed
- fix(<scope>): <concise description>
```
