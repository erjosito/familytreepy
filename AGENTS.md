# Repository Instructions

## Application versioning

Every pull request must bump the application version exposed by `GET /api/health`.

- Update `APP_VERSION` in `backend/app/main.py` in the same pull request.
- Use semantic versioning:
  - Patch for fixes, maintenance, documentation, and internal changes.
  - Minor for backward-compatible user features.
  - Major for breaking API or data-model changes.
- Never reuse a version that has already been deployed.
- Update the version assertions in `backend/tests/test_api.py`.
- Keep the semantic application version separate from the Git revision.
- After deployment, verify that `GET /api/health` reports both the expected version and the expected revision.
