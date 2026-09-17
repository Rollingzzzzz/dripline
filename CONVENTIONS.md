# CONVENTIONS

Working rules for this repository. Short on purpose.

1. **English only** — code, comments, docs, commit messages, issues. No exceptions.
2. **Zero-dependency core.** `src/dripline` runs on the standard library alone. NumPy and
   FastAPI integration live behind optional extras (`dripline[numpy]`, `dripline[fastapi]`).
3. **Conventional commits** (`feat:`, `fix:`, `bench:`, `docs:`, `chore:`). No direct feature
   commits to `main`; work on `vX.Y` branches, merge after the owner confirms it runs.
4. **Every version tag ships measurements.** A tag without its `bench/results/vX.Y.md` is not
   a release. Benchmark methodology is fixed in `bench/README.md` (Docker, pinned cores).
5. **Honesty in docs.** Limitations stay listed in the README; marketing never outruns the
   measured numbers.
6. **No secrets, ever.** Scan the staged diff before every push.
7. **Private until v0.1 + benchmarks.** Flipping the repo public is a deliberate release
   decision made by the owner, together with the announcement.
8. **Lint is part of done.** `ruff check .` must pass before every commit; the rule
   set and line length live in `pyproject.toml` under `[tool.ruff]`.
