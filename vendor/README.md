# Vendored code

`ptt/` is [PTT (parsett)](https://github.com/dreulavelle/PTT), MIT licensed (see `ptt/LICENSE`),
copied from the Appz4Fun/nzbdavkodi vendored copy, where `regex` was replaced by `re` and `arrow` by
`datetime` so it is pure stdlib Python. `cli.py` is omitted. Do not edit except for compatibility fixes.

`cyclops/verify_nzb.py` is [Appz4Fun/cyclops](https://github.com/Appz4Fun/cyclops) at `0de7ede` (stdlib-only async
NNTP STAT/BODY verifier, same owner). Unmodified; used by `donor_health.py`.
