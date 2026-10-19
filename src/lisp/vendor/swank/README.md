# Vendored Swank

These files are the Swank server and contrib sources from
[SLIME v2.32](https://github.com/slime/slime/tree/v2.32), commit
`2fd4c2f08bf335872b8fff046f25b05179fb8571` (2025-12-09). Upstream identifies
the Swank files as public domain.

The upstream `swank.asd` is named `pg-swank.asd` here and defines the systems
`pg-swank` and `pg-swank/exts`, so the pinned copy cannot be shadowed by another
ASDF system named `swank`. Its ASDF load hook reports protocol version `2.32`
because the Emacs frontend is installed separately.

Use the matching Emacs SLIME v2.32 frontend when connecting to this server.
