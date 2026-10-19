# AGENTS.md — src/lisp

Scope: the Common Lisp PostgreSQL-wire server under `src/lisp/`. See the
`README.md` in this directory for what the server supports and for the
storage/concurrency rules.

## Lisp host

Use `../sbcl/run-sbcl.sh` (absolute: `/home/imbaczek/proj/sbcl/run-sbcl.sh`) as
the SBCL. Do not assume a system `sbcl`. The server launcher is `./pg-lisp.sh`.
When you delegate to a reviewer or subagent, tell it the same.

## Work the Lisp way: reload, don't restart

The server loads the `pg` system with ASDF and can recompile itself in place.
While iterating on backend code, **keep one server running and reload it after
each edit** instead of killing and restarting the whole system every time.

Two ways to trigger a reload:

- `kill -HUP <pid>` — the running server recompiles and reloads `pg`.
- The control REPL: start the server with a control socket, then call `(reload)`:

  ```sh
  ./pg-lisp.sh -p 5433 -D /tmp/pgdata --control-socket /tmp/pg.sock &
  nc -U /tmp/pg.sock        # then type: (reload)
  ```

A reload runs `(asdf:load-system "pg" :force t)` in the live image. Functions are
redefined in place, so:

- existing connections and global state (xact counters, catalog, relation
  streams, locks) survive;
- per-statement and per-connection code (`exec-*`, `btree-*`,
  `handle-connection`, `control-serve`, …) takes effect for the next statement
  or connection;
- reloads are serialized by a lock, so SIGHUP and `(reload)` never interleave.

A **restart** is still required when:

- you change a `DEFSTRUCT` / `DEFCLASS` layout (existing instances would be
  stale);
- you change code that runs only once at startup and is already executing or
  already installed: `boot.lisp`, `pg-lisp.sh`, the accept loops (`serve`,
  `control-accept-loop`), `main`, the reload and postmaster worker threads, and
  the handlers installed by `enable-reload` / `enable-shutdown-handlers` /
  `enable-cleanup`;
- you add or remove files in `pg.asd`.

Caveats: a reload recompiles the whole system (about a second, not incremental),
and a reload whose compilation fails partway can leave a mixed image — restart if
the reload output reports an error. Reader evaluation (`#.`) is disabled at the
control REPL prompt, but `eval` still runs parsed forms, so the REPL is arbitrary
code execution; it stays off unless `--control-port` / `--control-socket` is
given, and the Unix socket is `chmod`ed to `0700`.

## Tests

```sh
./t/run-unit-tests.sh   # page/tuple/index/visibility checks
./t/psql-smoke.sh       # psql acceptance, diffed against t/psql-smoke.out
./t/unique-index-concurrency.sh # unique-index and relation-DDL races
./t/index-stress.sh     # repeated same-key updates (slow)
./t/hot-reload.sh       # SIGHUP reloads a running server
./t/control-repl.sh     # control REPL over TCP + Unix socket, reload, SQL intact
./t/shutdown.sh         # signals, shutdown modes, cancel/terminate admin functions
```

Each test boots its own throwaway server on its own port (5461–5467), so they are
safe to run while a dev server is listening on 5433.

## Conventions

- Match PostgreSQL's naming and directory shape; keep to the supported MVP
  subset rather than growing scope.
- Wire-protocol integers are big-endian; on-disk page integers are little-endian.
  Do not mix them.
- Lock order is write-lock → smgr/catalog, never the reverse.
- **Compatibility target:** the eventual, far-away goal is byte-for-byte,
  bug-for-bug compatibility with PostgreSQL. When in doubt, port the original
  behavior rather than inventing a simpler one. The MVP subset is a scope
  limit, not a license to diverge inside that scope.
- Project workflow: review each milestone with a subagent.
