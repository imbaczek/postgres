# AGENTS.md — src/lisp

Scope: the Common Lisp PostgreSQL-wire server under `src/lisp/`. See the
`README.md` in this directory for what the server supports and for the
storage/concurrency rules.

## Lisp host

The scripts default to `sbcl` from `PATH`. Set `SBCL` to a local executable or
wrapper when needed; do not hard-code a machine-specific path. The server
launcher is `./pg-lisp.sh`.

Run `./t/compile.sh` for a forced ASDF compile with compiler warnings treated
as errors. It uses `sbcl` from `PATH` by default and honors `SBCL` overrides.
`./t/format.sh` uses Emacs + SLIME and the same inferior Lisp by default.

## Work the Lisp way: reload, don't restart

The server loads the `pg` system with ASDF and can recompile itself in place.
While iterating on backend code, **keep one server running and reload it after
each edit** instead of killing and restarting the whole system every time.

Two ways to trigger a reload:

- `kill -HUP <pid>` — the running server recompiles and reloads `pg`.
- The control REPL: start the server with a control socket, then call `(reload)`:

  ```sh
  CONTROL_DIR=$(mktemp -d)
  ./pg-lisp.sh -p 5433 -D /tmp/pgdata --control-socket "$CONTROL_DIR/pg.sock" &
  nc -U "$CONTROL_DIR/pg.sock"  # then type: (reload)
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
  `control-accept-loop`), `xact-init`, `start-control-plane`, `main`, the reload
  and postmaster worker threads, and the handlers installed by `enable-reload` /
  `enable-shutdown-handlers` / `enable-cleanup`;
- you add or remove files in `pg.asd`.

Caveats: a reload recompiles the whole system (about a second, not incremental),
and a reload whose compilation fails partway can leave a mixed image — restart if
the reload output reports an error. Reader evaluation (`#.`) is disabled at the
control REPL prompt, but `eval` still runs parsed forms, so the REPL is arbitrary
code execution; it stays off unless `--control-port` / `--control-socket` is
given. The Unix socket's parent directory must be owned by the server user with
mode `0700`, and the socket node is `chmod`ed to `0700`; failure to enforce
those permissions aborts startup.

## Agent runtime debugging

When a task depends on live Lisp state, backend threads, or timing between SQL
sessions, agents should prefer the control REPL on a local server instance they
started for the task. Use it to inspect state, coordinate concurrent sessions,
and reload code; keep compilation and the test scripts as the verification
path. The control REPL evaluates arbitrary Lisp, so do not connect to an
unsolicited or unrelated endpoint. It is off by default; enable it only for the
debug instance. TCP control binds to loopback but has no authentication, so
verify it belongs to the debug server you started before connecting. A Unix
socket must use the private parent-directory permissions described above. Use
SLIME when an interactive Emacs session or its tracing tools are useful; it is
optional.

## Live SQL and SLIME debugging

From `src/lisp`, start a server with a fresh PGDATA and free SQL and Swank ports.
Swank is preloaded but its listener is off by default; the optional listener
binds to loopback unless asked otherwise:

```sh
DEBUG_DATA=$(mktemp -d)
./pg-lisp.sh -p 5439 -D "$DEBUG_DATA" --swank-port 4005
```

Connect from Emacs with `M-x slime-connect`, host `127.0.0.1`, port `4005`. Use
the matching SLIME v2.32 frontend for this repository's pinned Swank; see
[`README.md`'s SLIME debugging section](README.md#slime-debugging) for listener
and connection details. If Emacs reports a protocol-version mismatch, check
which frontend it loaded; evaluate `slime-protocol-version` with `M-:` and
confirm it reports `2.32`. Run SQL from a second terminal while inspecting the
live Lisp image in the SLIME REPL:

```sh
psql -X -h 127.0.0.1 -p 5439 -U postgres -d postgres
```

The SLIME REPL starts in `CL-USER`, so qualify internal server symbols with
`pg::`. Use `(pg::reload)` after source edits. SQL runs in backend threads, so
plain SBCL trace output may go to the server's output stream instead of the
SLIME REPL. To inspect calls in SLIME, collect them with a trace report
function:

```lisp
(pg::reload)
(defvar pg::*debug-trace-events* nil)
(defun pg::debug-trace-report (depth name event frame values)
  (declare (ignore depth frame values))
  (push (list name event) pg::*debug-trace-events*)
  nil)
(setf pg::*debug-trace-events* nil)
(trace :report pg::debug-trace-report
       pg::exec-create-index pg::build-unique-index)
```

From `psql`, create a small table and a unique index to exercise both traced
functions; then inspect the collected events in the SLIME REPL:

```sql
CREATE TABLE slime_debug_items (id int, label text);
INSERT INTO slime_debug_items VALUES (10, 'oak'), (20, 'elm'), (30, 'fir');
CREATE UNIQUE INDEX slime_debug_items_id_idx ON slime_debug_items (id);
```

```lisp
(reverse pg::*debug-trace-events*)
```

Remove the trace and its temporary helper after the demo:

```lisp
(untrace pg::exec-create-index pg::build-unique-index)
(fmakunbound 'pg::debug-trace-report)
(makunbound 'pg::*debug-trace-events*)
```

## Tests

```sh
./t/run-unit-tests.sh   # page/tuple/index/visibility checks
./t/psql-smoke.sh       # psql acceptance, diffed against t/psql-smoke.out
./t/unique-index-concurrency.sh # unique-index and relation-DDL races
./t/update-concurrency.sh # concurrent UPDATE chain and WHERE rechecks
./t/index-stress.sh     # repeated same-key updates (slow)
./t/hot-reload.sh       # SIGHUP reloads a running server
./t/control-repl.sh     # control REPL over TCP + Unix socket, reload, SQL intact
./t/swank.sh            # Swank preload, bind options, invalid/occupied ports
./t/shutdown.sh         # signals, shutdown modes, cancel/terminate admin functions
```

Each test boots its own throwaway server on its own SQL port (5461–5471), so they
are safe to run while a dev server is listening on 5433. The UPDATE concurrency
and shutdown tests also bind their local control REPL at SQL port + 2000 to
synchronize on active transaction-ID waits.

For a live debugger when a server-backed test fails, set `DEBUG_ON_FAILURE=1`:

```sh
DEBUG_ON_FAILURE=1 ./t/index-stress.sh
```

The test opens Swank on `SQL port + 1000` (6462 for the example). If an
assertion fails while the server is running, it prints the SQL port, Swank
port, PGDATA, and log path, then waits for Enter before cleanup. Set
`DEBUG_SWANK_PORT` to choose another Swank port. Run the test in a terminal or
tmux pane so the pause can wait while you attach with `M-x slime-connect`. If
the server has already exited, the test reports that there is no live listener
and cleans up without pausing.
The dedicated `t/swank.sh` test controls listener states itself and does not use
this mode.

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
- **Original C source:** implementers and reviewers must always consult the
  corresponding PostgreSQL C source and relevant C tests for backend behavior
  changes. Compare the Lisp behavior with those references and explain any
  deliberate differences.
- Project workflow: review each milestone with a subagent.
