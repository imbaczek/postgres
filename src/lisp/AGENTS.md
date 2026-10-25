# AGENTS.md — src/lisp

Scope: the Common Lisp PostgreSQL-wire server under `src/lisp/`. See the
`README.md` in this directory for what the server supports and for the
storage/concurrency rules.

## Common Lisp agent prompts

Read the relevant prompts in [`agent-prompts/`](agent-prompts/README.md) when
working on Common Lisp code:

- [Common Lisp expert](agent-prompts/common-lisp-expert.md): design, style,
  performance, and review guidance.
- [REPL-driven development](agent-prompts/repl-driven-development.md): explore,
  experiment, persist, and verify changes in a live Lisp image.
- [cl-spec-driven development](agent-prompts/cl-spec-driven-development.md):
  contracts and property checks when working with cl-spec.

These are upstream reference prompts. This file's project-specific rules and
the user's instructions take precedence. Use the tools available in the session
and this project's existing reload and test workflows; the prompts do not
require installing cl-mcp, cl-spec, or Rove.

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

### Choose the image that owns the failure

- The cl-mcp worker, the running SQL server, and the formatter's temporary
  SLIME/Swank image are separate Lisp processes. Loading `pg` in cl-mcp does
  not update the server under test. A formatter connection is not a debugger
  attached to that server. Record the server PID, PGDATA, SQL port and control
  socket/Swank port when starting an experiment.
- Start a task-owned dev server with a private control socket before debugging
  stateful SQL or replication failures. Reproduce through its SQL/replication
  connection, then inspect and instrument that same image through control or
  Swank. Prefer this to repeated blind restarts and inference from logs alone.
  Keep fresh, isolated peers for acceptance and intentional crash/restart tests.
- A control evaluation runs in its own thread, not the failing SQL backend's
  dynamic environment. Capture the relevant backend/thread explicitly; do not
  assume its `*backend*` binding is visible in the control REPL. Keep inspections
  under the appropriate locks short, and do not block a failpoint while holding
  locks needed by the session that must release it.

From `src/lisp`, the existing control client can send an experiment or reload
to the actual server and close just the control connection:

```sh
printf '(pg::reload)\n(quit)\n' |
  "${SBCL:-sbcl}" --script t/control-client.lisp unix "$CONTROL_DIR/pg.sock"
```

Check the response and server log for reload success before repeating SQL.
The native interop harness uses this mechanism to load
`t/interop/native-failpoints.lisp` into the publisher and install COPY,
publication-race, commit-end and feedback hooks. Reuse those bounded hooks when
appropriate instead of replacing the native C peer or adding a message relay.

### Redefine deliberately, then verify from saved source

- A source edit is not a live redefinition. After a cl-mcp structural edit,
  `load-system` updates the worker; `(pg::reload)` or SIGHUP separately updates
  the dev server. Individual REPL definitions are useful experiments, but save
  the final change and reload its system before acceptance. A function reload
  affects subsequent calls; it does not rewind active stack frames, reconstruct
  old closures or migrate structure instances. Reinstall changed test hooks.
- Inspect state at the failing boundary: for replication, distinguish source
  commit start/end, target durable progress, exact confirmed flush, retained
  restart floor and in-flight effects. Eventual row equality or a received WAL
  position alone cannot diagnose a durability failure. Preserve an independent
  wire/C-peer oracle after an interactive experiment succeeds.
- Use `unwind-protect` to restore temporary `symbol-function` replacements and
  hooks; untrace after debugging. Release paused actors during cleanup. These
  changes live in a persistent image and otherwise contaminate later tests.
- Serialize forced ASDF rebuilds, FASL deletion, server startup/reload and the
  Emacs/SLIME formatter when they share an output cache. A formatter inferior
  exited during an overlapping rebuild in this session; a serialized retry
  succeeded. Do not start another copy of a test merely because its output was
  truncated or a poll timed out; check the original process/session first.

### Network deadlines and live socket inspection

- In the current SBCL, `sb-ext:timeout` is not an ordinary `error` condition.
  A retry loop handling only `error` allowed a receiver timeout to escape its
  thread and terminate the server. Catch that timeout explicitly when the
  operation is retryable. Scope network timers around socket I/O, not request
  construction that can wait for admission, replay locks, or storage durability.
- Use the task-owned control REPL to inspect actual socket options when network
  progress is unexpectedly slow. In this session, enabling TCP_NODELAY on the
  live sender/receiver exposed a Nagle/delayed-ACK stall without restarting Lisp.
  Persist the setup change, compare C `pq_init` / libpq `connectNoDelay`, then
  verify fresh connections: reloading a setup function does not change existing
  socket options by itself.
- Large full-prefix recovery can exceed the small-test startup deadline. The
  owned-server readiness helper accepts an optional fourth attempt limit;
  increasing it must preserve PID/listener ownership checks. Diagnose a real
  startup failure separately from slow replay, and stop polling if the owned
  process dies rather than connecting repeatedly to its former port.

### Reader and test-discovery checks

- Use cl-mcp structural edits for Lisp. A rejected patch that would add an extra
  closing parenthesis leaves the source intact; correct the patch rather than
  bypassing that guard.
- Distinguish `#(...)` vector literals from `#.` read-time evaluation. Empty and
  nonempty vectors work in the current tools. Several server files legitimately
  use `#.(char-code ...)`; a tool with `*read-eval*` disabled can reject those
  files even when their parentheses are balanced. That is a reader restriction,
  not proof of damaged parentheses or ignored vector data.
- On a reader error, inspect the exact raw source with `lisp-read-file`
  `collapsed=false` or `fs-read-file`. A documented `readtable` override changes
  parsing behavior and is not a universal workaround for `#.`. Preserve valid
  source and report unsupported syntax instead of rewriting wire fixtures or
  enabling arbitrary read-time evaluation just to make a tool accept the file.
- Balanced parentheses do not establish the intended top-level structure. After
  inserting multiple definitions, inspect the collapsed outline and test names;
  a misplaced close once nested a `deftest` inside another form in this session.
  Confirm the expected tests actually execute, not just that loading succeeds.
  Collapsed output hides bodies, and raw output may be paginated; neither means
  the hidden forms were ignored by Lisp.
- Rove tests do not automatically execute registered cl-spec declarations. Call
  their runners from the test system, and check Function Specs and Properties
  separately. Record selection, seed and actual trial counts; a declaration
  digest does not identify the implementation or the server image being tested.

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
./t/vacuum-update-concurrency.sh # ordinary VACUUM with same-page concurrent writers
./t/index-scan-regression.sh # indexed/SeqScan differential and DROP INDEX handoff
./t/index-lock-contention.sh # unrelated INSERT progress during duplicate-key scans
./t/index-build-contention.sh # detached build, relation gate, publication name race
./t/join-streaming.sh   # outer-join results and unordered LIMIT early stop
./t/join-ddl-concurrency.sh # atomic JOIN admission against concurrent DROP TABLE
./t/sort-arena.sh       # opt-in SBCL sort arenas, capacity errors, transaction recovery
./t/index-stress.sh     # repeated same-key updates (slow)
./t/hot-reload.sh       # SIGHUP reloads a running server
./t/control-repl.sh     # control REPL over TCP + Unix socket, reload, SQL intact
./t/swank.sh            # Swank preload, bind options, invalid/occupied ports
./t/shutdown.sh         # signals, shutdown modes, cancel/terminate admin functions
```

Each test boots its own throwaway server on its own SQL port, so they are safe to
run while a dev server is listening on 5433. The UPDATE concurrency, index scan,
index contention/build, JOIN, and shutdown tests also bind a local control REPL
at SQL port + 2000 for synchronization.

For the kernel OOM recovery test, agents and tmux sessions should use a private
delegated systemd service. From `src/lisp`:

```sh
systemd-run --user --wait --pipe --collect -p Delegate=memory \
  --working-directory="$PWD" --setenv=SBCL="${SBCL:-$(command -v sbcl)}" \
  "$PWD/t/spill-oom-cgroup.sh"
```

Require `spill-oom-cgroup: PASS` in the output; `SKIP` also exits zero and does
not verify recovery. Do not move unrelated processes out of a shared tmux or
agent cgroup to enable memory control. See the README's
[OOM test instructions](README.md#testing) for user-bus setup, memory headroom,
and overrides (pass these through `systemd-run --setenv=NAME=value`).

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
- Lock order: acquire B-tree relation mutexes before the global write lock when
  an operation needs both; never wait for a B-tree mutex while holding the global
  write lock. The global write lock still precedes smgr/catalog locks.
- **Compatibility target:** the eventual, far-away goal is byte-for-byte,
  bug-for-bug compatibility with PostgreSQL. When in doubt, port the original
  behavior rather than inventing a simpler one. The MVP subset is a scope
  limit, not a license to diverge inside that scope.
- **Original C source:** implementers and reviewers must always consult the
  corresponding PostgreSQL C source and relevant C tests for backend behavior
  changes. Compare the Lisp behavior with those references and explain any
  deliberate differences.
- Project workflow: review each milestone with a subagent.


### SBCL descriptor streams and global variables

- An anonymous stream created by `sb-sys:make-fd-stream` need not support
  `file-length`, even when its descriptor names a regular file. The stale-PID
  restart test exposed this distinction. Use `sb-posix:fstat` / `stat-size` for
  descriptor metadata, and exercise the reopen path as well as initial creation.
- Some SBCL runtime variables are global lexicals rather than dynamically bound
  specials. `sb-ext:*exit-hooks*` cannot be LET-bound in this SBCL. A task-owned
  isolated test may save and restore its value with `unwind-protect`; do not infer
  dynamic binding behavior from earmuff naming. Use REPL introspection/compiler
  diagnostics to verify the variable kind when a test binding is rejected.

### Startup and timeout debugging

- Patroni's driver wrapper appends `-c search_path=pg_catalog` to connection
  options. Inspect the actual wrapper as well as ConnectionPool defaults. A
  startup setter can emit ParameterStatus through the backend stream even if
  its SQL response stream is discarded; suppress reports until authentication
  is complete. The private control REPL can instrument `write-message` in the
  owning server to distinguish discarded messages from actual wire output.
- Ordinary control evaluations hold live-operation admission. To test a closed
  reload gate, create a separate task-owned worker and use private file/semaphore
  barriers. Let the control client disconnect before releasing the worker to
  close admission; otherwise its `(quit)` can wait behind the gate it triggered.
- Check the exact C interrupt handler before making cancellation more aggressive.
  Pinned `ProcessClientWriteInterrupt` handles only ProcDiePending, deliberately
  deferring statement timeout during blocked partial output. Unwinding an SBCL
  write and retrying a whole frame can duplicate bytes; adding an IO timeout is
  not a safe substitute for reviewing protocol buffering and C behavior.
- Restart the cl-mcp worker after backend structure layout changes. A replacement
  worker loses ASDF registrations as well as definitions; register the project's
  `.asd` and load the system before interpreting a missing-dependency result as
  a source or dependency regression.
