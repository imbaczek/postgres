# Development TODO

- [x] Resolve the admission and receiver join race during concurrent reload.
  Receiver quiescence holds reload exclusion with admission open through stop,
  join and exit flush. It releases exclusion it acquired before replay joins;
  caller-owned exclusion remains the caller's responsibility. An
  existing reload owner with closed or terminal admission is rejected before
  stop is published. The checkpoint passed 1,439 physical checks and strict
  cold compilation. The owned-server `configured-follow.sh` matrix exercised
  manual stop and configured clear while reload admission was closed, followed
  by resume, metadata, retarget, cold restart and promotion. Its fixture simulates
  the reload lock and quiescence; it does not compile ASDF inside the gate.
  Evidence is in `/home/imbaczek/proj/pg-ha-artifacts/receiver-reload/`.

- [x] Add a native TCP primary clone for Patroni's default backup method.
  Staging pauses mutations, then streaming permits SQL to proceed. The native
  command and image differ from C's online base backup; a permanent follower
  slot must retain WAL until attachment. The reviewed checkpoint passed 903 unit,
  1,430 physical, 201 wire and 1,161 legacy checks and strict cold compilation.
  The pinned Patroni 4.1.5 TCP test covers HBA, format and occupied destination
  rejection, wrapper cancellation, SQL during streaming, damaged streams, source
  crash cleanup, cold follow restart and cloning a promoted timeline 2 primary.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/native-basebackup/pg-backup-export-hba-recheck.log`.
  Rewind, offline primary adoption, unsupported checkpoint fields, write/flush
  parity, the cold-load audit and broader documentation remain open.

- [ ] Support schema-qualified INSERT targets such as `INSERT INTO public.t`;
  the current parser returns 42601 although an unqualified target parses.

- [x] Flush outstanding WAL on an eligible writer cycle with an empty publication
  FIFO (`cc9ea0202c3`, `68785ed66c4`, `07c9368c734`). The checkpoint passed 903
  unit, 1,416 physical, 201 wire and 1,161 legacy checks and strict cold
  compilation. Six native idle-writer cases, eight FIFO regressions and one C
  unassigned read-only WAL flush case passed. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/idle-writer/`. Properties ran 100 trials
  at seeds 4020 and 4021 with respective digests `08daeb35cb803c4f` and
  `e5f7a51c19219c1c`; the authority target has no Function Spec, and input
  coverage and rejection counts were not measured. C's page rounding and
  background flush timing policy remain different.

- [x] Expose the native checkpoint SQL record and scalar oid8 subset
  (`f6cc550e1f8`, `2b2236e4aed`). The 20-column descriptor uses selected durable
  facts; eleven unavailable fields and additional missing legacy facts are NULL.
  The checkpoint passed 901 unit, 1,416 physical, 201 wire and 1,161 legacy
  checks and strict cold compilation. An independent C/native oracle checked
  metadata, text and numeric input/casts, JSON, primary control facts and the
  unchanged Patroni standby-leader query, including source-offline cold recovery
  and corrupt-pointer rejection. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/control-checkpoint/`. Two properties ran
  100 trials each at seeds 4018 and 4019, with respective digests
  `b9ea210f07666dd1` and `0aa6a19aa62a5126`.

- [x] Support typed scalar and fixed-descriptor composite functions in FROM
  (`b984839c702`, `de6637b686e`). The checkpoint passed 891 unit, 1,413
  physical, 201 wire and 1,161 legacy checks and strict cold compilation. Native C
  comparison covered 36 SQL cases for values, metadata and errors and two binary TEXT
  Bind/rebind cases; the 12 dollar-quote cases also passed. Existing binary INTEGER
  formats remain unsupported. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/scalar-from/`: `cold.log`, `physical.log`,
  `wire.log`, `legacy.log`, `native-c-reviewed.log`, `dollar-c-reviewed.log` and
  `live-format-diagnosis.log`; unit checks ran through the tool. The property over
  `parse-from-relation` and `expand-select-targets` ran 100 trials at seed 4014
  (digest `fnv1a64-v1:996181b0f1e156ab`); neither target has a registered
  Function Spec, and input coverage and rejection counts were not measured.
  At that checkpoint DO, PERFORM, `txid_current()` and full Patroni acceptance
  remained open.

- [x] Scan dollar-quoted strings and parameter boundaries (`1a831afe4bc`).
  At that checkpoint DO, PERFORM, `txid_current()` and scalar FROM support remained
  open. The checkpoint passed 885 unit and 201 wire checks and
  strict cold compilation. The native C comparison passed 12 value and error
  position cases, including Unicode tags, overlapping delimiters, parameter
  boundaries and a one-million-character literal. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/dollar-quoting/`:
  `cold-final.log`, `wire.log` and `native-c-reviewed.log`; unit checks ran through the
  tool. The helper/tokenizer property ran 100 trials at seed 4013 (digest
  `fnv1a64-v1:9318d3c8888b3fa3`); neither target has a registered Function Spec,
  and rejection counts and input coverage were not measured.

- [x] Enable public `synchronous_commit=off` (`0ce95c4e409`, `24cebb6d345`).
  The checkpoint passed 882 unit, 1,413 physical, 201 wire and 1,161 legacy
  checks, strict cold compilation, six native public OFF crash cases and the
  settings and fallback checks. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/public-async/`: `cold.log`,
  `physical.log`, `wire.log`, `legacy.log`, `native.log`, and
  `settings-final-reviewed.log`; unit checks ran through the
  tool. The normalization and policy property ran 100 trials at seed 4012
  (digest `fnv1a64-v1:db34c9708e1d3c6f`); neither target has a registered
  Function Spec, and rejection counts and input coverage were not measured.

- [x] Integrate private asynchronous acceptance with receipts, CLOG and backend
  cleanup (`280dc7f3541`, `0ff4a132af7`). A queued owner retains its resources
  after backend cleanup; pressure, resets or writer unavailability force a flush.
  Final DDL and remote waiting also force local flush. At that checkpoint public
  OFF remained disabled.
  The checkpoint passed 881 unit checks through the tool, 1,413 physical checks,
  strict cold compilation, six native crash/acceptance, eight native writer and
  eight native receipt cases. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/async-acceptance/`: `physical-final.log`,
  `cold-reviewed.log`, `native-crash-final.log`, `native-writer.log` and
  `native-receipts.log`. No new cl-spec contract was added for this milestone.

- [x] Add the logical CLOG acceptance transition for a linked pending
  publication (`d4a0b22ab30`). At that checkpoint the helper had no production
  caller and receipt and backend cleanup integration remained open. The checkpoint
  passed 874 unit and 1,413 physical checks and strict cold compilation. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/accepted-clog/` (`physical.log` and
  `cold.log`); unit checks ran through the tool. The property
  `accepted-clog-neighbor-writes-preserve-wal-order` ran 100 trials at seed 4011
  (digest `fnv1a64-v1:dadd7089ff788acf`); its target has no registered Function Spec,
  and rejection counts and input coverage were not measured.

- [x] Add retained WAL publication pressure accounting and admission
  (`e8673faf91a`). Pressure can settle predecessors and require synchronous
  completion; the limits bound asynchronous acceptance, not a transaction's
  transient allocations. At that checkpoint OFF remained disabled and integration
  still needed to honor the admission helper's third return value. It passed 871
  unit, 1,413 physical, 1,161 legacy and 201 wire checks, strict cold compilation,
  eight native writer cases including pressure and SIGKILL, 21 native page and five
  native CLOG cases. Logs are in `/home/imbaczek/proj/pg-ha-artifacts/wal-pressure/`.
  Two cl-spec properties ran 100 trials each at seeds 4009 and 4010, with respective
  digests `491b955b8bd3e744` and `19c520ff03696461`. Neither pressure property
  has a Function Spec; coverage and rejection counts were not measured. Asynchronous acceptance, OFF,
  and unrelated WAL flushing with an empty FIFO remained open at that checkpoint.

- [x] Add the primary WAL writer authority and FIFO drain foundation. No writer
  loop or startup is enabled. The checkpoint passed 842 unit checks and strict
  cold compilation. The scoped property ran 100 trials at seed 4007 (digest
  `fnv1a64-v1:e5f7a51c19219c1c`); it has no Function Spec, and coverage and
  rejection counts were not measured. At that checkpoint, adoption guards and
  CLOG reset epoch preparation and publication remained open.

- [x] Guard storage adoption against live or unjoined writers and prepare and
  publish CLOG reset epochs (`cf1a4a95d8d`). The checkpoint passed 851 unit,
  1,413 physical, 1,161 legacy and five native CLOG checks, plus strict cold
  compilation. Evidence is in `/home/imbaczek/proj/pg-ha-artifacts/wal-writer-epoch/`:
  `physical-current.log`, `legacy-current.log`, `native-clog-current.log`, and
  `cold-final.log`. The property `stale-clog-reset-plans-have-no-storage-effects`
  ran 100 trials at seed 4008 (digest `fnv1a64-v1:c2552cb71de45bf9`); there is
  no Function Spec, and coverage and rejection counts were not measured. At that
  checkpoint the loop and lifecycle had not yet been activated.

- [x] Activate the primary WAL writer lifecycle (`09d10de9ca7`, `1ece86c5993`).
  At that checkpoint the auxiliary writer drained the publication FIFO; unrelated
  WAL flushes with an empty FIFO remained open. It passed 858 unit, 1,413 physical,
  1,161 legacy and 201 wire checks, strict cold compilation, six native writer
  cases including SQL XID wrap, 21 native page, five native CLOG and two snapshot
  cases, native promotion and shutdown. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/wal-writer-lifecycle/`: `physical.log`,
  `legacy.log`, `wire.log`, `cold.log`, `native-final.log`, `native-wrap.log`,
  `native-pages.log`, `native-clog-current.log`, `native-snapshot.log`,
  `native-promotion-current.log`, and `shutdown.log`. No new cl-spec contract
  was added. Bounded pressure, asynchronous acceptance proof, the logical CLOG
  overlay, OFF and full Patroni acceptance remain open; no coordination was added.

- [x] Add the generic transferable temporary cleanup domain foundation. At that
  checkpoint fresh private owners could be adopted before use, but no production
  caller adopted one; WAL integration and later work remained open. The checkpoint
  passed 829 unit, 1,413 physical, 201 wire,
  and 1,161 legacy tests, strict fresh compilation, native spill and live reload.
  Evidence is in `/home/imbaczek/proj/pg-ha-artifacts/cleanup-domain/`:
  `physical-suite.log`, `cold-final.log`, `native-spill-reviewed.log`,
  `reload.log`, `wire.log`, and `legacy.log`. The cl-spec property
  `cleanup-domain-adoption-preserves-private-owner-and-quota` ran 100 trials
  each at seeds 42 and 2707969488570409574 (digest
  `fnv1a64-v1:320a49a385e9fcd9`). This adoption contract has no Function Spec;
  input coverage and rejection counts were not measured.

- [x] Give primary WAL spools and shared dirty scratch store-owned cleanup
  authority (`ff63d497263`). Cleanup retries retain unresolved ownership across
  threads and storage boundaries reject remaining debt. Recovery page spools
  retain ordinary creator-thread cleanup ownership. This adds no tracked primary
  writer, asynchronous OFF, snapshot-prefix handling, or coordination. The
  checkpoint passed 834 unit, 1,413 physical, 201 wire and
  1,161 legacy checks, strict fresh compilation, 21 native page and five native
  CLOG cases (including production SQL FIFO completion on another thread), native
  spill and live reload. Evidence is in `/home/imbaczek/proj/pg-ha-artifacts/wal-cleanup/`:
  `physical-current.log`, `cold-final.log`, `native-pages.log`, `native-clog.log`,
  `native-spill.log`, `reload.log`, `wire-current.log`, and `legacy-current.log`.
  The 834 unit checks ran through the tool; there is no `unit-final.log` and no
  new cl-spec contract for this milestone.

- [x] Settle the pending primary WAL publication prefix before capturing a fresh
  snapshot (`034d03464c4`), keeping WAL locked through XID capture and selecting
  logical snapshot boundaries after settlement. Existing transaction and portal
  snapshot reuse remains unchanged. This adds no tracked primary WAL writer,
  OFF, or coordination.
  The checkpoint passed 837 unit checks through the tool, 1,413 physical, 1,161
  legacy, 201 wire, strict cold compilation, 21 native page and five native CLOG
  cases, and two native snapshot reuse cases. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/snapshot-prefix/`: `physical-initial.log`,
  `legacy.log`, `wire.log`, `cold-initial.log`, `native-pages-final.log`,
  `native-clog.log`, and `native.log`. There is no final unit log or new cl-spec
  contract for this milestone.

- [x] Implement the source sender-status foundation. The sender and activity
  SRFs can be joined by PID. Captured registration tokens enforce ownership;
  snapshots are detached copies with independent, nullable feedback positions.
  Connection cleanup retires the registration. Activity exposes only PID
  and application name for registered backends, including NULL enumeration.
  Lag is unknown and reported NULL; sync priority/state are 0/async. All SQL
  sessions currently use the postgres superuser; ordinary-role visibility
  filtering is not implemented. The source C oracle is
  `/tmp/pg-sender-status-c-oracle-fhw2qqsb/result.log`.

- [x] Support whole-row arguments and composite JSON conversion in the supported
  query subset. An unmatched outer row is SQL NULL, distinct from a present
  all-NULL record. Named composite types and whole-row RETURNING remain unsupported.
  C parity evidence is `/tmp/pg-sender-status-c-oracle-fhw2qqsb/whole-row-parity.log`.

- [x] Support `json_agg` in the supported aggregate and window subset. The
  sender-status JSON subquery is tested with real registry rows through both
  callback and iterator execution. Aggregate DISTINCT and ORDER BY remain
  unsupported. C parity evidence is
  `/tmp/pg-sender-status-c-oracle-fhw2qqsb/json-aggregate-parity.log`.

- [x] Establish the commit lifecycle prerequisite for later synchronous waiting:
  retain the XID through local apply and retire resources on an indeterminate
  poisoned WAL outcome. No remote wait or coordination was added. The existing
  no-WAL replacement and CLOG failure boundary is not claimed hardened. Final
  code passed 709 unit and 201 wire tests; 1,323 physical tests passed before a
  diagnostic-only first-error fix, covered by a new two-error unit test. Strict
  cold compilation passed (`/tmp/pg-commit-lifecycle-final-cold.log`), as did
  actual psql smoke (`/tmp/pg-commit-lifecycle-smoke.log`).

- [x] Establish a termination handoff for later synchronous waiting: publish
  pending termination before queuing the target interrupt, and consume it at
  delivery. No remote wait was added. The checkpoint passed 711 unit and 201
  wire tests, strict cold compilation, and the real shutdown test
  (`/tmp/pg-sync-termination-shutdown.log`).

- [x] Add the synchronous standby configuration parser and sender selection
  foundation. At this checkpoint no GUC, public synchronous state or commit wait
  was wired, and config, feedback and source fencing integration remained open.
  Restart the Lisp image for the sender structure layout change. The checkpoint
  passed 719 unit and 201 wire tests, plus seven existing sender physical
  regressions (not a full physical rerun); strict cold compilation passed
  (`/tmp/pg-syncrep-selection-final-cold.log`). The 31-case C parser oracle is
  `/tmp/pg-syncrep-parser-c-oracle.json`. Scoped cl-spec checks at seed 42 ran
  100 trials each: parser Function Spec (digest `2ff4d2ff15c97d0d`), quoted
  property (`a3f4bbb1a9500ce5`) and quorum property (`99cb3720a61bacdc`). Input
  coverage and rejection counts were not measured.

- [x] Add the private synchronous publication and wait core. Real worker
  threads modeled feedback, config, cancel and termination; SQL commit waiting
  was not tested or wired at that checkpoint. It passed 732 unit and 201 wire tests,
  seven existing sender physical tests (not a full physical rerun), and strict
  cold compilation (`/tmp/pg-sync-wait-reviewed-cold.log`). The scoped cl-spec
  queue-prefix property ran 100 trials at seed 42 (digest `9248d664822bed72`);
  input coverage and rejection counts were not measured. The user-XID wait is
  now wired; public settings, final DDL and maintenance policy, and asynchronous
  `synchronous_commit=off` remain open below.

- [x] Capture exact durable catalog terminal boundaries before local apply, with
  fail-stop publication and backend failure fencing (`a59b2256057`). Install a
  fresh source incarnation at startup, promotion and accepted follow or history
  changes; capture sender ownership during startup (`cdfa0829c6c`). Capture each
  transaction's own catalog and commit boundaries for future waiting
  (`809b80b6110`). The latest checkpoint passed 744 unit, 1,325 physical and 201
  wire tests, strict cold compilation (`/tmp/pg-transaction-obligation-cold.log`)
  and SQL smoke. This captures obligations only; it adds no remote wait.

- [x] Prepare and publish owned configuration roots under existing quiescence,
  with predecessor checks and follow generation publication (`cb3a1f3ee46`).
  The checkpoint passed 744 unit, 1,332 physical and 201 wire tests, strict cold
  compilation (`/tmp/pg-prepared-config-cold.log`), SQL smoke
  (`/tmp/pg-prepared-config-smoke.log`) and live valid/invalid reload checks.
  At that checkpoint production readers still used globals.

- [x] Capture one installed root for SHOW/settings rows, authentication backup
  and startup admission, and derive reload candidates from their captured base
  (`c907ba9d05b`). The checkpoint passed 744 unit, 1,337 physical and 201 wire
  tests, strict cold compilation (`/tmp/pg-config-readers-cold.log`), SQL smoke
  (`/tmp/pg-config-readers-smoke.log`) and live divergence/reload checks. No root
  means uninstalled; quota admission takes explicit captured limits.

- [x] Capture restartpoint settings before the store mutex (`99ade2667aa`) and
  pass one retention policy and keep size through each removal calculation and
  catalog snapshot. Owner claims and removal retries capture fresh
  settings while preserving the ticket's WAL endpoint. The checkpoint passed
  744 unit, 1,341 physical and 201 wire tests, plus 1,159 legacy checks, strict
  cold compilation and SQL smoke. No independent configuration service is added.

- [x] Fence receiver-local effects and desired generation publication
  (`074b04f0e48`). Admission precedes the receiver fence; network I/O and joins
  stay outside it. A superseded generation still flushes previously accepted
  bytes while its runtime owns the store; a retired runtime leaves it alone.
  Runtime publication and retirement use the same fence. The checkpoint passed
  749 unit, 1,350 physical and 201 wire tests, 1,159 legacy checks, strict cold
  compilation and SQL smoke. At that checkpoint controller durable publication
  and the independent configuration service remained open.

- [x] Fence controller intent and active publication and defer notification of
  captured obsolete senders until after fence release (`a50140c079c`,
  `54702573208`). The installer still requires quiescence. The checkpoint passed
  1,363 physical (`/tmp/controller-fence-final-physical.log`), 752 unit
  (`/tmp/controller-fence-final-unit.log`), 201 wire
  (`/tmp/controller-fence-final-wire.log`) and 1,159 legacy checks
  (`/tmp/controller-fence-legacy.log`), strict cold compilation
  (`/tmp/controller-fence-cold.log`) and SQL smoke
  (`/tmp/controller-fence-smoke.log`).

- [x] Close worker-start interruption gaps in `physical-follow-start-operation`
  and `physical-follow-launch-workers` (`8687a20236b`). Mask thread creation,
  owner publication and start-gate release together; failures before handoff
  completion release each child without storage work. The checkpoint passed 1,367 physical
  (`/tmp/worker-start-final-physical.log`), 752 unit
  (`/tmp/worker-start-final-unit.log`), 201 wire
  (`/tmp/worker-start-final-wire.log`) and 1,159 legacy checks
  (`/tmp/worker-start-legacy.log`), strict cold compilation
  (`/tmp/worker-start-cold.log`) and SQL smoke (`/tmp/worker-start-smoke.log`).

- [x] Fence private history selection and live promotion through source rotation,
  carrying sender retirement and completion through the live promotion
  (`88a00f72820`, `39bf35d5396`). The checkpoint passed 1,370 physical
  (`/tmp/source-lifecycle-final-physical.log`), 752 unit
  (`/tmp/source-lifecycle-final-unit.log`), 201 wire
  (`/tmp/source-lifecycle-final-wire.log`) and 1,159 legacy checks
  (`/tmp/source-lifecycle-final-legacy.log`), strict cold compilation
  (`/tmp/source-lifecycle-final-cold.log`) and SQL smoke
  (`/tmp/source-lifecycle-final-smoke.log`). Real HBA backup and promotion with
  `DEBUG_PROMOTION=1` passed (`/tmp/source-lifecycle-hba-debug.log`, artifacts
  `/tmp/pg-hba-policy.49dp84dn`).

- [x] Run configuration reload independently of code drain (`c1c7f634e8a`,
  `6476238b1fd`). Candidate preparation and publication are serialized with
  definition replacement without taking code reload admission. This supplies no
  failover coordinator.

- [x] Wait for a user XID's own locally committed WAL boundary using the private
  synchronous wait mode (`ad8194a65c6`). The checkpoint passed 1,407 physical
  tests plus three final focused cases, 754 unit, 201 wire and 1,159 legacy
  checks, strict cold compilation and an owned seven-case SQL script.

- [x] Capture explicit final DDL catalog boundaries and wait without an XID;
  XID-bearing DDL waits for its user commit using the prepared mode
  (`64877d34169`). The checkpoint passed 1,409 physical, 755 unit, 201 wire
  and 1,159 legacy checks, strict cold compilation, a focused no-WAL rerun and
  the native DDL scenario script. The scoped cl-spec property ran 100 trials at
  seed 42 (digest `942121ab116a97c6`); coverage and rejection counts were not
  measured.

- [x] Prepare synchronous replication configuration before publishing it under
  the sender lock (`a6eae66b69d`). The checkpoint passed 1,409 physical, 759
  unit, 201 wire and 1,159 legacy checks, strict cold compilation and native
  user and DDL scripts. The scoped cl-spec property ran 100 trials at seed 42
  (digest `3f289db7c7c4a86e`); input coverage and rejection counts were not
  measured. At that checkpoint root and generation wiring remained open.

- [x] Publish validated `synchronous_standby_names` with the configuration root
  (`6aad437aef5`). The SIGHUP string defaults to empty; SQL SET returns 55P02.
  The native rescue test preserves blocked commit and XID waits across three
  invalid reloads, releases both after clearing names, and shows SQL progress
  while a retained portal holds code reload until it closes. The checkpoint
  passed 1,413 physical, 760 unit, 201 wire and 1,159 legacy checks, strict cold
  compilation, native user and DDL scripts, and real configured follow. The
  scoped cl-spec property ran 100 trials at seed 42 (digest `5501aa7e8c09f1d4`);
  input coverage and rejection counts were not measured.

- [x] Support session and client `synchronous_commit` values `on`, `local`,
  `remote_write` and `remote_apply`, including SET LOCAL and RESET
  (`b57b9154ce1`). `on` waits for remote flush; `local` keeps local durability.
  OFF and false aliases return 0A000 pending true asynchronous commit. The
  checkpoint passed 1,413 physical, 772 unit, 201 wire and 1,159 legacy checks,
  strict cold compilation and native DDL, user SQL and default-change reload
  tests. The scoped property ran 100 trials at seed 42 (digest `76743fc438b0bd6f`);
  coverage and rejection counts were not measured. Configuration publication is
  independent, but SIGHUP also requests code reload and can pause new SQL while
  draining an active commit. The reload test uses a pre-admitted callback across
  SIGHUP; ordinary control evaluation also requires admission.

- [x] Report current FIRST/ANY sender priority and state from one sender-lock
  snapshot (`336ab343ffb`). The native status test uses an owned server, public
  reload and SQL with controlled sender registrations. Insufficient ANY peers
  still show quorum candidates; unmeasured lag is NULL. The checkpoint passed
  1,413 physical, 777 unit,
  201 wire and 1,159 legacy checks, strict cold compilation and the native status,
  user, DDL and reload tests. The physical run used a tracked private active-XID
  binding; the unit run followed clearing 247 FASLs. The property ran 100 trials
  each at seeds 42 and 98749024680794852 (digest `cdce92d88fc2e9ad`); input
  coverage and rejection counts were not measured.

- [x] Capture and validate immutable commit acceptance receipts (`44f402191f6`).
  For WAL-ready commits, the validator requires the observed durable callback and
  WAL proof. OFF remains rejected with 0A000. The checkpoint passed 1,413 physical, 786 unit,
  201 wire and 1,159 legacy checks, strict cold compilation, native receipt (8
  PASS) and user (7 PASS) tests, and DDL and reload tests. The property ran 100
  trials each at seeds 42 and 3286687836371262213 (digest
  `fnv1a64-v1:92a441ee7fa3a065`); rejection counts and input coverage were not
  measured.

- [x] Capture a commit policy once before publication (`e01e84fadb3`), keeping
  requested local durability and remote wait distinct from receipt facts. Final
  DDL retains that policy for both XID and no-XID completion; an XID after a
  published final catalog boundary forces effective local flush without changing
  the request or adding a remote wait. C likewise forces a flush for forced-sync
  commits and non-temporary relation deletion before files can be deleted; Lisp
  additionally waits on the final catalog boundary for no-XID DDL when requested.
  The structure layout change requires a fresh worker. Strict cold compilation,
  1,413 physical, 790 unit, 201 wire and 1,159 legacy checks passed. Native
  receipt (8), user (7), DDL and reload checks passed. The scoped property ran
  100 trials each at seeds 42 and 4538924396900646344, digest
  `fnv1a64-v1:11f68abafa1ea68c`; rejection counts and input coverage were not
  measured. True `synchronous_commit=off` remains pending CLOG/page overlay,
  common FIFO and WAL writer work; Patroni acceptance remains open.

- [x] Add the pending CLOG foundation (`8a2a92a720d`). Production commits
  still materialize immediately after WAL flush, so pending CLOG after return is
  not enabled. The checkpoint passed 1,413 physical, 797 unit, 201 wire and
  1,161 legacy checks, strict cold compilation, native receipt (8), user (7),
  DDL and reload checks. Four failure cases cover fail-stop and restart before
  and after the byte write, SIGKILL after staging, and SIGKILL after reset WAL
  flush before replacement; the last proves redo because a previously committed
  CLOG slot is zero after restart while a frozen row survives. Process kills do
  not simulate power loss. The
  fresh-process legacy fixture now resets poison and epoch, with two assertions;
  no old tests were ported. The property ran 100 trials each at seeds 42 and
  3263075368047784275, digest `fnv1a64-v1:0aa70a24580bcf9c`; rejection counts
  and input coverage were not measured.

- [x] Own pending page publication through durable materialization (`65409e21153`). A single
  owner retains the sealed spool and dirty maps at the exact terminal boundary;
  the current commit flushes and drains it immediately before acceptance.
  The checkpoint passed 801 unit, 1,413 physical, 201 wire and 1,161 legacy
  checks, strict cold compilation, nine native page, eight receipt and four
  CLOG failure cases. The ownership property ran 100 trials each at seeds 42 and
  3483828763443979182, digest `fnv1a64-v1:a52e06a995fdb660`; rejection counts
  and input coverage were not measured. A lifecycle fault fixture now isolates
  its recovery-failure flag. Native tests caught a pre-flush response leak and
  a guard outside WAL that rejected a healthy competing commit; both are fixed.

- [x] Order transaction and maintenance page publications in a common FIFO
  (`63c3684d70c`). Normal calls flush and drain immediately. Synchronous barriers
  settle predecessors; cold installation and identity changes reject debt. Resets
  force sync and settle predecessors before private publication. Catalog and drop
  closures are not queued. The native WAL format is unchanged.
  The checkpoint passed 807 unit, 1,413 physical, 201 wire and 1,161 legacy checks,
  strict cold compilation, 21 native page cases, eight receipt and seven user
  cases, DDL and reload checks, and four CLOG failure/crash cases. The prefix
  property ran 100 trials each at seeds 42 and 3282750418480750988, digest
  `fnv1a64-v1:81cff11f8481e2b2`; the ownership property ran 100 each at seeds
  42 and 145193323930899829, digest `fnv1a64-v1:a52e06a995fdb660`. Rejection
  counts and input coverage were not measured for either property.

- [x] Add the CLOG completion reservation prerequisite (`f87ede05484`). A
  reservation can precede the terminal append without exposing committed status;
  completion requires durable WAL. Production queue integration remains open,
  and OFF remains disabled. The checkpoint passed 811 unit tests, four native
  cases and strict cold compilation. The scoped property ran 100 trials each at
  seeds 42 and 265648549833292381, digest `fnv1a64-v1:93537ecfe22d4395`;
  rejection counts and input coverage were not measured.

- [x] Integrate CLOG completion with the publication FIFO (`ec9f21ebe3f`).
  Transaction owners reserve before terminal insertion, publish against the actual
  end, and materialize CLOG at the durable queue head before page retirement.
  Normal production commits still flush and drain synchronously; OFF remains
  disabled. The checkpoint passed 820 unit, 1,413 physical, 201 wire and 1,161
  legacy checks, strict cold compilation, five native CLOG cases (including
  cross-thread SQL and restart), 21 page, eight receipt and seven user cases,
  DDL and reload checks. The `queued-clog-and-pages-retire-the-same-prefix`
  property passed 100 trials each at seeds 42 and 4575550243879747699, digest
  `fnv1a64-v1:71d269dfe392cfa8`; rejection counts and input coverage were not
  measured, and there is no function spec. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/queued-clog/`.

- [x] Implement asynchronous OFF using captured policy and acceptance receipts,
  a tracked primary writer, bounded retained-resource pressure and CLOG overlay.
  Preserve WAL-to-CLOG lock order and never put pending committed bits on disk
  before WAL. No coordination was added.

- [ ] Match C background WAL write and flush timing and page-rounding policy;
  native eligible cycles currently sync the complete tail under WAL exclusion.

- [x] Implement `txid_current()` (`34044987044`) and the inline DO/PERFORM
  subset (`90fed2d98ff`) required by Patroni's unchanged sync-change method.
  The pinned monitor oracle (`f46d7c0ad0c`) passes its full query and unchanged
  sync handler on C and native primaries with an empty sender registry. C feedback
  clients check FIRST/ANY monitoring; real native Patroni followers also check
  one handler's FIRST-to-ANY transition. Evidence is
  `/home/imbaczek/proj/pg-ha-artifacts/patroni-sync/final.log`.

- [x] Exercise independent FIRST/ANY acknowledgment and crash boundaries against
  C and native peers (`b97973dab93`, `d36f2f45a0e`). The checkpoint passed 898
  unit, 1,416 physical, 201 wire and 1,161 legacy checks, strict cold compilation,
  50 C/native feedback cases and the Patroni regression. Native ANY 2 crash
  recovery cold-started both credited followers while the source stayed down.
  Evidence is `/home/imbaczek/proj/pg-ha-artifacts/sync-ack/`; the 100-trial
  reanchor property at seed 4017 had digest `fnv1a64-v1:a46ee7aac2e86822`, with no Function
  Spec for its target. The existing 200-trial property at seed 42 retained digest
  `fnv1a64-v1:23e4bc80c1e52a8e`.

- [ ] Complete the broader synchronous-replication backend contract.
  Persist missing checkpoint policy, freeze, MultiXact, commit-timestamp and
  checksum facts before claiming the complete C checkpoint contract.
  Complete remaining bootstrap and rejoin work, and finish the cold-load audit
  and broader C-style documentation. No coordination is added.

- [x] Publish an allocated backend XID with its active registration under the XID
  lock and interrupt mask, following C's ownership-before-unlock ordering. Raw
  `assign-xid` without an owner remains caller-owned. Real SQL termination tests
  cover the reserved XID 3 and cached XID 4 paths, abort cleanup and clean fast
  shutdown (`/tmp/pg-xid-owner-sql.log`, artifacts `/tmp/pg-xid-owner-q296anu1`).
  The milestone passed 1,312 physical, 683 unit and 186 logical wire tests, a
  strict cold compile (`/tmp/pg-xid-owner-cold.log`) and Patroni reconnect and
  promotion (`/tmp/pg-xid-owner-patroni.log`).

- [x] Implement desired-generation reconciliation for `primary_conninfo` and
  `primary_slot_name` for native standard follow, including reload, durable
  management ownership, cold restart and promotion. `recovery_target_timeline`
  is active at cold startup and changes require restart. Archive-only recovery
  remains unsupported; coordination is outside this scope.

- [ ] Complete native bootstrap and rejoin for the pinned Patroni path. Real
  `Postgresql.follow` and promotion now cover a descendant handoff, ancestry
  rejection and explicit local reseeding (`53b16b79f49`); the oracle has no DCS
  or coordination. Authenticated native export and installation now support the
  default remote Patroni bootstrap clone, with staged publication and cancellation
  cleanup. Even a nondivergent former primary lacks
  verified recovery provenance; safe offline prefix-proved adoption remains open.
  Native rewind and divergent rewind are unimplemented, and C tools are incompatible.
  The complete unchanged Patroni writer oracle now passes, including `hba_file`
  and `ident_file`. These settings accept only disabled or unset values:
  `max_prepared_transactions`,
  `track_commit_timestamp`, `recovery_target`, `recovery_target_lsn`,
  `recovery_target_name`, `recovery_target_time` and `recovery_target_xid`.
  Positive prepared-transaction capacity, commit timestamp tracking and targeted
  recovery remain unimplemented. Keep the complete writer oracle unchanged.
  Ordered trust/reject HBA rules are enforced at startup, with validated reloads
  and active-policy preservation in physical backups. SQL role authorization,
  other methods and selectors, raw-byte names split at 63 UTF-8 bytes, and scoped
  IPv6 addresses remain unsupported. Numeric IPv4 includes C's legacy forms.
  `wal_level` accepts replica and logical; minimal WAL generation remains
  unsupported. Replica uses dynamic logical capture for live or arming slots,
  while logical captures even without slots. Each XID keeps its capture choice.
  `wal_log_hints` accepts on and off; supported native paths have no persistent
  hint-bit writers. Full C hint full-page-image behavior and rewind remain open.
  Source effective WAL level and activation status, including WAL/checkpoint
  propagation, remain unimplemented; logical decoding on a standby is unsupported.
  Internal checks now derive shared
  relation-object and owner counts from published registrations and waiting
  requests. Cold startup installs a finite shared object/owner pool together with
  the local resource tuple. `max_locks_per_transaction` now controls sizing and
  reporting, with default 128 and changes pending until restart.
  `max_worker_processes` installs and reports the managed-worker slot capacity;
  changes are pending until restart. The validated prepared-transaction setting
  supplies zero to the installed capacity; implementing two-phase commit remains
  open.
  C sizes a shared pool, not a per-transaction limit.
  Preserve atomic multi-relation admission, repeated
  acquisition, abort/cancellation/disconnect and virtual-reader cleanup.
  Persisted source resource requirements and standby validation now follow
  C's `XLogReportParameters` and `CheckRequiredParameterValues`.
  The shared five-field value, strict native codec, floor comparison, capacity
  arithmetic and optional positioned control metadata are implemented. Native
  utilities and independent control readers accept the extension. The explicit
  primary reporter now flushes an empty-batch parameter-change terminal before
  publishing and reading back control metadata. Primary redo repairs an interrupted
  publication from durable WAL. Logical/history readers and slot resume checks
  validate the terminal without publishing applied resource facts. Owned-server
  SIGKILL tests cover both publication boundaries. Ordinary primary startup and
  child-primary completion report installed capacity before production readiness.
  Ordinary checkpoints now carry an optional positioned tuple, and verified base
  images require exact agreement with copied control and a valid ancestor origin.
  Their manifest hashes bind the checkpoint facts without extra manifest fields.
  Exact facts now follow applied/required positions, restartpoint seeds, private
  follow scans and durable follow witnesses. Pending promotion freezes the parent
  fact, and its child checkpoint and primary control preserve it across restart.
  Legacy seeds reconstruct facts from available durable WAL before restoring
  storage when a known durable requirement needs proof. Missing legacy
  facts stay unknown; do not substitute current defaults or newer received facts
  for an older replay seed. Cold hot-standby admission now requires positive
  proof after reconstruction; OFF recovery permits unknown facts. Online
  insufficiency persists the report, pauses before later WAL, preserves readers,
  refuses promotion and shuts down on resume. Owned tests cover direct pauses,
  insufficiency discovered during promotion backlog replay, and adequate restart.

- [x] Guard direct WAL publication in `xid-assign-with-wal-reservation` and
  `wal-report-resource-parameters`. Their publication regions defer interrupts;
  preflight and lock waits remain cancellable. Controlled tests cover termination
  and storage failure. This targeted change does not establish WAL-wide coverage.
  One earlier source SIGINT poison log lacked a captured cause; controlled tests
  independently reproduced the asynchronous guard bug, and owned restart passed
  after the fix.

- [x] Fix signed integer GUC parsing for C23 binary prefixes independently of
  the unsigned recovery timeline parser. The signed converter uses C23 strtol
  when available and clears errno after symbol lookup. C/native metadata tests
  cover signs, units and malformed forms; 200 generated int32 cases pass.

- [ ] After implementation, tests and code reviews are complete, document modules,
  functions and structures in the style of the original C source. Use simple,
  straightforward language; explain purpose, assumptions, invariants, ownership
  and failure behavior where they matter. A gpt-6.1-terra subagent should write
  this pass; the primary agent reviews its documentation separately.
  The child checkpoint/primary, online promotion and reader-safe row replay
  modules, source running-transaction metadata helpers, covered standby snapshots,
  virtual reader lifecycle helpers, private child adoption and the private
  promotion reader-enrollment prerequisite, native online reader-preserving
  promotion, relation reader identity selection, statement snapshot scope bookkeeping,
  temporary stream ownership, executor parking, client application-name settings,
  supported implicit query blocks, SQL promotion, fresh child backup/follow and
  durable receive-history selection, source-side follow anchors, the read-only
  extent layer, immutable inventory publication, local retained-root ownership and
  receive-control inventory validation, receive-compaction sealing and configured
  native follow, and managed-worker lifecycle and shutdown have completed this
  pass and its review. Keep the same order for the remaining HA work.

- [ ] Restore legacy unit-test runner fixture state after the suite. A warm worker
  retained `*active-xids* = (3)` after 1,598 passing checks, causing later physical
  promotion and shutdown checks to fail. Use the intended fresh legacy entrypoint
  for acceptance and inspect or isolate fixture state before later warm suites;
  do not restart an owned live SQL server to hide leaks. A later warm worker also
  had orphan fixture XIDs and cached streams pointing at deleted legacy PGDATA;
  the fresh intended legacy runner passed 1,159 checks. The leaking test and
  suite-isolation fix are not yet known.

- [ ] Audit cold-image compilation and ASDF load-order dependencies across `pg`
  and its test systems. A warm REPL must not conceal missing declarations or
  compile-time dependencies.
  The physical-slot range validator exposed another instance: calling a timeline
  structure accessor before its `DEFSTRUCT` caused an inlining warning only in a
  cold compile. Moving the slot module after its dependencies fixed that case;
  the wider audit remains open.

- [ ] Reproduce and report Dissect's SBCL frame-local inspection fault. While Rove
  reported a missing installed configuration in a retention fixture, Dissect's
  `FRAME-LOCALS` called `SB-DI::SUB-ACCESS-DEBUG-VAR-SLOT` on a mutex frame and
  raised a memory fault. A direct handler reported the original `SIMPLE-ERROR`
  correctly. Preserve the original condition before collecting locals; do not
  infer storage corruption from this secondary reporting failure. Observed on
  2026-10-31 in SBCL process 1984912 during the retention-reader milestone.
  Background test logs must catch errors inside `with-open-file` so abort cleanup
  keeps the log. A direct REPL reproduction without Rove later exposed an ordinary
  promotion error hidden by the same frame-local fault.

- [ ] Extend implicit simple-query transactions to the remaining statement and
  transaction-control cases, including transactional DDL and TRUNCATE, using C as
  the reference. Those messages currently retain the prior per-statement path.

- [ ] Audit server-file and command-line defaults for `application_name`; client
  startup and SQL settings are implemented.

- [ ] Implement and verify C-style pre-authentication connection pooling,
  dead-end handling and authentication timeout. The current independent session
  quotas apply after startup packet parsing and do not bound pending raw sockets.

- [ ] Fix cl-mcp character-name alias acceptance and custom-reader handling of
  unknown packages across server and worker images; add regressions for parinfer
  repairs that alter balanced but misindented forms. Keep reader evaluation disabled.

- [ ] Fix cold test declarations in `t/promotion-intent-tests.lisp`.
  `call-with-promotion-fixture` binds `pg::*shutdown-mode*` and
  `pg::*shutdown-requested*` lexically before their declarations, producing four
  style warnings. Production strict compilation passes; this remains part of the
  open cold-load audit.

- [ ] Reject target/temporary path aliasing in atomic file publication. The WAL
  byte helper derives a `.tmp` pathname; a target already ending in `.tmp` can
  name that same file. A fixture exposed successful return followed by deletion
  of the target. Audit callers and add a regression for rejection or a distinct
  temporary name before changing the cleanup path.

- [ ] Add a real process-kill and restart oracle at the durable slot-loss save
  and WAL unlink boundary. Current injected cuts exercise this in process, but
  do not establish crash recovery across an actual process death.

The cold-image audit should cover:

- Cross-file constants and specials referenced before their definitions or
  proclamations, especially `LET` bindings compiled before `DECLAIM SPECIAL`.
- Macros, compiler macros, symbol macros, classes and structures whose definitions
  must exist during compilation; check `EVAL-WHEN` and ASDF dependency ordering.
  The active-follow tests exposed fixture macros used before their test module
  loads. Those calls now use the fixture function directly; audit other test
  modules for the same dependency hidden by a warm image.
  Include forward calls to structure predicates: the follow-anchor work needed
  local `NOTINLINE` declarations in the earlier source-pin module. A warm image
  compiled it without exposing the missing subtype definition.
- Entry points that work only after another system or optional feature has
  populated the image, and stale FASLs or inlined constants that conceal defects.
- Each supported system built and loaded in a fresh SBCL process with an empty
  output cache. Check diagnostics as well as exit status, including warnings
  deferred until compilation-unit completion.

For each finding, preserve a small cold-image regression and fix the declaration
or dependency at its owning layer. Keep expected SBCL reload/redefinition notices
distinct from actual compiler warnings. Live reload remains the iteration model;
cold builds are an independent verification path.

The child-base work exposed another warm-image dependency: bootstrap history
inventory used `timeline-history-entry` accessors before that `DEFSTRUCT` was
compiled. Warm tests passed, but a fresh strict compile and the formatter's
fresh SLIME image failed. The ASDF order now loads timeline definitions before
bootstrap/recovery. Preserve this cold-build check when extending those modules;
the broader audit remains open.

This remains an **open audit**. It was prompted by the receive-store lock opener
using the close-on-exec constant before its defining file loaded: the live image
already knew the constant, while fresh compilation warned. The compile harness
also returned success for that deferred warning. It now rejects deferred
compilation-unit warnings; the broader dependency audit remains open.

The active HA/backend implementation roadmap is [PHYSICAL-HA.md](PHYSICAL-HA.md).
