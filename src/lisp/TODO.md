# Development TODO

- [x] Recover interrupted preserved-data reseed installation moves.
  Verify the recorded move prefix or one completed rename ahead across the
  target, incoming image and quarantine. Sync visible journal progress before
  another move, then sync both move parents before recording the next step.
  Validate the installed base before publishing the terminal step. Keep the old
  entries in quarantine and retain the startup fences through follow handoff.
  Validation passed 1,852 physical tests, strict compilation and eight SIGKILL
  cases with fresh-process recovery and independent file identity checks.
  Code commits are `7bfa3540dfd` and `a31d9f8e27b`. Documentation is in
  `8216360fe1f`. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-install-moves/namespace-progress.json`.

- [x] Reconcile partial cancellation-phase publications.
  Derive ABORTING from the trusted final attempt after checking a canonical
  prefix in the held temporary file. Sync its empty truncation before writing
  replacement bytes. A fresh CLOSED/ABORTED source receipt authorizes ABORTED.
  Generic recovery also handles complete initial temporaries without a final
  attempt and rejects partial records in that state. Source cleanup uses the
  saved route while staging and incoming directories are unavailable.
  Validation passed 1,861 physical tests, 86 focused tests and strict compilation.
  Native checks covered 16 repair cuts, three complete initial-record recoveries
  and one expected partial-record refusal through fresh processes. The prefix
  property passed 80 trials at seed 102 with digest
  `fnv1a64-v1:dee007a39777a2a8`. Input coverage and rejection counts are unmeasured.
  Code is in `34165f98d0a`, with mutation-test corrections in `be47bf354b9`.
  Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-cancel-reconcile/progress.json`.

- [x] Add source authority for backup-lease handoff to a proved follow pin.
  Match the original base to an unused READY successor and durably publish
  CLOSING/HANDED-OFF before retiring bootstrap retention. Protect the successor
  until CLOSED, then allow normal retirement while keeping its historical lease
  assignment. Exact handoff retries settle the recorded outcome. Primary and
  standby startup validate successor ownership before persisting bootstrap loss.
  A completed backup can hand off after promotion through its INTERRUPTED state.
  Validation passed 1,872 physical tests and strict cold compilation. Fifteen
  actual SIGKILL cuts recovered in fresh processes with unchanged successor
  bytes and retained WAL. Tests also cover disk-only standby recovery and
  descendant receivers before and at the fork. Code and refactoring reviews
  passed. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-follow-handoff/progress.md`.

- [x] Expose backup-lease handoff through the replication protocol and client.
  BACKUP_LEASE_HANDOFF carries the original request and exact successor identity.
  It returns a checked CLOSED/HANDED-OFF receipt. The client snapshots ownership
  before I/O and rejects a different outcome or successor. Native checks passed
  both normal completion and reply loss after source commit, followed by saved
  route status, exact retries, successor retirement and historical retries.
  Source SQL remained writable, with no retained pins or helper owners.
  Validation passed 1,877 physical tests, strict cold compilation and 80 generated
  wire trials at seed 103, digest `fnv1a64-v1:702a786762318f65`. Input coverage
  and rejection counts are unmeasured. Code and refactoring reviews passed.
  Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-follow-handoff/progress.md`.

- [x] Add durable source allocation and startup cleanup for lease follow pins.
  Persist generation and name ownership, validate the complete parent and pin
  graph at startup, and finish canceled or aborted cleanup before releasing
  retention. Selected handoffs retain their separate closing and closed rules.
  Code: `b463c32c5db` and `3b59a0a6ab8`.

- [x] Expose allocated follow preparation and cancellation through the source
  protocol and strict client. The client freezes the intent before I/O; cancel
  returns a parent receipt and submitted identity, not a child-state claim.
  The fresh full run executed 1,962 distinct test heads with positive assertions;
  three later tests passed separately. Strict cold compilation passed. Native
  reply-loss checks covered four modes, restart and writable SQL. The wire
  property passed 100 trials at seed 113, digest `fnv1a64-v1:fb2a5a7ba229d029`;
  input coverage and rejection counts were not measured. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-lease-wire/progress.md`.

- [ ] Finish target reseed follow handoff and startup reconciliation.
  Persist the target's follow intent and inert configuration before source
  settlement. Reconcile the source outcome and activate follow only after the
  required durable checks; retire installation fences during startup reconciliation.

- [x] Add the target follow codec and read-only owner foundation. Validate
  bounded generation records and the installed predecessor and image under a
  scoped owner. The next item adds durable intent publication and recovery.

- [x] Publish and recover durable target follow intent. The local store accepts
  empty genesis and PREPARING intent only, with checked predecessor and image;
  incomplete publication fences the owner until reopen. Validation passed 200
  selected tests and two later tests, strict cold compilation, and eight SIGKILL
  cuts with 16 fresh images. A 100-trial property at seed 117 passed with digest
  `fnv1a64-v1:b04db31708e74ac1`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/progress.md`.

- [x] Prepare and record the target's historical source proof. Check the frozen
  route, writer identity, full history and flush around the exact prepare RPC,
  then prove the installed phase-zero base locally before publishing PROVED.
  A stable proof is historical; handoff must revalidate source authority. The
  public intent publisher still accepts only empty and PREPARING records.
  Validation accounted for 241 distinct selected tests (239 in a fresh image
  and two later tests), including 18 new tests, and a strict fresh compile
  of 204 files without warnings or errors. The empty-base property passed 100
  trials at seed 118, digest `fnv1a64-v1:f212ed2b3a0ae697`; input coverage and
  rejection counts were not measured, and there is no FunctionSpec. Native TLI 1
  and SQL-promoted TLI 2 sources each passed ten cases, including eight SIGKILL
  cuts, with ten target images and 19 fresh target processes per run. Native RPC
  tests cover same-timeline proofs; descendant and retained proofs used local
  installed fixtures and properties. See `prepare-notes.md`, `progress.md`, the prepare
  check JSON files and native results under
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/`.

- [x] Durably stage disabled target follow configuration. Save Building intent
  before creating workspace outputs, then publish Staged after checking and
  syncing the exact guard, directory and file identities and derived bytes.
  Reopen recognized complete prefixes; preserve torn or unexplained entries.
  Staged retries are read-only. The installed base remains at phase zero, and
  startup remains fenced. Code: `9fefd569231`, `0cd7cc2e9e5` and `321916a3d70`.
  Validation passed 266 distinct selected tests in fresh processes, strict
  compilation of 210 production files, and the 31 focused tests after splitting
  the new test modules. Each code commit also compiled as an independent staged
  snapshot. Native Lisp TLI 1 checks covered ten target images, 30 fresh target
  processes and nine SIGKILL cuts, with source-off construction and recovery.
  Native proofs were same-timeline; installed fixtures covered all five proof
  geometries. The copy property passed 80 trials at seed 120, digest
  `fnv1a64-v1:ee7af4059f73f009`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/progress.md`.

- [x] Define the inert configuration move plan and verification prerequisites.
  The pure plan accounts for every image across four or five moves, including
  the saved original control and unused staged images. Shared verification can
  check an actual decoded original control separately from the exact namespace;
  ordinary recovery retains both checks. A separate directory scope holds the
  install guards without granting namespace or startup authority. Code:
  `b1c7e895256`, `56d48cf6806` and `e302374b627`. Validation passed 79 distinct
  selected tests in fresh processes and strict fresh compilation. The move
  position property passed 80 trials at seed 121, digest
  `fnv1a64-v1:38048727ed1c3732`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/inert-directory-checkpoint.md`.

- [x] Install and recover disabled inert target configuration. Hold the original
  base and all ten directory roles while moving receive and control metadata.
  Publish Installing/0 separately, then perform at most one no-replace move per
  call, sync both parents and publish its exact progress. Reopen one-ahead
  layouts without repeating a move. Promote only a complete, exact next
  temporary; preserve partial or conflicting evidence. Inert publication is
  separate, checks the actual pending state and leaves startup fenced.
  Validation covered 111 distinct selected tests across fresh runs, including
  all five installed proof geometries, original-file corruption, guard and path
  replacement, actual advance preflight and hook failures, and strict pending
  witness rejection. Strict cold compilation passed all 217 source files with
  no warnings or errors. Native Lisp TLI 1 checks passed 13 captured images,
  52 fresh target processes and 12 SIGKILL cuts, with the source stopped during
  local construction and recovery. Descendant and retained installation cases
  used local installed fixtures. The inventory and prefix-signature properties
  each passed 80 trials at seeds 122 and 123, with digests
  `fnv1a64-v1:0ff7e43f10c22c03` and `fnv1a64-v1:97580d0206f8561c`;
  input coverage and rejection counts were not measured. Code: `ac52498ee2c`,
  `d8ce64749bb`, `666e6aa3ba6` and `e545abbe9f8`. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/inert-operation-scope-progress.md`.

- [x] Persist and reconcile target source settlement. Publish exact handoff-pending
  intent before remote effects, observe STATUS first on the frozen source route,
  and record only the exact CLOSED parent outcome and successor. Separate pending
  and terminal publication authority; recheck local snapshots after network I/O.
  Recover complete next temporaries offline and preserve partial or conflicting
  evidence. Historical settlement after local abort grants no activation or cleanup
  authority. Configuration remains Inert and disabled, with startup fenced.
  Validation passed 165 distinct selected tests and 3,089 assertions in a fresh
  process, strict compilation of 223 production files, and independent commit
  snapshots. Native TLI 1 acceptance covered 21 cases, 119 fresh target processes,
  11 target SIGKILLs and one source SIGKILL, including lost replies and cold reopen.
  All five installed proof geometries passed with mocked source transport.
  Properties passed 80 trials each at seeds 124 and 125; input coverage and
  rejection counts were not measured. Code: `999d6d4e87e`, `7bdf47c9871`,
  `b1a38bab591` and `1fba3d45b1d`. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/progress.md` and
  `settlement-related-regression-check.json` in that directory.

  Activation, startup fence retirement, cancellation and replacement remain open,
  as do logical drain, control metadata and WAL parity, audits and
  pinned Patroni acceptance. No coordination is provided. During full acceptance,
  audit older harnesses including `t/interop/child-bootstrap.py`: direct control
  calls to physical base backup and manual follow start now fail live admission.
  Run these operations in task-owned workers outside control after it returns.

- [x] Reserve follow-journal room for activation and retirement before new
  pending publication or HANDOFF. Compute a conservative completion bound from
  exact terminal receipts and compatible activation/retirement field widths,
  adding the existing reserve once. Keep size-only templates separate from
  publication authority. Preserve the old exact receipt-size contract, journal
  readability, historical STATUS reconciliation and recovery of a complete
  pending temporary. The shared terminal constructor keeps receipt invariants
  in one place. A fresh regression passed 61 tests and 738 assertions; strengthened
  property coverage passed 100 trials at seed 130, digest
  `fnv1a64-v1:20733aff3238b523`, with input coverage and rejection counts
  unmeasured. Additional focused tests cover all 11 legal completion phase
  shapes from each fixture prefix, printer bindings and the shared-constructor
  refactor. Strict cold compilation passed all 227 production files. Luna
  checkpoint, Sol full review and the focused refactor rereview passed. Source
  docs were reviewed separately.
  Evidence: `capacity-green-check.json`, `capacity-followup-check.json`,
  `capacity-contract-check.json` and `capacity-refactor-cold-check.json` under
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-activation/`.

- [ ] Activate settled target follow and retire startup fences. The pure active
  configuration derivation and optional activation/retirement codecs are
  implemented. They preserve historical predecessors, reject inode aliases and
  advance only exact prefixes. Retirement uses the actual attempt identity order
  (device, inode, mode, uid, nlink, size, mtime, ctime). A producer-backed
  regression covers that boundary. Validation passed 53 selected tests with
  611 assertions, strict cold compilation of 226 production files, and a normal
  formatter check of all nine changed Lisp/ASDF files. Luna checkpoint and Sol
  full-diff reviews passed; the Sol refactoring review found no worthwhile
  extraction. Source documentation was reviewed separately.
  The pure activation overlay now checks the exact captured active image and
  describes the two configuration moves and all three inventory prefixes,
  preserving every historical receive/history/recovery object and unused
  image. A shared position-move helper centralizes unique-source and
  empty-destination checks. Validation passed 19 selected tests with 206
  assertions, strict cold compilation of 228 production files and formatting
  of five Lisp/ASDF files. The property passed 100 trials each at seeds 131
  and 654776489432024191, digest `fnv1a64-v1:ea8ff940de7008c7`; input
  coverage and rejection counts were not measured. Static prefix descriptions
  grant no filesystem or startup authority. Evidence: `plan-contract-check.json`,
  `plan-refactor-regression-check.json`, `plan-refactor-cold-check.json` and
  `plan-refactor-format-check.json` in the activation artifact directory.
  Read-only filesystem proofs now check the active image and exact mapped
  names, identities and bytes at all three move prefixes. Building output
  admits only absence or a complete canonical private image; partial and
  conflicting entries stay fenced. Both layout wrappers share the filesystem
  proof while keeping their separate plans and owner checks. Validation passed 10
  selected tests with 270 assertions, including all five fixture geometries,
  strict cold compilation of 229 production files and five-file formatting.
  The tests construct proposed layouts without publishing activation intent.
  Evidence: `layout-refactor-regression-check.json`,
  `layout-refactor-cold-check.json` and `layout-format-check.json` in the
  activation artifact directory.
  The activation context now binds the actual journal, install and attempt
  snapshots to held directories. It checks the observed move prefix, the
  original base and receive state, and the pending or active control at its
  prescribed location. Publication requires the exact original PREMANIFEST
  attempt. Beginning activation checks future journal capacity and publishes
  Building intent before creating active output. Repeating Building preserves
  the journal inode.
  Recovery promotes the exact next complete Building temporary and
  preserves partial or conflicting state. Installation and activation share
  predecessor acquisition while keeping their own context and admission rules.
  Fresh verification passed 30 selected tests with 641 assertions, strict cold
  compilation of 231 production files and eight-file formatting. This includes
  Building publication across all five geometries, semantic checks at all three
  actual move prefixes, attempt and snapshot changes, frozen-candidate checks,
  journal cuts and preservation of unrelated namespace entries. See
  `context-final-check.json` in the activation artifact directory.
  Local Staged publication now holds and rechecks the active file descriptor
  across file, staged-directory, workspace and ancestor syncs. A scoped witness
  binds the captured entry and statx row through publication. Complete Staged
  temporary recovery checks the proposed entry before syncing, resyncs that
  same file and rechecks the proof before promotion. Partial and conflicting
  files stay fenced. Staged describes durable local output.
  Fresh verification passed 42 selected tests with 878 assertions, strict cold
  compilation of 231 production files and five-file formatting. Coverage
  includes all five geometries, nine output/directory sync cuts, five journal
  cuts, partial and invalid output, replacement and same-byte mutation, abort,
  three injected fsync failures and the earlier regressions. The transition
  property passed 100 trials each at seeds 134 and 135 with definition digest
  `fnv1a64-v1:0d476da5846170de`. Input coverage and rejection counts were not
  measured. See `staging-final-check.json` in the activation artifact directory.
  Fresh selected-source authority, operational activation moves, archive
  retirement and startup admission remain open. Source liveness must be
  checked before activation advances.

- [x] Fence the reserved target follow intent names before startup or older
  reseed ownership. Both final and temporary names reject every entry type;
  specialized cleanup and install roles repeat the check. Validation passed 44
  related tests, strict cold compilation and 16 fresh startup cases. The property
  passed 80 trials at seed 114, digest `fnv1a64-v1:e93f14d6ca661cf2`; input
  coverage and rejection counts were not measured. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/progress.md`.
  Handoff and activation remain open as described above.

- [x] Fence reserved reseed installation state across startup and owner roles.
  Both `physical-reseed.install` and `physical-reseed.install.tmp` refuse startup
  before PID inspection or creation, even without an attempt journal. Ordinary
  stopped reseed ownership refuses them before reading PID bytes and on later
  checks. Clean adoption refuses all four reseed intent names, including after
  PID removal. Its specialized startup owner and both startup entrypoints repeat
  the shared guard, so intent appearing between callbacks cannot pass through
  an inherited-method gap or retire pending adoption journals.
  Eight new Rove tests first failed against the previous behavior, then passed.
  Validation passed 21 selected tests, fresh strict compilation and 16 actual
  startup attempts covering both install names, four entry types and present or
  absent opaque PID data. All original names, bytes, inodes and modes survived.
  The refactor review found no useful macro; the shared guard keeps the policy
  in an early module without a later startup dependency. Code commit:
  `3bfca57247b`. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-install/fence-evidence.json`.
  This reserves and enforces fences only. The install journal format, dedicated
  owner, quarantine moves, crash recovery and source cleanup after a PID move
  remain open; no install authority or coordination has been added.

- [x] Connect the stopped-target journal to remote capture and cancellation.
  A new start freezes a separate private stage and the accepted source route
  with its effective credential owner before sending CAPTURE once. Reopening
  uses the saved request and credentials for STATUS/ABORT only, including when
  no manifest arrived or the source reports UNKNOWN. Abort intent is durable
  before the RPC; only CLOSED/ABORTED permits the local ABORTED phase. Both
  journal names remain startup fences, and cancellation does not need the stage
  or a matching current source timeline.
  Receipt duplicates the held stage descriptor and keeps descriptors for its
  base, global and pg_wal children through verification. Exclusive nofollow
  creation prevents overwriting existing files; scoped readers and exact
  descriptor inventories reject unsafe leaves and unexpected names. A new
  child must be empty when opened; this does not prove it is the inode created
  by mkdirat. Final stage identity checks reject a renamed root or child.
  Validation passed 1,740 distinct physical tests, 22 focused tests and fresh
  strict compilation. Nine native cases covered normal capture, root and child
  replacement during receipt, pre-RPC and post-capture cuts, lost local ABORT
  acknowledgment, a nonlocal connection exit, source failure before streaming,
  and an unexpected stage entry. Each case preserved the old file bytes,
  inodes and inventory apart from the deliberate journal fence, reacquired
  stopped ownership for STATUS/ABORT, and rejected late CAPTURE after cancellation.
  Source SQL remained intact and writable, with no helper channels, capture
  owners or retained source pins left. The stage property passed 100 trials at
  seed 95, digest `e731cd00462a62f1`; input coverage was not collected.
  Code commits: `f74ee6c4a2b`, `cd9b2e172ff`, `00f929a4584`. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-remote/evidence.json`.
  Preserved-data installation, actual follow handoff and startup reconciliation
  remain open. No coordinator is implemented.

- [x] Add the stopped-target reseed journal foundation.
  Freeze the original lease request, physical source settings and credential
  owner, and local directory/lock/PID evidence in a canonical private record.
  Publication uses exclusive temporary creation, file sync, rename, directory
  sync and exact readback. Reopen only under the same stopped ownership.
  Complete temporary records can finish a valid transition; partial or malformed
  entries remain fenced. Both names refuse startup before PID mutation.
  ABORTED publication requires a caller-supplied typed CLOSED/ABORTED receipt
  for the original request; live observation belongs to the future RPC driver.
  Recovery can finish an already receipt-checked complete temporary publication.
  ABORTED retains the startup fence because the old divergent tree remains.
  Validation passed a fresh 1,718-test physical run before the final receipt
  guard, followed by all 15 new tests with that guard, including 24 SIGKILL cuts
  and 24 fresh-image reopens, and fresh strict compilation. The codec property
  passed 100 trials at seed 94, digest `50e18d5e3bfcd5b3`; input coverage and
  rejection counts were not measured. An earlier interrupted full run is not
  counted as a pass. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-attempt/evidence.json`.
  Connecting this journal to remote capture/status/cancellation, binding the
  receive stage, preserved-data installation and follow handoff remain open.

- [x] Expose remote backup lease capture, status, cancellation and strict clients.
  CAPTURE_BACKUP_NATIVE, BACKUP_LEASE_STATUS and BACKUP_LEASE_ABORT now have
  source dispatch and strict clients. COPY completion follows stage cleanup;
  READY status rechecks the pin, origin and required WAL. Fresh closeout passed
  1,705 physical tests and strict compilation. Twelve focused tests passed;
  final STATUS cancellation assertions and formatting were followed by another
  three-test run. Earlier verification passed 903 unit tests, 201 wire tests
  and 16 native scenario checks. The wire property passed 100 trials at seed 93,
  digest `8aa4973e17643a0b`; input coverage and rejection counts were not measured.
  Terra wrote the source documentation; primary and Sol reviews checked it
  separately from the code review.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/backup-lease-wire/evidence.json`.
  Target journal persistence followed in the checkpoint above. Connecting it
  to the remote commands, follow handoff and preserved-data installation remain
  open; there is no coordination.

- [x] Capture verified native backups under durable lease ownership.
  One runtime owner claims a new request; retries return status without
  recapturing. Recoverable errors and nonlocal exits interrupt unfinished
  attempts. Poisoned-WAL cleanup leaves the durable record for cold startup;
  both paths keep any private retention pin. ABORT saves closing intent first
  and defers pin removal until the capture owner exits. Cleanup keeps its own
  request copy and cannot be called early through the callback's owner token.
  The driver holds reload exclusion through copying, verification, publication
  and owner cleanup. It checks the actual base, manifest, source and pin, then
  publishes the origin and READY under one WAL fence. READY proves source
  capture, not delivery to a receiver. STATUS/ABORT use ordinary admission and
  wait while copying holds quiescence.
  Validation passed 1,694 physical tests, 903 unit tests, 201 wire tests,
  82 selected tests and strict fresh compilation. Ten new tests cover ownership,
  retries, cancellation and cleanup. The ownership-context property passed 100
  trials at seed 92, digest `a137ea3f9125d99e`; input coverage and rejection
  counts were not measured. A real server captured SQL data, rejected recapture
  into the nonempty stage, and canceled after copying and after verification
  without publishing an origin. Three SIGKILL/restarts covered origin-before-
  READY, durable READY before memory publication, and later READY recovery;
  retained pins and checked aborts remained usable, and SQL writes resumed.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/backup-lease-capture/evidence.json`.
  These internal source APIs gained remote capture/STATUS/ABORT commands and
  clients in the checkpoint above. Target attempt persistence, follow handoff
  and preserved-data reseed installation remain open. No coordination is
  implemented.

- [x] Enforce backup lease pin ownership and reconcile leases at startup.
  Indexed names, including closed tombstones, reject ordinary pin creation,
  streaming, feedback and release. Checked retirement requires the exact durable
  closing request; a primary also requires the registered pin generation.
  Startup loads the lease index before source pins. READY requires a matching
  pin and durable origin. Explicit loss, a changed timeline or standby startup
  persists pin loss before removing its retention floor; unexplained missing
  READY pins or WAL remain fatal. Closing cleanup resumes without old WAL.
  Validation passed 1,683 physical tests and one additional fault test run
  separately, 903 unit tests, 201 wire tests, 72 selected tests, strict fresh
  compilation, four pin-creation and four startup-loss nonlocal cuts. Two real
  postmaster SIGKILL/restarts verified interruption and closing reconciliation.
  The ownership property passed 100 trials at seed 91, digest
  `03055e58a9eeb791`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/backup-lease-authority/evidence.json`.
  Internal capture and checked STATUS/ABORT APIs followed in the checkpoint
  above. Remote commands followed later; follow handoff and target reseed
  remain open.

- [x] Add checked source backup request records and private durable publication.
  Request identity is immutable, interrupted capture cannot restart under the
  same identity, and closed records count toward capacity without eviction.
  File publication and cold loading use the WAL publication fence; final bytes
  are synced and read back before the new memory index is visible. Loading
  interrupts unfinished records and preserves source pins.
  Validation passed 52 selected lease, source-pin and export-origin tests,
  including 24 new tests, strict fresh compilation, twelve error/nonlocal cuts,
  twelve real SIGKILL cuts and twelve fresh-image reopen probes. Two properties
  passed 100 trials at seeds 89 and 90, digests `f44aabb064b11e6d` and
  `e5ffed3f68a4677f`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/backup-lease-codec/evidence.json`.
  At this checkpoint the records were internal metadata only. Pin authority,
  READY-to-pin/origin validation and startup wiring were added in the next
  checkpoint above. Internal capture and remote commands followed later.

- [x] Add stopped-target ownership and a pre-PID fence for future full reseed.
  Only ESRCH proves that an existing PID is dead. Preserve its complete bytes
  and identity, reject changes or uncertain liveness, and keep the reseed owner
  out of clean-adoption APIs. Any `physical-reseed.attempt` entry refuses real
  startup before stale PID repair. A common-ancestor helper follows C pg_rewind's
  timeline/begin walk; it establishes history geometry only.
  Validation passed 1,640 physical, 903 unit and 201 wire tests and strict fresh
  compilation. Real crashed-server ownership and fenced startup preserved old
  file contents and inodes; normal startup and cold restart also passed.
  Two properties ran 100 trials at seeds 87 and 88, digests
  `f08e33b11713bba1` and `9f51140381ef0a7e`; input coverage and rejection counts
  were not measured. Code commits: `401d71c1195`, `a532100c533`. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-owner/evidence.json`.
  Source backup leases, preserved-data reseed installation and terminal startup
  reconciliation remain open. No reseed operation or coordination is provided.

- [ ] Retain source WAL across remote reseed capture and follow handoff.
  Save the original request identity before creating its private bootstrap pin;
  bind the actual verified base and manifest, and support STATUS and checked
  ABORT even when the client never received a manifest. Persist cancellation
  before releasing retention so delayed requests cannot recreate a canceled
  attempt. Keep the source pin until a matching ready follow pin and durable
  target follow configuration own retention. Interrupted attempts must not
  silently recapture a different base under the same identity.

- [x] Connect the target journal to one-shot remote capture and cancellation.
  Use the stopped-target owner, not the clean-adoption capability. Save the
  original lease request and the accepted source route with its credential
  owner in a private bounded record at `physical-reseed.attempt`. Reuse the
  checked frozen-source representation; do not resolve a new host on retry.
  Keep PID evidence and dirty data untouched. A missing manifest must still
  permit STATUS and checked ABORT with the original request. Cover durable
  publication cuts, reopen, changed local ownership, changed source route and
  delayed capture after cancellation. This record must precede source capture;
  the existing clean-adoption attempt is created too late and requires clean
  control facts. Installation and retention handoff follow separately.
  Use `:premanifest`, `:aborting` and `:aborted` as target intent phases; do not
  guess source progress from a disconnected client. Open resumed operations
  from the saved frozen credential owner, not merely a supplied connection at
  the same route. UNKNOWN never authorizes recapture under the original ID.
  Save abort intent before the source RPC; wait through CLOSING until the source
  confirms CLOSED/ABORTED. The target's ABORTED record remains a startup fence:
  canceled source retention does not make the old divergent primary safe to
  start writable. Do not add journal retirement in this milestone.
  Before any lease RPC, require exclusive temporary creation, file fsync,
  rename, directory fsync and exact readback under the stopped-target owner.
  Publication errors or nonlocal exits stop RPCs. Reopen the original record
  conservatively; malformed or ambiguous final/temporary state cannot authorize
  a new identity. Extend the pre-PID startup check to reject the reserved
  temporary pathname too. A temporary-only failure precedes remote effects,
  but it still marks an interrupted attempt and must not lift the local fence.
  Expose CAPTURE only through a new-start entrypoint that requires both journal
  names absent. Reopening or recovering any journal permits STATUS/ABORT only,
  even when source status is UNKNOWN. Bind a private empty receive stage outside
  PGDATA by canonical path, device and inode before the first lease RPC; missing
  or damaged stage contents must not prevent remote cancellation. Reconnect from
  the saved frozen credential owner and recheck owner, journal, route and source
  identity before dispatch. Initial capture requires the saved source timeline;
  cancellation after source promotion must still accept the same system ID.

- [x] Prepare an independent incoming image for full reseed.
  Reopen the frozen receive stage through held read-only directory descriptors,
  verify its exact inventory and complete base, and check the original source
  lease is READY before writing the separate empty incoming workspace. Copy
  files into independent inodes, checking length, checksum and stable source
  identity. Compare all six metadata file digests as well as manifest contents,
  then recheck the source image, lease, original journal and incoming image.
  Require ordinary stopped ownership and a common descriptor-verified mount.
  Leave partial incoming files in place and do not abort the source lease on
  failure; saved STATUS and checked ABORT remain available. The returned facts
  are observations, not a durable installation proof. No old data moves or
  startup retirement occur.
  Verification: 1,760 distinct physical tests, 28 focused tests, 40 generated
  copy trials (seed 96), fresh strict compilation and 13 native cases passed.
  The two successful native copies independently checked equal bytes and
  distinct inodes for every one of their 13 files. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-image/evidence.json`.

- [x] Add the opaque old-entry inventory for a full reseed install plan.
  Read only the old root's immediate entries through a paired directory
  descriptor. A single no-follow statx observation binds each entry's mount,
  inode and metadata; require two complete matching scans, including nanosecond
  modification/change times, and recheck the original owner and attempt.
  Preserve hardlinked names and unreadable files/directories without opening
  contents, traversing children or following symlink referents. Keep management
  names out of the move inventory and reject their inode aliases.
  Enforce entry and encoded-name byte limits during readdir, before allocating
  the complete list or inspecting every entry. Invalid UTF-8 filenames remain
  unsupported and fail before any future install-journal publication. Descendant
  mounts are not inspected; later unsupported rename interactions must leave
  startup fenced.
  These are observations for a future plan, with no moves or new ownership role.
  Verification: 1,773 distinct physical tests including 13 new tests, 49 focused
  checks, 100 generated trials (seed 97), and fresh strict compilation passed.
  The shared management-name refactor also passed focused checks and strict
  compilation after the full suite had started. Evidence and next-journal notes:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-inventory/evidence.json`.

- [x] Anchor reseed attempt I/O and recover initial-install source cleanup.
  Hold a checked root descriptor across metadata reads, exclusive temporary
  creation, renameat promotion, parent sync and readback. Root replacement
  regressions reproduced temporary creation and rename into a substitute tree
  before this change. The descriptor paths preserve the owned tree through
  those cuts, and ownership rechecks stop remote commands when the name changes.
  A separate cleanup owner uses the valid final attempt under the held lock,
  original stopped PID evidence, and immutable request/source/local bindings.
  It permits saved STATUS and checked ABORT with an initial install temporary
  present and final install journal absent. Every temporary inode kind stays
  intact, and a missing receive stage permits source cleanup. Forward phases
  may replace the attempt inode while preserving its identity fields.
  Use opaque owner printing because the saved attempt contains credentials.
  Verification passed 1,784 distinct physical tests including 11 new tests,
  81 focused checks, fresh strict compilation and six native libpq cases.
  Native checks covered four install temporary kinds, reopened cancellation
  cuts after write/rename, preserved old bytes/inodes, late capture refusal,
  released source retention and writable SQL. The shared immutable-field-list
  refactor passed focused checks and strict compilation after broader runs began.
  Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-cleanup/evidence.json`.

- [x] Encode the dedicated full-reseed installation plan and progress record.
  Keep the original active attempt, initial attempt-file identity, source manifest,
  guarded workspace identities and old/new inventories in a bounded canonical
  CRC-protected image. Check alias exclusion, consistent old hardlinks, private
  incoming objects and original PID facts. Decode consecutive progress lines and
  report only a prefix of the next line as partial. Cache the terminal step once
  while decoding so long progress histories do not repeatedly traverse the plan.
  Validation passed 1,790 distinct physical tests, 87 focused Rove tests and
  fresh strict compilation. The
  generated property passed 100 trials at seed 98, declaration digest
  `fnv1a64-v1:e15893845e333891`. Input coverage and rejection counts were not
  measured. Luna and Sol reviewed the code. Terra wrote the source comments,
  followed by primary and separate Sol documentation reviews.
  The first full run exposed an existing PID fixture mutating a quoted identity
  list across warm executions. A two-iteration regression reproduced the failure
  after a fresh test load. Allocate the list for each iteration and retain the
  original assertions. Focused checks and repeated warm executions then passed;
  Luna and Sol reviewed this separate fixture fix.
  Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-install-codec/evidence.json`.
  Guarded publication and install ownership are the next milestone. Publish
  them together so a final install journal retains saved STATUS/ABORT access.
  Before creating the root temporary, sync workspace guards, copied files,
  directories and naming parents. Hold both startup lock-file and directory
  locks. Derive the journal identity from its retained exclusive descriptor.
  Persist device/inode identities and compare fresh mount IDs within each
  operation. The initial attempt-file identity excludes aliases at publication;
  checked cancellation may replace that inode later. Actual namespace proof
  must precede progress-tail repair, with full image verification before the
  terminal step. The codec supplies the record format for that implementation.

- [x] Hold guarded incoming and quarantine directories for reseed installation.
  A checked scope retains target and auxiliary descriptors, directory and startup
  file locks, private single-link guards, and exact guard bytes and identities.
  Reopening requires saved guard facts. Fresh descriptor mount IDs must match
  the held target mount. Preparation checks ordinary stopped ownership before
  guard creation and before the callback, then permits common ownership checks
  after the callback so future journal publication can supersede that role.
  The scope revokes its check closure on exit and returns independent copies of
  facts. Guard files remain after errors or nonlocal exits.
  Guarded image inventory admits only the two fixed root guard names under a
  descriptor scope. Default receipt and all child inventories remain strict.
  The future publisher must check guard identities and contents around image
  verification and sync copied image files before creating the root journal.
  All 1,800 distinct physical tests, ten new Rove tests, 95 focused checks and
  fresh strict compilation passed.
  Tests cover both locks, saved reopen, scope/thread revocation, mutable copies,
  three arriving-intent cuts, directory replacement, guard corruption and unsafe
  guard entry kinds. Luna and Sol reviewed the code. Sol found no useful macro
  refactor. Terra wrote the comments, followed by primary and separate Sol review.
  Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-install-publication/auxiliary-evidence.json`.
  Guarded initial journal publication and install ownership remain the next
  coupled milestone, followed by moves and progress recovery.

- [ ] Add explicit reconciliation for a partial cancellation attempt temporary.
  A valid final attempt can identify the original source, but existing recovery
  keeps a partial or malformed `physical-reseed.attempt.tmp` fenced before RPC.
  Design and review a cancellation-only recovery under the root lock, with exact
  private temporary inode checks and parent sync, followed by durable ABORTING
  before ABORT. Complete canonical forward temporaries keep existing recovery.
  A partial temporary without a valid final attempt lacks a trusted source route.
  Cover publication cuts under both initial-install cleanup and future moved-PID
  install ownership, preserving startup fences and the original request.

- [ ] Install a full reseed image while preserving the opaque old data tree.
  Introduce a separate `physical-reseed.install` plan and install owner. Select
  that owner whenever a valid install journal exists, including before the first
  move; do not grant clean-adoption authority or require clean control/WAL facts
  from the divergent target. Reverify the external stage and source READY, clone
  a separate incoming image, and verify its exact inventory before publishing
  the immutable move plan. Bind the original attempt, request, frozen source,
  root/lock identities and all workspace identities.
  Preserve the root lock and attempt fence in place. Move the other original
  top-level entries, including the original PID inode, into a private external
  quarantine on the same mount. Treat dirty contents and symlinks as opaque;
  do not parse or follow them. Use checked no-replace renames and sync both
  parents before recording each completed step. Recovery may accept the recorded
  namespace or exactly one proven planned rename ahead; both/neither locations,
  changed identity or ambiguous progress must fail closed.
  The install owner must check the original PID bytes, inode and stopped-process
  evidence at the location derived from the journal, with absence at the other
  location. An actual rename probe preserved bytes and identity fields 1–7 but
  changed field 8, ctime. After a journal-proven PID move, permit only that saved
  ctime difference; still stabilize the current complete lstat/descriptor/lstat
  identity and retain the attempt's original local facts unchanged. The ordinary
  stopped owner must keep its exact original root-PID check.
  STATUS/checked ABORT must remain usable under this owner even if the
  stage or incoming image is damaged. Advancing installation requires healthy
  image evidence; remote cleanup does not. Keep the attempt/install startup
  fences after installation or cancellation, retain the old tree, and do not
  roll back or permit writable startup automatically. Real follow-successor
  handoff and terminal startup reconciliation follow separately.
  Reuse adoption's no-replace rename, mount/sync and one-step-ahead mechanics,
  not its old-tree snapshot, clean proof, codec or ownership validator. Cover
  every move/publication cut, fresh-image reopening and cleanup after the PID
  moves. Sol xhigh design notes are in the reseed-remote evidence file above.
  Bind immutable attempt fields rather than the attempt file's inode: checked
  cancellation republishes that file. A partial initial install temporary must
  still permit cleanup-only ownership after proving the original attempt and
  root PID; it must never grant capture, preparation, moves or startup authority.
  Preserve old hardlinked names independently rather than rejecting shared
  inodes. A rename can change their shared ctime even through an unmoved alias.
  Use stable membership identities across steps and stabilize complete current
  stat observations within each read. Parent directory timestamps, size and link
  count also change during moves; check identity and the expected namespace.
  Keep the PID's stronger byte and identity rule described above.

- [x] Retire terminal adoption fences during real postmaster startup. A durable
  terminal RETIRE marker permits checked removal of handoff, install and attempt
  in that order, with follow removed last. Nonterminal follow progress remains
  fenced. Retirement leaves the server a standby; old quarantine
  and the source pin remain, and no coordinator, DCS or election is provided.
  Validation passed 1,624 physical, 903 unit, 201 wire and 20 focused tests,
  strict fresh compilation, 30 error cuts, 12 real-postmaster SIGKILL cases,
  16 valid prefix cases, five mutations and two late ownership faults. Two
  properties ran 100 trials at seeds 85 and 86, digests `4e5b9b5a61762595` and
  `c3fb2f17a0456875`; input coverage and rejection counts were not measured.
  Native startup retired the fences; with the helper configured it caught up from
  E=33752368, applied a new source commit (`proof_progress=703001`), and rejected
  a standby write with 25006. It passed a target cold restart; before restart,
  receive and replay matched at `0/02046CCB`. Root and lock inodes and preserved
  workspaces, stage and reference files remained unchanged. The first native
  attempt lacked `pg_config`; the rerun used an explicit local `PG_CONFIG` without
  a production logic change. A helper build needs `pg_config` on PATH or explicit
  `PG_CONFIG`; `PG_LIBPQ_HELPER` can select an installed helper. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/adoption-startup/evidence.json`.
  Code commits: `eab9aa41730`, `bd7b43dabcd`, `09bca60b619`. Divergent rewind
  or explicit reseed, logical/pgoutput drain, checkpoint facts, WAL policy,
  audits and Patroni acceptance remain open.

- [x] Prepare offline follow metadata from a ready adoption handoff. A fixed
  derived plan in a private third workspace uses a durable no-replace rename
  journal, accepting its recorded prefix or one completed move ahead. The
  terminal state is active-fenced; original controls and pending configuration
  are saved, and A/J/H/F, quarantine and the source pin remain in place.
  Validation passed 1,604 physical, 903 unit and 201 wire tests, 16 focused
  tests, 66 move cuts, five prepublication and four authority cuts, four negative
  mutations, seven fresh-process SIGKILL cases and strict cold compilation.
  Native publication and a refused cold startup passed; the startup probe
  preserved protected files. Two properties ran
  100 trials each at seeds 83 and 84, digests `e49d091c22d1d20d` and
  `682b6cb65b8ddf0e`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/adoption-follow/evidence.json`.
  Code commits: `9fe2159fd5b`, `fb2e059d880`, `70eb4942613`. Real postmaster
  startup reconciliation and fence retirement, actual catch-up, and divergent
  rewind or reseed remain open. Coordination is not provided.

- [x] Encode and privately publish bounded adoption attempt records under the
  active offline owner. The CRC-protected record preserves clean control facts,
  one bounded source history and monotone phase/proof state. Publication syncs
  and reads back an owned copy. These are structural checks, not fresh source or
  stage validation or authority to prepare or install. The record contains private
  credentials; hex encoding does not encrypt them.
  Validation passed 1,509 physical, 903 unit and 201 wire tests, ten new Rove
  tests and strict cold compilation. The property passed 100 trials at seed 76,
  digest `fnv1a64-v1:6f8d577637fba897`; it has no Function Spec and rejection
  counts and input coverage were not measured. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/adoption-attempt-record/evidence.json`.
  Full context validation, route construction, network PREPARE/PROVE and ABORT
  retirement remain open.

- [x] Fence startup on any `physical-adoption.attempt` directory entry before
  backup-stage cleanup or authentication completion. Only ENOENT permits startup;
  the check does not parse or follow the entry. A future attempt writer must use
  the same cooperative PGDATA lock. Ten focused PID tests, including three new
  Rove tests, native corrupt-file, dangling-symlink and directory fence cases,
  normal restart, and strict cold compilation passed. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/adoption-attempt-fence/`.
  Full context validation, network retry and installation remain open.

- [x] Stage a verified canonical E/Mnew image from a cooperatively owned, clean
  former primary. The guarded, private target is verified with the ordinary base
  and authentication checks, while historical B/M/H_E remains a separate witness.
  This does not install or start the image or grant writer authority. Failure can
  leave a guarded partial target that must not be retried as an empty directory.
  Validation passed 1,496 physical, 903 unit and 201 wire tests, strict cold
  compilation and eight new Rove tests. The manifest property passed 100 trials
  at seed 75, digest `fnv1a64-v1:8e1df9be1a4e8493`; no Function Spec is registered
  and rejection counts and input coverage were not measured. Native staging,
  PREPARE and retained proof passed with independent H_E/root comparison; the old
  primary's files and inode identities remained unchanged. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/adoption-stage/`.

- [x] Prove retained origin history on the promoted source for an initial READY
  pin. The source hashes the selected root from historical B/M through E while
  holding a root-reader lease, and rechecks the pin and source context before
  replying. The typed result remains a claim until independently compared locally.
  This prerequisite does not install an image or authorize adoption or writes.
  Validation passed 1,488 physical, 903 unit and 201 wire tests and strict cold
  compilation. The query property passed 100 trials at seed 74, digest
  `fnv1a64-v1:f77e314e8f25d6cb`; no Function Spec is registered and rejection
  counts and input coverage were not measured. Native evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/retained-origin-proof/`.

- [x] Add read-only local checks for former-primary adoption. Keep the existing
  PGDATA lock and directory inodes, require exact clean checkpoint/catalog,
  resource and allocator facts, and extend a genuine durable export-origin
  commitment through the clean endpoint using original validated WAL bytes.
  Actual next-XID remains separate from reserved allocation capacity. An empty
  checkpoint active-XID horizon may equal next-XID rather than zero.
  The checkpoint passed 1,476 physical, 903 unit and 201 wire tests and strict
  cold compilation. The raw-byte property ran 100 trials at seed 73; rejection
  counts and input coverage were not measured. Read-only probes matched the
  stopped former primary's commitment to the promoted descendant's selected
  retained root, leaving all 17 and 34 files respectively byte-identical.
  That real source receipt had P=E, so its suffix was empty; fixtures checked
  nonempty suffixes, incomplete records and corruption.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/adoption-preflight/`.
  These helpers grant no writer or recovery-installation authority.
  Durable attempt recording and journaled installation remain open.

- [x] Preserve source-owned history commitments for verified remote exports.
  A durable origin binds the original system, timeline, base and manifest to a
  source-proved prefix. Confirmation saves that commitment before advancing the
  private retention cursor. Origins retain no WAL and are excluded from clones;
  completed receipts survive client cancellation. The bounded registry has private
  list/forget operations. Progress currently requires the original timeline and a
  retained cursor at or before the saved proof endpoint.
  The milestone passed 1,464 physical, 903 unit and 201 wire tests, strict cold
  compilation, native backup fault/capacity checks and six shutdown scenarios.
  An independent multisegment oracle proved recycling, source restart and promotion.
  Offline origin loading left source file contents unchanged. Evidence is
  in `/home/imbaczek/proj/pg-ha-artifacts/export-origins/`.
  This supplies historical evidence; it does not yet implement offline adoption.

- [ ] Implement offline former-primary adoption using genuine source history
  commitments and an independently validated descendant retained root. Require
  stable PGDATA ownership and exact clean endpoint validation before staging
  a canonical recovery image. Journal installation before changing files; every
  interrupted startup must resume the exact installation or refuse writer startup.
  Prepare normal follow at E/Mnew with the empty-prefix seed and independently
  compare the source `PROVE_RETAINED` claim with local evidence. Save the exact
  attempt before PREPARE, and keep existing cleanup debts separate from the new
  base's provenance.
  Then add native divergent rewind or explicit reseed fallback. Do not add
  coordination, elections or DCS management in this backend work.

- [x] Keep ready native physical senders through the primary shutdown checkpoint.
  Frozen stream capabilities drain to the validated flush target under a narrow
  shutdown and cleanup lane, then release all claims before clean state. An empty
  plan keeps the terminal fence path. Disconnect is abandonment, not an ACK;
  clean exit does not prove delivery of the shutdown checkpoint's WAL endpoint
  to a disconnected peer.
  SIGQUIT's zero exit timeout avoids thread-join waiting, not every possible exit
  delay. Six native scenarios, 1,447 physical, 903 unit and 201 wire tests, strict
  cold compilation and the existing shutdown test passed. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/shutdown-streaming/`. Graceful logical and
  pgoutput pre-checkpoint drain remains open relative to C; former-primary
  adoption, rewind, provenance and coordination remain outside this milestone.

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

- [ ] Audit destructive sequence operations on quoted or shared lists, including
  `SORT`, `NREVERSE`, `NCONC` and the final argument to `APPEND`. Installation
  verification once changed a quoted `APPEND` tail, breaking the next clone in
  the same image. A handoff negative test also changed a quoted fixture plist
  reused by later checks. Those cases now use fresh lists; check for other cases
  and exercise repeated operations in the same Lisp image.

- [ ] Audit `wal-atomic-write-bytes` callers for temporary destination aliases.
  Its temporary pathname uses type `tmp`, so a destination already ending in
  `.tmp` can alias the temporary and cleanup can remove the destination. This
  was observed in a new fixture; production callers have not been fully audited.

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
