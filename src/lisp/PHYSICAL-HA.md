# Physical HA and Patroni backend work

Status: durable native intake/bootstrap/recovery, continuous TCP receipt/reconnect,
advancing source retention, explicit/automatic local restartpoints and Linux
PGDATA/PID ownership and a startup/reload configuration subset are
implemented, 2026-10-25. Pinned Patroni direct startup, process discovery,
reload and stop plus a psycopg2 driver smoke test pass. Full physical-slot SQL/control,
promotion, synchronous durability and remaining Patroni interfaces are unfinished.
This is an implementation ledger, not a
claim of physical replication support. Coordination is excluded; tests use
manual control. Behavior oracle: the repository's original C PostgreSQL source.

## Verified starting points

### Timeline-qualified native WAL namespace

Filename construction/parsing now centralizes C `XLogFileName`'s TLI/log/segment
arithmetic (`xlog_internal.h:165–169`) while retaining the native `.wal` suffix.
Legacy 16-hex names always identify TLI 1; qualified names contain 24 uppercase
hex digits. Discovery selects exactly the requested TLI and grammar. SQL WAL
filename reporting shares the same arithmetic. This does not change native WAL
record bytes, insertion timeline, physical stream version or server role.

PGWAL6 adds a CRC-bound `legacy`/`timeline` namespace independently of both timeline
identities. Older controls imply legacy names. Startup chooses the recorded grammar
before scanning and rejects missing control when any `.wal` file exists. Recovery
uses the verified base control namespace for provenance/inventory and checkpoint
reads; the receive spool retains its independent legacy naming. Bootstrap inventory
and prefix-copy limits follow the selected source paths.

The explicit primary-only `wal-migrate-timeline-namespace` control operation owns
reload quiescence, writer and WAL locks. It reconciles the entire qualified TLI-1
inventory, requiring existing aliases to be regular files with identical device
and inode; equal contents or symlinks do not suffice. Unmatched destinations are
rejected before mutation. It flushes WAL, creates hard-link aliases, syncs their
directory, durably publishes the new namespace, installs memory state, then removes
only reverified legacy aliases and syncs again. Errors/nonlocal exits during this
mutation fence WAL before releasing its lock; uncertain control publication must
never permit continued legacy-mode writing. Before normal writes or recycling,
startup reconciles inactive aliases: legacy control rolls back verified qualified
staging links, while qualified control finishes verified legacy cleanup. Both
directions prevalidate the complete plan and preserve active names. Ambiguous or
unpaired aliases refuse startup instead of being adopted or discarded. Explicit
operator retry also finishes verified cleanup after a committed switch. A shared
unlink helper rechecks both inode identities and rejects removal of the active name.

Current evidence: 228 physical tests, 169 interop tests, 1,598 legacy checks and fresh strict compilation
pass. The filename identity property passes 200 trials at seed 42, digest
`fnv1a64-v1:1d287bbeea620499`; no FunctionSpec is registered, and generated coverage
and rejection counts remain unmeasured. A task-owned live primary migrated through
its control socket, committed another row, and survived SIGKILL/restart with both
rows and the qualified namespace intact; the independent checkpoint inspector
passed (`/tmp/pg-wal-namespace-manual.xXzJOW`). Unit tests cover stale destinations,
different inodes with equal bytes, symlinks, retryable same-inode aliases, control
rename followed by directory-sync failure, and nonlocal exits before memory adoption.

`t/wal-namespace.sh` builds three retained segments with 2,200 committed updates,
then kills the actual server after partial linking, link sync, control rename,
durable control before memory adoption, memory adoption and partial cleanup. It
also kills startup during rollback/cleanup and verifies repeated recovery. The
independent inspector compares complete active-segment SHA256 hashes and control
identity/LSNs; it does not independently parse each WAL record. Continued follow
from a legacy base, qualified bootstrap, source/standby restart and inactive-alias
removal before SQL admission pass. Evidence: `/tmp/pg-wal-namespace-saved-source.log`;
the fixture can reuse its stopped task-owned baseline via `WAL_NAMESPACE_SEED`.
These SIGKILL tests are not simulated power-loss tests. C logical replication in
both directions and all three manual logical switchovers pass at
`/tmp/pg-wal-namespace-logical-interop.log` and
`/tmp/pg-wal-namespace-logical-switchover.log`.
Checkpoint legacy-format upgrade, publication crash boundaries and unsupported
child rejection also pass at `/tmp/pg-wal-namespace-checkpoint-upgrade.log`.

The live-image investigation also exposed a directory-enumeration hazard:
`directory :resolve-symlinks nil` omitted a dangling alias in this SBCL. WAL
inventory now uses `opendir`/`readdir`, checks EOF versus errno as C `ReadDirExtended`
does, and closes the handle on every exit. Unit and process tests preserve the
unmatched-dangling-alias rejection rather than weakening that invariant.

Final reviews found no remaining correctness blocker. Refactoring review's shared
verify/unlink helper is implemented; production functions remain under 120 lines.
The service refused new model-specific threads, so existing review threads were
reused without exposed model identity. Insertion TLI above 1 is still rejected.
Lineage-aware parent/child scanning, durable promotion completion and rejoin remain
subsequent requirements; no coordination is introduced.

### Independent WAL control timeline identities

C's insertion timeline and latest checkpoint-record timeline can differ between
end-of-recovery publication and the first child checkpoint. `StartupXLOG` assigns
`InsertTimeLineID`/`PrevTimeLineID` at `xlog.c:6699–6703`;
`CreateEndOfRecoveryRecord` flushes child WAL before advancing `minRecoveryPoint`
and its TLI (`xlog.c:8277–8299`). Inferring current insertion from the checkpoint
copy would lose this distinction.

Native `PGWAL5` now CRC-binds checkpoint end, durable end, committed insertion TLI,
checkpoint-record TLI and the existing optional checkpoint copy. Ordinary flushes
preserve both identities; checkpoint publication changes only the checkpoint
identity. PGWAL2/3/4 remain readable as implicit TLI 1. Primary startup rejects any
unsupported child identity before WAL directory sync, scan, tail truncation or
adoption; standby preparation also rejects it before dirty-control publication.
The codec can represent distinct TLIs, but the operational namespace remains TLI 1.
This native control layout is an implementation choice, not C disk compatibility.
Current checks: 213 physical tests, 169 interop tests, 1,598 legacy checks, fresh strict compilation,
and a 200-trial property at seed 42, declaration digest
`fnv1a64-v1:7b7392006a76e7a6`. The encoder has no registered FunctionSpec;
generated input coverage and rejection counts remain unmeasured. The native
checkpoint script writes a valid-CRC insertion-TLI-2 fixture into a stopped owned
primary, verifies startup refusal and compares exact hashes of table, CLOG, WAL
and global metadata before/after. Legacy upgrade and both checkpoint crash
boundaries still pass (`/tmp/pg-control-timelines-checkpoint-final.log`). Native
follow/restart and pinned Patroni status calls pass at
`/tmp/pg-physical-follow.OeXAOv`. New PGWAL5 writes require upgraded binaries;
unchanged WAL7 framing does not imply an older binary can open the new control.
The focused standby guard test presents real child WAL-control bytes and recovery
control bytes while stubbing only the preceding read-only base check; it verifies
the exact unsupported-timeline error and zero dirty-control publication events.
Design and final source reviews found no correctness blocker or useful macro
extraction. Existing review threads were reused; their model identity was not
exposed. All production functions remain below the required length limit.

The namespace inventory identifies the next integration requirements:

| Area | Current evidence | Required before child WAL can be operational |
| --- | --- | --- |
| Primary segments | `wal-segment-path`/`wal-discover-segments`: 16-hex segment `.wal`, no TLI | Explicit `(TLI, segment)` keys and durable namespace migration; old names always mean TLI 1 |
| Native record transport | WAL7, raw XLogData, TLI-1 negotiation only | Versioned branch identity or validated switch event; preserve existing live WAL7 behavior |
| Source retention | `PGPHYSPIN2` embeds literal TLI 1 | Timeline-bound restart/confirmed positions and prefix proof |
| Base/recovery | Manifest v3 and recovery-control v3 require TLI 1 | Distinguish immutable base lineage, replay position and source current lineage |
| Receive/R-point | Receive-control v3 stores one TLI; segment files omit it | Bind floor/flush/digest and R-point replay to their branch |
| Follow/reporting | Query and identity checks require TLI 1 | Fetch/validate ancestry; report the TLI paired with chosen receive/replay LSN |
| Promotion | History publication only | Pending transition recovery, child prefix initialization, durable EOR and control, then writer admission |

Record ownership must follow verified history intervals at each record start,
stop parent-owned records at the fork and never select the highest-numbered file. C copies
only the valid parent prefix for an in-segment switch; at a segment boundary it
creates the next child segment (`XLogInitNewTimeline`, `xlog.c:5867–5937`). A child
file or history file alone grants no writer authority. Pending child bytes must
not be published through ordinary parent `wal-save-durable-lsn` updates. Future
promotion needs recoverable pending/committed state and its own crash tests; no
coordination, custom writer generations or automatic failover is introduced.

File selection is a separate operation from record ownership. C's
`XLogFileReadAnyTLI` (`xlogrecovery.c:4300–4394`) tries entries from the selected
history newest first, excludes timelines whose starting segment is later than
the requested segment, and never goes below the previously opened file's TLI.
It deliberately does not filter an ancestor by its ending segment. A child file
can therefore supply the copied parent prefix; requiring the parent-named file
would reject valid C recovery layouts. The internal `timeline-segment-candidates`
helper now reproduces this filename search order, including partial known roots
and equal forks. Its additional input guard requires the previous file TLI to
be zero (initial/random access) or present in the selected history.

This helper confers no recovery or writer authority. The new internal
`wal-scan-lineage` reader now uses it for explicitly bounded, read-only inspection
of an immutable/quiesced segment set. It validates record ownership at the
record's start (`xlogrecovery.c:3864–3875`), rejects records crossing a fork,
preserves the successful filename TLI as its monotonic floor, and validates native
segment rotation gaps. It never invokes the primary scanner's tail
truncation on ancestor files. Native WAL7 record headers still carry neither TLI
nor system identity: filenames, history and a valid record CRC alone do not prove
that a copied prefix belongs to this cluster. Durable branch-bound provenance
and transport/control integration remain prerequisites for operational child WAL.
The candidate prerequisite passes all 230 physical tests after a fresh ASDF
rebuild, both focused cases after formatting, and fresh strict compilation
(`/tmp/pg-lineage-candidates-compile.log`). A reused review thread found no
correctness blocker; its model profile was not exposed by the service.

The bounded reader clips a parent file at its history fork, ignoring any
divergent physical tail. If child WAL starts in the following segment, its first
record must be too large for the remaining capacity; a fork strictly inside that
gap is rejected. Child-named files may contain copied ancestor records. Missing
files permit older history candidates, but corrupt, symlink or special-file
candidates fail without fallback. Opens use no-follow/nonblocking flags and
regular-file descriptor checks; all streams close on callback errors/nonlocal
exits. The callback receives record owner and filename TLI separately. Results
include the actual cursor, used file identities and whether the requested end
was reached, including early callback stops.

The caller must supply a previously verified starting cursor and independent
cluster/history/prefix provenance. A zero-length range reads no bytes and proves
no boundary; a valid embedded header alone cannot authenticate an arbitrary
starting offset. This reader is not wired into primary redo, standby replay,
streaming or promotion yet. Those paths still enforce the operational TLI-1
restriction. Integrating authenticated branch state and recoverable child
publication remains the next operational milestone.

Reader validation: 238 physical tests, 169 interop tests, 1,598 legacy checks
(`/tmp/pg-wal-lineage-final-unit.log`) and fresh strict compilation
(`/tmp/pg-wal-lineage-final-compile.log`). The generated ownership
property ran 100 trials at seed 42, declaration digest
`fnv1a64-v1:6c5dcc1e0dfb0a73`; no FunctionSpec is registered, and rejection counts
and generated input coverage are unmeasured. Fixed cases exercise copied prefixes
without parent files, CRC failure without fallback, unchanged files after read
errors, divergent parent tails, exact boundaries/equal forks, rotation gaps,
backward file selection and symlink/FIFO refusal. Reused-thread design,
full-diff and refactoring reviews found no remaining blocker or useful macro
extraction; the service did not expose the requested model profiles.

### Preparing child WAL prefixes

Restartpoints now preserve the authenticated final-segment prefix before
receive-floor advancement. The old receive checksum starts at its retained floor;
it cannot authenticate earlier bytes still present in that segment, and a spool
whose original base starts mid-segment has no base bytes in that initial hole.
Promotion cannot safely copy that raw file and declare its prefix verified.

New GEN2 generations with metadata v4 embed at most one native segment prefix.
Its exact length is `LSN mod segment-size`; an additional prefix digest, generation
CRC and pointer-bound whole-generation hash cover the bytes. Capture starts with
the immutable manifest-bound base prefix or a previously verified generation
anchor, then appends only validated raw-record callbacks through the applied
cursor. Coverage must be contiguous. Bytes below the receive floor are never
read from the spool as proof, and original bootstrap WAL remains unchanged.
The generation and pointer become durable before runtime anchor adoption, receive
floor publication and spool deletion. Startup validates the selected embedded
prefix before restoring catalog/storage, including the pointer-ahead-of-floor
crash window. Fresh plist keys are installed through the global recovery state,
including the restored restartpoint LSN.

GEN1/v1–v3 remain readable. Legacy images with no authenticated pre-floor anchor
can continue recovery and restartpoint publication with an explicitly unavailable
prefix (`NIL`), rather than trusting leftover bytes. They regain a known prefix
at an exact segment boundary or in a fully validated later segment; a known empty
byte vector is distinct from unavailable proof. Promotion must refuse/reseed if
required proof is still unavailable. Metadata v4 retains checkpoint/source-XID
reservation checks and the exhausted FullXID representation. New GEN2 writes
require upgraded binaries. This is a native storage format, not C disk/WAL
compatibility and not an operational promotion transition.

Prefix-retention validation: 250 physical tests, 169 interop tests, 1,598 legacy
checks and fresh strict compilation pass (`/tmp/pg-restartpoint-prefix-final-unit.log`,
`/tmp/pg-restartpoint-prefix-final-compile.log`). The embedded-prefix property ran
100 trials at seed 42, digest `fnv1a64-v1:fcee0f270a2059d7`; no FunctionSpec is
registered, and generated input coverage/rejection counts remain unmeasured.
Fixed tests cover repeated same-segment compaction despite corrupted pre-floor
spool bytes, pointer-ahead-of-floor recovery followed by another compaction,
valid-envelope corruption rejected before catalog restoration, legacy unavailable
proof, later-segment recovery and known-empty/unknown/old-format distinction.
Native automatic restartpoints survive injected generation-publication failure,
SIGKILL and fresh-process recovery; the independent Python inspector checks
pointer/generation/prefix checksums and compares the complete embedded prefix with
source WAL (`/tmp/pg-restartpoint-prefix-auto-final.log`, artifact
`/tmp/pg-restartpoint-auto.FbFj4t`). This is process-crash evidence, not simulated
power loss. Continuous follow/reconnect/restart and pinned Patroni 4.1.5 status
calls pass at `/tmp/pg-physical-follow.OIqgFA`
(`/tmp/pg-restartpoint-prefix-follow.log`). Astra high design advice, Luna max
checkpoint and Sol high full-diff reviews completed; the latter's fresh-startup
plist-key finding is fixed and asserted. A reused refactoring thread found no
useful extraction; exact Sol medium creation was refused by the service thread
limit, and the reused thread's model profile was not exposed.

C's `XLogInitNewTimeline` (`xlog.c:5867–5937`) copies only the valid parent prefix
for an in-segment fork; at an exact boundary it initializes the following child
segment without copying the previous segment. `XLogFileCopy` syncs its temporary
file before installation (`xlog.c:3535–3652`). Native
`wal-stage-timeline-prefix` now implements this preparation step with a unique
temporary inode: copy exactly the fork offset from an explicit regular,
no-follow parent source, or prepare an empty file at a segment boundary; sync the
file and its directory; return the temporary pathname to its owner. It supports
explicit legacy TLI-1 or qualified source filenames. Native segments have variable
length, so their prepared EOF is the fork offset; C's zero-filled remainder is a
fixed-size segment detail, not appended native WAL.

The caller must pin immutable source files and independently verify the fork's
durable applied-terminal and cluster/history provenance. The bounded reader
does not supply these proofs. In particular, an applied standby suffix may be
in its receive spool rather than `pg_wal`: promotion must materialize/pin that
prefix before calling this helper. No child filename, history, control or runtime
timeline is published. Ordinary errors and nonlocal exits clean up this call's
temporary file. SIGKILL may leave an unclaimed `.timeline-*` temporary, which
ordinary WAL discovery ignores; no startup path adopts it. Successful callers
own the returned temporary and must install or remove it.

Canonical installation now follows the private verified intent as described below.
C's in-segment `InstallXLogFileSegment(find_free=false)` explicitly
durably unlinks an existing destination and durably renames its prepared file
(`xlog.c:3698–3724`). At a boundary, `XLogFileInit` may reuse a preallocated file
(`xlog.c:3325–3339`); native variable-length reuse must distinguish an empty owned
pending child from uncommitted record bytes. Both branches require recovery to
persist and recheck ownership of the pending child. An existing
child history/filename or matching prefix is insufficient evidence. Parent WAL
control and write exclusion must remain authoritative through preparation and
installation; only later EOR flush and durable recovery/control transition can
admit child writes. This is local crash recovery state, not coordination.

Operational recovery still uses phases 0/1/2 and TLI 1. The new private v4
recovery-control format can represent phase 3: parent recovery remains in
`DB_IN_ARCHIVE_RECOVERY`, while child TLI, original source filename namespace
and the exact restartpoint generation hash are CRC-bound to `applied` as the
chosen fork. The 88-byte layout retains the existing source system identity,
checkpoint/base provenance, actual next-XID visibility and canonical exhausted
FullXID flag. Ordinary writes remain byte-compatible 72-byte v3 controls;
v1/v2/v3 reads remain supported. A generation hash identifies an immutable local
restartpoint image, not a writer-generation coordination token.

The private verified intent publisher described below now writes this format.
The ordinary progress writer rejects pending phase/state/child markers, globally
pending state and failed recovery before its monotonic-max or atomic-write path,
so stale state cannot silently rewrite the intent as ordinary recovery.
Runtime SQL/worker admission rejects phase 3, and startup refuses it before base
manifest processing, recovery-control rewriting or opening storage. No child
WAL/history artifact is adopted and no writer authority is granted. Dedicated
restart reconciliation remains missing; the format is not exposed as a promotion
command. Publication verifies the exact GEN2/v4 pointer and prefix, source
checkpoint and complete allocation reservation, binds the source namespace
(legacy is allowed), and installs pending memory under quiescence before releasing
exclusion. It preserves immutable base WAL and control. A final target namespace switch
also needs durable parent-file inventory reconciliation before primary admission.

Pending-format validation: 258 physical tests, 169 interop tests, 1,598 legacy
checks (`/tmp/pg-pending-control-final-unit.log`) and fresh strict compilation
(`/tmp/pg-pending-control-final-compile.log`) pass. A 200-trial seed-42 identity
property has digest `fnv1a64-v1:45f9cb42bbf4c06c`; no FunctionSpec is registered,
and generated coverage/rejection counts remain unmeasured. Fixed cases cover
valid-CRC invalid schema, every new identity field's CRC binding, canonical
exhaustion, ordinary-writer refusal and unchanged startup storage.
The native automatic-restartpoint/crash test passes with an independent
CRC-valid v4 fixture prepared only in a stopped PGDATA clone
(`/tmp/pg-pending-control-native.log`, `/tmp/pg-restartpoint-auto.VWaiXp`).
After review added the omitted root `pg_xact` to the critical snapshot, a fresh
clone again refused startup for the pending-state reason and preserved exact
SHA256 hashes of all 15 selected storage files
(`/tmp/pg-pending-control-final.n5Bqqp`). PID/option lifecycle files are outside
that storage snapshot. The fixture is not a live intent publisher, and this test
does not claim resume or EOR crash safety. Sol high full-diff review's snapshot
finding is fixed; reused design/checkpoint/refactoring threads found no remaining
blocker or useful extraction. The service refused new Sol xhigh, Luna max and
Sol medium threads; reused thread model profiles were not exposed.

### Verified local promotion intent publication

`physical-promotion-publish-intent` now publishes phase 3 from verified live
standby state. Its private caller owns follow-command exclusion, reload exclusion
and quiescence, in that order. The publisher then takes restartpoint registration
and history locks. Receiver and restartpoint workers must already be stopped;
it never joins workers while holding these locks. Open reader transactions and
Swank admission reject publication. A future public command must orchestrate
worker shutdown and restore recovery after an eligibility rejection.

Child selection follows C `findNewestTimeLine` (`timeline.c:264–290`): probe
consecutive local history files and choose the first absent identifier, with
explicit exhaustion and special-file checks. Archive lookup remains missing.
The selected GEN2/v4 generation must prove the exact applied fork, actual next-XID,
authenticated segment prefix, source checkpoint and complete allocation capacity.
Immutable base control, source namespace, receive identity and cumulative receive
digest must agree. Complete durable backlog must be replayed first; an unfinished
native batch cannot advance the chosen fork. Reading the restartpoint returns
its already-validated generation hash rather than rereading the pointer.

The shared control-byte persister retains existing fsync/rename/directory-sync
ordering and hooks. Successful publication installs pending state in memory
before quiescence ends. Any error or nonlocal exit once publication begins fences
recovery, including uncertainty before memory installation. Ordinary progress
writes cannot erase this fence or overwrite globally pending state with an older
phase-2 object. Base WAL, parent WAL control and runtime insertion timeline stay
unchanged. Native WAL and physical negotiation stay at version 7.

C flushes EOR before advancing minimum-recovery-point control
(`xlog.c:8275–8299`). This milestone does not perform that later transition:
history/EOR publication, pending startup resume,
writer admission and operational promotion remain required. No coordination or
custom writer-generation token is introduced.

Validation passes: 282 physical tests, 169 interop tests, 1,598 legacy checks,
fresh strict compilation and formatting. The intent property runs 50 trials at
seed 42, declaration digest `fnv1a64-v1:62e65d7f2ed89d2c`; no FunctionSpec is
registered, and input coverage/rejection counts remain unmeasured. Reviews of
the full change and final stale-state/failure guards found no remaining blocker;
the reused review threads did not expose their model profiles. The refactoring
review found no useful additional extraction; production functions stay below
120 lines.

`t/promotion-intent.sh` invokes the actual private publisher in an owned native
process, then uses SIGKILL after file fsync, rename, directory fsync and memory
installation. Before rename, restart remains an ordinary readable standby.
After publication, startup refuses pending state and preserves independent
SHA256 snapshots of all 14 selected storage files, including root `pg_xact`.
The Python inspector independently binds recovery CRC/fork/child/namespace to
the current generation and verifies its embedded prefix against source WAL.
Normal publication also denies runtime admission. Evidence:
`/tmp/pg-promotion-intent-native-final.log`, artifact
`/tmp/pg-promotion-intent.bcT7b1`, using the stopped seed
`/tmp/pg-restartpoint-auto.VWaiXp`. The optional seed route was exercised;
the harness's default seed-generation route was not rerun. These are process-crash
tests, not simulated power-loss evidence or completed promotion recovery.

### Installing the pending child prefix

`physical-promotion-install-prefix` now revalidates phase-3 intent and installs
the authenticated generation prefix at the timeline-qualified child filename,
including when its immutable source uses legacy names. It rereads every persisted
intent field, selected generation hash, exact fork/visibility counters, source
checkpoint/reservation and receive provenance. Cached prefix/proof must agree
with that generation. The shared promotion exclusion macro centralizes the
command/reload/quiescence precondition, registration lock, stopped-worker check
and history lock for both publication stages.

The staging helper accepts explicitly supplied authenticated bytes and requires
their exact fork-offset length, avoiding the incomplete parent `pg_wal` suffix.
After temporary-file sync, installation checks the destination is regular or
absent, unlinks it, syncs the directory, renames the temporary and syncs again.
This follows C's force-install ordering. At an exact boundary, native segments
start empty; stale regular destination bytes are replaced rather than trusted as
preallocated WAL. Special files reject before staging. Errors/nonlocal exits
during staging or installation fence recovery; ordinary failure cleanup removes
this call's temporary file.

Child history must still be absent. C `StartupXLOG` installs the segment at
`xlog.c:6670`, publishes history at `6692`, then emits EOR. Future native
orchestration must preserve this durable order. Once history exists, this helper
refuses and preserves any later child/EOR bytes; a distinct resume path must
inspect them. Repeated installation before history is idempotent. It does not
change recovery intent, parent WAL/control, insertion/checkpoint TLIs, namespace
or admission. Startup still refuses phase 3. Operational promotion, orphan staging
cleanup and pending startup reconciliation remain unfinished.

Validation: 288 physical tests, 169 interop tests, 1,598 legacy checks and fresh
strict compilation pass. The new child-prefix
property runs 50 trials at seed 42, digest `fnv1a64-v1:6622f1083b64c304`;
no FunctionSpec is registered, and input coverage/rejection counts are unmeasured.
Tests cover both source namespaces, stale destinations, repeated installation,
changed proof, symlinks, history/EOR preservation, errors and nonlocal exits at
all staging/install boundaries. Empty-prefix segment boundaries are covered by
the lower-level staging test, not the complete native promotion fixture.

`t/promotion-prefix.sh` kills the actual native installer after prefix fsync,
staging directory sync, unlink, unlink directory sync, rename and final directory
sync, plus a normal run. Its independent Python inspector compares installed
bytes with the verified generation/source, distinguishes stale/absent/installed
children, then checks startup refusal and unchanged critical storage snapshots
including `pg_xact` and parent WAL control. Evidence:
`/tmp/pg-promotion-prefix-native-final.log`, artifact
`/tmp/pg-promotion-prefix.OOP8x9`, stopped seed
`/tmp/pg-restartpoint-auto.VWaiXp`, fork 51787. SIGKILL before rename can leave
an inert unique `.timeline-*` file; startup never adopts it. These process-crash
checks do not establish power-loss durability or successful startup resume.

Sol xhigh design advice, Luna max checkpoint and Sol high full-diff reviews found
no remaining correctness blocker. A reused refactoring thread recommended the
shared exclusion macro; the requested Sol medium thread was refused by the
service limit. A reused full-review thread found no issue in the final macro
delta. Production functions remain below 120 lines. Native WAL and
physical negotiation remain version 7; no coordination is introduced.

Replay publishes an applied terminal after syncing
pages/CLOG and its recovery progress, but source allocation reservation and
checkpoint provenance remain separate in-memory facts until a restartpoint
captures them (`physical-replay-publish-terminal`). Promotion must bind these
facts to its selected fork before stopping recovery. C then flushes its EOR
record before advancing `minRecoveryPoint` and `minRecoveryPointTLI`
(`CreateEndOfRecoveryRecord`, `xlog.c:8257–8302`). Neither the parent durable LSN
nor an older checkpoint TLI can stand in for this child boundary. Required next
work is restartable pending ownership/source-prefix materialization, canonical
child installation, versioned EOR and control publication, and only then clearing
recovery exclusion; startup crash tests must cover each boundary.

Preparation validation: 244 physical tests, 169 interop tests, 1,598 legacy
checks (`/tmp/pg-timeline-prefix-final-unit.log`) and fresh strict compilation
(`/tmp/pg-timeline-prefix-final-compile.log`). The generated prefix property ran
100 trials at seed 42, digest `fnv1a64-v1:426990b225941da2`; no FunctionSpec is
registered, and generated input coverage/rejection counts remain unmeasured.
Tests include both source namespaces, exact boundaries without a source file,
multi-buffer copy from a later segment, unchanged parent/control/runtime facts,
short/symlink/FIFO source rejection, and cleanup after three nonlocal exits.
The reused design and full-diff/refactoring review threads found no blocker or
useful further extraction; their model profiles were not exposed. New staging
has no SIGKILL/power-loss test yet and does not claim promotion completion.

### Native end-of-recovery record and switch checks

The current experimental WAL format adds type 13 for end of recovery. Its canonical 24-byte payload
preserves `xl_end_of_recovery`'s signed PostgreSQL timestamp, child TLI, previous
TLI and C `WalLevel` value (0/1/2), with four zero reserved bytes. The pinned C
headers confirm offsets 0/8/12/16 and size 24 on this platform
(`t/interop/eor-layout.c`); a zero-initialized C fixture matches native payload
bytes. C's writer does not promise zero ABI padding, so this is native semantic
layout, not C WAL binary compatibility. C references: `xlog_internal.h:305–311`,
`CreateEndOfRecoveryRecord`, and `xlog.c:9489–9510`.

The research project has no deployed compatibility contract: native record
version 7 and the `pg_lisp_wal_7` negotiation token remain unchanged. EOR extends
that experimental format directly. No protocol upgrade machinery is introduced.

EOR syntax is checked after CRC validation and before receive callbacks, including
zero XID, version/type/length, field bounds and record positions. The encoder is
pure and private; there is no EOR append/publication command. A shared guard blocks
EOR from ordinary append, batch consumers, ordinary WAL scans, receive-store
writes/validation and source export/range proofs. In particular, startup must not
adopt an EOR beyond its previous durable boundary into parent control, or truncate
it as an ordinary unflushed tail. Slot acknowledgment/rebuild and logical readers
cannot silently cross it. The separate read-only lineage scanner can inspect it.

`timeline-check-end-of-recovery` implements C's previous/current TLI agreement,
expected history and minimum-recovery-point rule (`xlogrecovery.c:1913–1963`,
`checkTimeLineSwitch:2368–2406`) plus native record interval coverage. Equal-fork
zero-length ancestors need not be adjacent to the record's previous TLI. It returns
the checked child without changing replay, role, control or writer state. The
application path must later install the replay TLI before EOR redo. C logical
decoding treats EOR as a serialization point; native logical progress must gain
that behavior with verified lineage before operational child timelines are admitted.

A native EOR totals 56 bytes. If it cannot fit at the applied fork, its actual
record starts in the next child segment; the pending intent must retain the
original fork and prefix. Durable child installation, EOR flush, minimum recovery
point/TLI publication and restart reconciliation are still missing. Pending phase
3 startup and child insertion remain blocked; no coordination is introduced.

EOR validation: 273 physical tests, 169 interop tests, 1,598 legacy checks and
fresh strict compilation pass (`/tmp/pg-eor-final-unit.log`,
`/tmp/pg-eor-final-compile.log`). The 200-trial seed-42 field property has digest
`fnv1a64-v1:5281b787ce171e54`; no FunctionSpec is registered, and generated input
coverage/rejection counts remain unmeasured. Reused reviews found and fixed an
ordinary-scan omission that could adopt EOR into parent durable control before
redo rejected it. Startup regressions now preserve valid, malformed and incomplete
EOR bytes without adopting or truncating them; slot acknowledgment/rebuild also
refuse them. Refactoring centralized record sizes and the ordinary-path guard;
all production functions remain below 120 lines. Requested model-specific threads
were refused by the service thread limit, so reviews reused threads with unexposed
model identity.

Native checkpoint upgrade/crash/restartpoint checks pass
(`/tmp/pg-eor-checkpoint-final.log`, `/tmp/pg-checkpoint-snapshot.cE8RIr`).
Continuous physical follow/reconnect/restart and pinned Patroni 4.1.5 queries pass
(`/tmp/pg-eor-follow-final.log`, `/tmp/pg-physical-follow.gwdLGy`); an earlier
parallel run stopped at its startup port-ownership check, and the isolated retry
passed without relaxing that guard. C logical replication passes both directions
(`/tmp/pg-eor-logical-interop-final.log`). All manual logical switchovers pass
(`/tmp/pg-eor-logical-switchover-final.log`): Lisp→Lisp
`/tmp/pg-switchover.Umeidb`, Lisp→C `/tmp/pg-switchover.TyQgtJ`, C→Lisp
`/tmp/pg-switchover.DRYRtx`. The complete native WAL crash-recovery regression also
passes (`/tmp/pg-eor-wal-crash-final.log`). These protect existing behavior and
do not prove operational EOR publication, physical promotion or synchronous
acknowledged-commit-loss semantics.

### Timeline history publication prerequisite

The C promotion sequence selects a new timeline, initializes a writable child WAL
segment, writes history, then flushes an end-of-recovery WAL record and updates
the recovery control boundary before ordinary writes. References:
`xlog.c:6658–6703`, `CreateEndOfRecoveryRecord:8257–8302`, and
`timeline.c:writeTimeLineHistory:304–454`. History publication alone does not
perform any of these role or WAL transitions.

The new internal history encoder copies parent octets verbatim and emits C's
decimal parent, tab, uppercase high/zero-padded low LSN words, tab, reason and
newline, including C's extra newline whenever a parent file existed. Publication
uses an exclusive temporary inode, file sync, no-clobber link installation,
installed-inode sync and directory sync. It never overwrites an existing child.
Later errors preserve a complete installed file and report failure; callers must
not interpret pathname visibility as durable promotion completion.

Native ancestry validation deliberately requires a parent history above timeline
1, rejects backward forks and embedded reason NUL/newline, and retains existing
native line/file bounds. C allows a missing parent file and uses a larger line
buffer. Archive restoration and archive notification remain unimplemented.
An existing empty or partial parent file remains representable, as in C: ancestry
may begin above timeline 1. The encoder does not certify that this lineage belongs
to a particular base image. Promotion/rejoin must prove that relationship from
the node's verified WAL prefix and system identity before adopting a child.

Current evidence: 208 physical tests and fresh strict compilation pass. The new
encoder property runs 200 trials at seed 42, declaration digest
`fnv1a64-v1:d21b5f8e46f5379e`; no FunctionSpec is registered, and rejection counts
and generated input coverage are unmeasured. `t/timeline-publication.sh` passes
SIGKILL before installation, after installation and after directory sync, followed
by actual SQL-server restart and exact history reads through both replication
connection modes. Independent publishers race at the same destination; the loser
cannot alter the winner, including an existing dangling symlink. The pinned Patroni
4.1.5 reader parses the published lineage, and SQL/protocol identity checks confirm
that runtime role/timeline remain unchanged. Task-worker PIDs are verified against
their command, PGDATA environment and owned launcher before SIGKILL. Evidence:
`/tmp/pg-timeline-publication.zli598`. These are process-crash tests, not simulated
power-loss evidence. Crashes may leave unreferenced temporary history files;
subsequent publication creates a new exclusive temporary file.
Design and final source/process reviews found no publication blocker and no
worthwhile macro extraction; production functions remain below 120 lines. The
reused review thread's model identity was not exposed. Existing 169 logical
interop tests also pass. Native source roles and WAL namespaces remain unchanged.

The next promotion requirements remain timeline-keyed WAL/storage/control and
receive progress, recoverable end-of-recovery publication, restoration of source
allocation floors, truthful role transition and manual physical promotion/rejoin
tests. No coordinator or custom writer-generation authority is introduced.

### Durable native recovery control state

Native recovery control now records the C `DBState` distinction between startup
(0), active archive recovery (5) and clean shutdown in recovery (2). These are
separate from the native bootstrap phases 0/1/2; clean state never grants primary
write authority. `recovery.required` and verified provenance remain mandatory.
The 72-byte `PGRSTAT3` format retains existing identity/progress fields, adds
state and a signed 64-bit diagnostic Unix-seconds update time (`pg_time_t`), and
CRC-checks the entire record.
Legacy 60-byte `PGRSTATE` files read as unknown state/time; v2 remains readable.
Both migrate before mutable initialization. All new writes use v3, whose explicit
phase-word flag represents the exhausted one-past FullXID without wrapping to zero.
Earlier binaries cannot read v3.
This is native metadata, not C `pg_control` byte compatibility.

Startup validates control/manifest/base provenance, then atomically persists
active state before catalog restoration, CLOG reopening or redo. Replay also
publishes active state while preserving the maximum previously required applied
boundary and observed XID. After durable receive flush and restartpoint/floor
publication, shutdown records clean state while the final admission gate remains
closed, even for an unchanged point or immutable base. Clean publication refuses
an incompletely reconstructed required boundary. The shared writer returns a
copied state only after successful publication; callers install it explicitly,
including the durable phase. Timestamps never determine authority or safety.

Failure before rename retains the prior record; failure after rename may leave a
valid clean record visible because its data/restartpoint is already durable.
Either way shutdown failure fences recovery and exits nonzero. It must not try
to roll a valid published record back. Startup failure to persist active state
prevents storage initialization; an unchanged prior clean record remains truthful.

`t/physical-shutdown-signals.sh` passes in
`/tmp/pg-physical-shutdown-signals.SVRE4p`. Its independent Python decoder checks
raw magic, layout, CRC, state and applied LSN after smart/fast/immediate shutdown,
restartpoint and clean-control fsync failures, repeated shutdown at the identical
pointer, legacy migration and restart. A startup fsync failpoint leaves hashes of
all base/global/WAL/CLOG files unchanged. Recovery still rejects application
writes after migration and automatically recovers all seven source rows.
`t/physical-shutdown-prepare.sh` also passes in
`/tmp/pg-physical-shutdown.52GUAR`. Native following and actual pinned Patroni
status queries pass in `/tmp/pg-physical-follow.noud9w`.

The pinned C-to-C `t/physical-control-c.sh` oracle passes in
`/tmp/pg-control-state-c.gVUxyF`: `pg_controldata` reports active recovery after
startup and clean recovery after shutdown, twice at the same checkpoint location.
C references are `pg_control.h:97-106`, `pgtime.h:23`, `StartupXLOG`, and `CreateRestartPoint`'s
unchanged/new-point shutdown branches (`xlog.c:8525-8593`, `8665-8696`). This oracle
does not pass C physical WAL to Lisp. Use `C_PG_BINDIR`/`C_PG_REVISION` for the
pinned C installation when running it.

All 160 physical, 169 interop and 1,598 legacy checks pass, with formatting and
fresh strict compilation. The new cl-spec property checks 200 valid codec
round trips at seed 42, declaration digest `fnv1a64-v1:ac618bec08e490b6`.
Its Function Spec is not registered; generated input coverage and rejection
counts are unmeasured, and the property does not prove I/O durability. Separate
Rove tests and process fixtures exercise those failure paths. Checkpoint and
crash-safety review found no remaining blocker; refactoring review's returned
phase postcondition is implemented. Full checkpoint metadata, primary shutdown
checkpoint parity, promotion, timeline changes, rejoin and synchronous durability
remain unfinished. No coordination is introduced.

### FullXID reservation boundary correction

The reservation audit found a native allocator bug independent of WAL transport:
the old exclusive end `nextXid + 1000` could land on low 32 bits 0, 1 or 2.
After a crash, startup loaded that end as the next assignable FullXID. C's
`FullTransactionIdAdvance` (`src/include/access/transam.h:127–138`) skips those
special low IDs, and `GetNewTransactionId` relies on that behavior.

`canonical-next-full-xid` now rounds reservation ends past special low IDs,
preserving the 2^64 one-past exhaustion sentinel. New batches use that canonical
end. Startup also canonicalizes and durably replaces pre-upgrade bad end values
before opening CLOG or admitting SQL. Reaching a new epoch's first normal XID
still requires the existing freeze/wrap completion path; normalizing an end
does not bypass it. Snapshot v1 still cannot encode an exhausted one-past next
XID and fails closed rather than wrapping it.

The new Rove cases exercise actual assignment/save and actual `xact-init` over
all three special low-bit values, high epochs and exhaustion. Two cl-spec
properties pass 200 trials each with seed 42: arbitrary-end idempotence/bounds
(`fnv1a64-v1:8bc7ced5240fd0ef`) and targeted special-end rounding across epochs
(`fnv1a64-v1:1dead604fc4cc1d3`). Input coverage and rejection counts are
unmeasured; there is no FunctionSpec. All 178 physical tests, 169 interop tests
and 1,598 legacy checks pass. Model review of this correction is pending because
the review service currently rejects new runs at its agent thread limit.

### Replayable native XID reservations — implemented, model review pending

C advances recovery's next FullXID past WAL record XIDs (`varsup.c:298–338`,
`xlogrecovery.c:1911`). The native primary additionally preserves a 1,000-ID
reservation file. Logging that reservation preserves a native contract; it is
not C WAL binary compatibility or a C allocation algorithm.

Native WAL v7 adds type 12, an eight-byte inclusive reserved-through value.
The successor must be a canonical normal FullXID or the 2^64 exhaustion sentinel.
A reservation is the sole terminal after an empty, zero-XID `BATCH_START`; page,
catalog or logical changes cannot share its batch. The marker validator is shared
by primary redo and standby history validation. Source allocation flushes this
batch before saving `global/pg_control` and before handing out any XID. The fast
path retains its XID-only lock; capacity exhaustion releases that lock before
entering WAL-to-XID order and rechecking. Ordinary wrap/exhaustion refusals occur
before critical I/O. I/O failure and nonlocal exits fence further WAL allocation
without handing out an ID.

Primary startup takes the maximum of the native control reservation and the
checkpoint copy, normalizes it, then durably incorporates replayed reservation
records before SQL listeners start. Standby redo never writes its immutable base
reservation file or substitutes the source upper bound for reader visibility.
Its separate source frontier records the bound, applied-through LSN and allocation
coverage. V7 checkpoint/reservation anchors establish coverage; older records make
it unknown until another anchor. A regressing source bound or an observed XID
beyond a covered bound is rejected. This coverage says only that the allocation
contract is known through that LSN; it grants no writer authority and makes no
no-acknowledged-commit-loss claim.

Restartpoint metadata v3 persists this frontier before spool compaction. Bootstrap
manifest v3 and restartpoint v3 preserve the exact exhausted next-XID sentinel.
Base visibility comes from the manifest's actual counter, checked against the
source checkpoint, independently of the copied reservation file. Checkpoint redo
advances actual visibility even for transactions without row-change WAL.
Legacy v1/v2 restartpoints remain decodable, but their ambiguous capacity counter
requires rebuilding actual visibility from the complete retained base prefix;
already compacted legacy images require reseeding. Legacy recovery controls keep
their required-applied boundary while migrating visibility from the base actual
counter. Startup reconstructs that boundary before SQL admission. New actual
observed-XID floors remain monotonic across subsequent restarts.

Stored WAL versions 2–6 remain readable, including v6 checkpoint payloads. Native
streaming now negotiates `pg_lisp_wal_7`, derived from the writer version. Upgrade
both live native peers; a v6 peer cannot consume reservation records.

The new `t/xid-reservation.sh` kills the source after reservation WAL flush but
before its XID control-file update, and again after bytes reach disk but before
WAL control advances. It verifies primary startup adoption/floors, then drives
1,005 native reader transactions that roll back without row-change WAL. The
standby learns their reservation while keeping its original visibility counter
and immutable base control file, and retains it through compaction and restart.
It then applies a source checkpoint carrying the actual reader-only progress,
verifies that value independently of capacity, and restarts through another local
restartpoint. Real legacy v1/v2 generations and v2 recovery controls are converted
offline, then startup is killed after dirty v3 control publication: the full
prefix recovers, while compacted legacy history refuses with a reseed requirement.
Evidence: `/tmp/pg-xid-reservation.iLlMhw`. Checkpoint crash/legacy conversion
passes at `/tmp/pg-checkpoint-snapshot.yh7Saa`.

All 201 physical tests, 169 interop tests and 1,598 legacy checks pass; strict
compilation also passes. The reservation codec property passed 200 trials with
seed 42, digest `fnv1a64-v1:c3baa8de42545025`; coverage/rejection gaps remain
unmeasured and no FunctionSpec is registered. Both C logical directions and all
three manual switchovers pass (`/tmp/pg-wal-xid-logical-*.log`). Native shutdown
signals and automatic restartpoints pass. The native follow regression passes
after the base-visibility correction (`/tmp/pg-physical-follow.nSRygj`). Allocation
waiters recheck WAL health under the XID lock, including failures after their
early check; nonlocal exits after capacity publication also fence handoff.
Existing review threads were reused after the service refused new agents with
`agent thread limit reached`. Reviews found and prompted fixes for logical batch
grammar, shared replay staging, actual versus reserved visibility, and exhausted
counter persistence. Final stabilized-source and process-fixture reviews found no
remaining correctness blocker; the existing thread's model identity was not
exposed, so this does not certify the originally requested model profiles.

### Native checkpoint snapshots and durable publication

The internal checkpoint snapshot codec now preserves explicit record start/end,
native redo position, actual next FullXID, separately reserved-through FullXID,
next OID (including an explicit unknown state), signed Unix checkpoint time,
timeline lineage and oldest active FullXID. Its versioned 88-byte payload binds
to the containing version-6-or-newer checkpoint record and validates segment boundaries.
Legacy version-2 through version-5 empty checkpoints retain unknown metadata.
Checkpoint payloads introduced in version 6 have 88-byte payloads and 32-byte headers.
The current native stream negotiation token is `pg_lisp_wal_7`; old live peers are
rejected during negotiation, rather than accepted and sent unreadable WAL.
Stored WAL versions 2–6 remain readable, including version-5 logical records.
Upgrade both native streaming peers before reconnecting them.

As in C's `CheckPoint`/`ControlFileData`, `PGWAL4` keeps a CRC-bound copy alongside
checkpoint-end and durable-end positions. Capture reads the real next FullXID,
inclusive reservation bound and active-XID horizon under the XID lock, with the
existing writer-to-WAL-to-XID lock order. OID allocation is excluded by the writer
lock. Native v1 redo equals the checkpoint record start: native checkpoints
synchronously install every preceding page, unlike C's concurrent checkpoint
algorithm. Earlier redo is rejected until a future format implements that scan
contract. WAL flushes preserve the previously published copy until page, CLOG and
directory syncs finish. Only checkpoint control publication installs the candidate.

Scanning and physical pins use explicit checkpoint starts; pin/base validation
compares the complete record with its control copy. New bootstrap manifest v2
persists that start, while v1 derives it only for legacy empty records. Replay
preserves source checkpoint facts separately from standby read-visibility XIDs.
Restartpoint metadata v2 retains them through receive-spool compaction and restores
them before suffix replay. Legacy restartpoint metadata v1 remains readable with
unknown source facts. The bounded literal reader now reads NIL as CL NIL, fixing
its former interpretation as the keyword `:NIL`. New persistent formats require
the upgraded binary; older binaries cannot read these records.

A captured checkpoint reservation alone does not make promotion safe at a later
LSN. Post-checkpoint reservations now have a durable replayable contract, described
above, with separate allocation coverage and visibility. Promotion still needs its
own timeline/history, recovery completion and durability contracts. No authority
follows from these snapshot fields.
The full 20-column C checkpoint SQL contract remains incomplete.

Validation: 173 physical tests, 169 interop tests, 1,598 legacy checks,
strict fresh compilation and formatting pass. The cl-spec round-trip property
passed 200 trials with seed 42 (digest `fnv1a64-v1:dfb6fd7c9db473c3`);
input coverage and rejection counts are unmeasured, and no FunctionSpec is
registered. `t/checkpoint-snapshot.sh` independently decodes control/WAL CRCs,
upgrades a stopped legacy fixture, kills the primary before data sync and after
control publication, rejects a CRC-valid mismatching control copy, and checks
source facts through standby replay, restartpoint compaction and restart.
Evidence with v6 negotiation: `/tmp/pg-checkpoint-snapshot.yNodwn`; native shutdown signals:
`/tmp/pg-physical-shutdown-signals.ZwccYD`; explicit preparation:
`/tmp/pg-physical-shutdown.RpLJiv`. The ordinary pinned Patroni query and native
follow regression pass (`/tmp/pg-physical-follow.ACIbvj`), as do both C logical
directions and all three manual logical switchovers (`/tmp/pg-checkpoint-logical-*.log`).
The broader WAL crash/corruption regression also passes
(`/tmp/pg-checkpoint-wal-crash.log`), after updating its independent inspectors for
version-6 checkpoints and the existing timestamped commit record format. Automatic
standby restartpoints pass at `/tmp/pg-restartpoint-auto.1a2jz2`.
These are native physical and C logical results, not C physical compatibility.

### Postmaster startup timestamp and checkpoint reporting inventory

`pg_catalog.pg_postmaster_start_time()` now returns a stable timestamptz
(OID 1184, length 8), following `timestamp.c` and `PostmasterMain`'s `PgStartTime`.
The native postmaster samples PostgreSQL-epoch wall-clock microseconds once after
successfully locking its PGDATA, before PID/storage initialization. That datum
is separate from the Linux process-birth seconds used by `postmaster.pid` and
stale-owner validation. Reload preserves it; queries and new connections never
resample the clock. Failed PID creation clears the captured datum, and access
before initialization raises 55000 instead of fabricating a start time. The
catalog's stable volatility classification is retained for aggregate sharing.

The shared wire probe calls pinned Patroni 4.1.5's actual
`Postgresql.postmaster_start_time` method and checks metadata, argument errors,
multiple connections and unchanged values after reload. Pinned C evidence:
`/tmp/pg-c-start-time.T90j9h`; native startup/reload evidence:
`/tmp/pg-patroni-start.SHE7lj`. All 145 physical, 169 interop and 1,598 legacy
checks pass, along with formatting and strict compilation. Luna max's failed
PID-creation cleanup finding is fixed and re-reviewed; Sol high found no further
defect, and the 120-line analyzer remains within the function-length limit.
Servers already running a definition from before this capture was introduced
need a one-time process restart; a reload cannot reconstruct an unrecorded
initialization timestamp. Output uses the existing UTC timestamptz subset.

The standby-leader query remains blocked by the full `pg_control_checkpoint()`
contract, not by startup-time reporting. Pinned C20 has **20** OUT columns in
`pg_proc.dat` (OID 3442); its function reads CRC-checked durable control data, not the
current allocator state. These are required compatibility fields:

| Fields | Native evidence/dependency before SQL exposure |
| --- | --- |
| `checkpoint_lsn`, `redo_lsn`, `redo_wal_file` | Native snapshots now persist record start/end and synchronous redo. Map C's checkpoint record start and redo filename explicitly; do not expose native checkpoint-end or a local restartpoint as C's checkpoint LSN. |
| `timeline_id`, `prev_timeline_id` | Native snapshots persist both as 1. Promotion/history must supply real changing lineage before dynamic reporting. |
| `full_page_writes`, `logical_decoding` | Persist the actual checkpoint policy flags; full native page images and logical events alone are not a C control snapshot. |
| `next_xid`, `next_oid` | Native snapshots now preserve actual source counters, separately from reservation bounds and standby visibility. The API still needs C's `epoch:xid` text and C20 `oid8` type contract. |
| `next_multixact_id`, `next_multi_offset`, `oldest_multi_xid`, `oldest_multi_dbid` | MultiXact lifecycle/control fields are not implemented. |
| `oldest_xid`, `oldest_xid_dbid`, `oldest_active_xid` | Native checkpoints now capture the oldest active FullXID, including C's no-active nextXid sentinel. Durable C-compatible freeze/retention metadata and SQL type mapping remain. |
| `oldest_commit_ts_xid`, `newest_commit_ts_xid` | Implement truthful commit-timestamp tracking/control state; logical commit timestamps are a separate facility. |
| `data_page_checksum_version`, `checkpoint_time` | Signed Unix checkpoint time is now durable. Page-checksum policy/control state and the complete SQL record remain; native WAL CRCs are not C data-page checksums. |

SQL `xid`/`oid8` output contracts and coherent publication of all these fields
remain necessary. No partial all-NULL record or invented constants were added
to make Patroni's standby-leader query pass. The cl-mcp reader fix now permits
the necessary writer/control edits to valid `#.` source files. The
ordinary Patroni cluster query remains verified separately; enabled slots,
synchronous replication and standby-leader variants are not yet complete.

### Automatic smart/fast standby shutdown

Smart and fast process shutdown now invoke native recovery preparation before
successful exit. Last-connection cleanup only claims finalization and stops the
accept loop under the existing accept/accounting lock. It does not publish a
restartpoint or exit while its backend still holds live-operation admission.
After `serve` returns, the main thread performs preparation at depth zero, then
marks shutdown finished, logs completion and exits. The separate signal worker
remains available for SIGQUIT while main-thread preparation waits. Preparation
errors propagate to the launcher's fatal path and exit 1 without a successful
shutdown log. Primary shutdown storage behavior is unchanged.

The accept loop no longer erases an earlier stop request. Startup catches only
the specific recovery-worker shutdown condition when process shutdown has
actually begun; this closes the signal interval between either worker-start
check and launch. Other startup errors and prepared-recovery refusals without
process shutdown remain errors. Changed active `main`/`serve` frames need a
fresh process; ordinary reload cannot replace an already executing frame.

`t/physical-shutdown-signals.sh` passed at
`/tmp/pg-physical-shutdown-signals.RGlJeh`: SIGTERM waits for the last connected
read transaction; SIGINT terminates one; both preserve pause and received-ahead
F>A WAL while checkpointing R=A, join recovery workers and retain follow
configuration. Fresh processes automatically follow and recover subsequent
rows. SIGQUIT remains responsive while a bounded hook blocks main-thread
preparation. A restartpoint publication error exits 1, preserves recoverable
state and configuration, and a fresh process recovers all six source rows.
Hooks wrap the real preparation routine in the actual owned server image.

All 151 physical, 169 interop and 1,598 legacy checks pass. Formatting and fresh
strict compilation pass. The existing primary shutdown script stops at an
earlier admin-function timeout coercion assertion, also reproduced unchanged
at `2153fdf16d1`; its unmodified signal sections pass separately, including
smart/fast/immediate, escalation and fast shutdown during reload quiescence.
Native following and pinned Patroni status regression passes in
`/tmp/pg-physical-follow.XywJjc`. Both real C logical directions pass in
`/tmp/pg-shutdown-logical-interop.log`; all three manual logical switchovers
pass in `/tmp/pg-switchover.TGXcV5`, `.PUDtro` and `.JItVuE`.

C oracle: `PostmasterStateMachine` waits for regular backends and dispatches a
shutdown checkpoint to the checkpointer (`postmaster.c:3083-3117`);
`ShutdownXLOG` requests a shutdown restartpoint in recovery (`xlog.c:7394-7436`).
Native workers remain threads in one process. The durable native counterpart of
`DB_SHUTDOWNED_IN_RECOVERY` is now recorded as described above. This does not
complete C control metadata, primary shutdown checkpoint parity, promotion or
synchronous replication. Existing 15-second
worker joins can fail closed on slow I/O. No coordination is introduced.

### Callable standby shutdown preparation

`physical-recovery-prepare-shutdown` now provides the local finalization primitive
needed by the postmaster. Invoke it as a standalone private control-REPL form;
it owns admission and must not be nested in an admitted expression. It does not
exit the process. Follow/replay workers are stopped and joined without deleting
`physical-follow.control`, then the restartpoint maintenance worker is joined.
All joins precede the reload lock and quiescence gate, because receiver exit
flush needs admission and replay/maintenance can own the reload lock. A running
older maintenance loop exits itself at the replaceable step boundary; no thread
is asynchronously interrupted during publication.

The process-local `:stopping` phase rejects new worker starts but permits existing
exit flush and admitted control work to drain. Final quiescence flushes pending
complete receive records even without a follower, then publishes a restartpoint
at the last applied terminal A. It neither resumes paused replay nor promotes
received-ahead F>A data into the applied checkpoint. `:prepared` is published
before admission reopens; supported receive/flush/replay/checkpoint/pause and
follow mutations are then rejected. Replay, streaming setup and restartpoint
requests recheck after acquiring their gate, closing the queued-call race. Any
failure after preparation begins latches `:failed` and recovery failure until
process restart. The prepared call is idempotent, and an immutable base needs no
new spool or restartpoint. Explicit follow stop still deletes follow configuration;
the shared runtime quiescence helper preserves it for shutdown.

The live fixture `t/physical-shutdown-prepare.sh` passed in
`/tmp/pg-physical-shutdown.mQ8RFo`: immutable recovery, paused replay with F>A,
configuration preservation, mutation rejection, SIGKILL/restart with automatic
following, genuine direct unflushed receipt without a follower, an incomplete
received batch, and publication-failure restart/resume. A self-restoring wrapper
dispatches preparation while automatic maintenance publication is paused, then
the fixture checks completion after releasing it; this is not a deterministic
assertion about the exact mutex-wait schedule. Unit tests additionally simulate
publication between an outer work check and acquisition of quiescence. All 143
physical and 167 interop tests pass. Existing following/Patroni regression passes
in `/tmp/pg-physical-follow.enPsYC`, and receiver-exit durability regression in
`/tmp/pg-physical-exit-flush.TmLzJ2`. All 1,598 legacy checks pass. Formatting and
strict compilation pass.
Luna max and Sol high found no production defect; Sol medium found no useful
refactor beyond the shared runtime-quiescence extraction.

This primitive is now used by automatic signal integration above, but does not
complete C `ShutdownXLOG` equivalence. Durable recovery state is now recorded
even for duplicate restartpoints, as described above. The cl-mcp fork's inert
source readtable now permits structural
edits of the original valid `#.` source without enabling read-time evaluation.
No shell-edit, shadow-definition or exit-hook workaround was used.
C oracle: `ShutdownXLOG` and
`CreateRestartPoint` in `xlog.c`, plus `016_min_consistency.pl`'s offline recovery
point-versus-page-LSN check. No coordination or C physical disk compatibility
is added by this primitive.

### Receiver session exit flushing

Native receiver session cleanup now follows C `WalRcvDie`: flush completed writes
before clearing receiver status and closing the connection, without sending exit
feedback. `physical-store-flush` fsyncs WAL and directory entries, atomically
persists receive control, then publishes the flush cursor. The reload admission
gate protects the flush; it runs outside network deadlines and without holding
the lifecycle mutex. Manual `physical-follow-stop` joins outside admission and
checks recovery/store health before reporting success or clearing the runtime.
After joining, stop also performs a final flush: an already-running stack frame
can retain its pre-reload cleanup code. This is a no-op for a current receiver
and preserves the stop durability contract after live reload.

Only whole CRC-validated native records advance written/flush positions. Decoder
fragments remain unacknowledged, including a fragment after a completed record.
This differs from C's raw physical byte stream, which may flush bytes belonging
to an incomplete WAL record. An idle exit does not invent received progress.

A new exit durability failure poisons the store and latches recovery failure,
logs a terminal receiver error even during an explicit stop, and blocks SQL with
57P03 until restart. Cleanup still clears the monitor/client and closes the
socket; an existing transport error remains the unwinding condition. Already
poisoned input is not flushed again. This is the native fail-closed equivalent
of C's WAL-fsync PANIC, rather than a claim that SBCL uses C's process model.

The new `t/physical-exit-flush.sh` live experiment passed in
`/tmp/pg-physical-exit-flush.X02dae`: private control-REPL hooks pause the actual
receiver after a below-threshold complete write, then exercise source SIGKILL,
explicit stop, target SIGKILL/restart and continuation from the exact durable
cursor. A receive-control-fsync failpoint leaves the prior cursor intact, makes
stop fail visibly, closes SQL, and permits safe restart/resume. The same source
commits four rows; all four are recovered after the tests. A self-restoring
live-REPL helper wrapper emulates an older frame skipping session cleanup, proving
the post-join fallback in the actual standby image. Six Rove tests cover
complete-plus-partial input, idle exit, poisoned input, exit flush failure and
preservation of an unwinding transport condition and stop after an older frame.
All 137 physical and 167
interop tests and 1,598 legacy checks pass. The full native physical-follow and
Patroni status regression passes in `/tmp/pg-physical-follow.yU80m1`.
Formatting and strict compilation pass; requested reviews
found no blocking issue or useful further refactor. Production functions remain
below 120 lines.

This receiver primitive is now used by whole-server smart/fast finalization
above, which stops/joins native workers and publishes an applied restartpoint.
The durable recovery state is now recorded too; remaining HA work includes
promotion, child timelines and synchronous commit waits.

### Receiver statistics and ordinary Patroni cluster status

`pg_stat_get_wal_receiver()` now returns the 15 typed OUT fields from pinned C
`walreceiver.c`/`pg_proc.dat`, including separate real local written/flushed LSNs,
requested receive-start LSN, verified timeline, source WAL-end hints and message
send/receipt/end timestamps. Source-end hints never substitute for local durability.
The receiver thread's actual Linux TID is the PID field; native workers share a
process rather than C's separate receiver process. Metadata is published under
the lifecycle mutex and progress sampled under the receive-store mutex, with
store-before-lifecycle lock order. It is monitoring data, not a commit durability
or authority predicate.

Like C, a ready connecting worker exposes `connecting` with old sender details
cleared. The last source-end LSN survives same-process receiver reconnects,
while startup resets message timestamps, matching C shared-memory lifetime.
Established connections expose `streaming`, stop requests can expose
`stopping`, and cleanup removes the exact runtime-tagged snapshot. No worker,
bootstrap-only recovery and stopped follow return a NULL scalar composite, while
`FROM pg_stat_get_wal_receiver()` still produces one all-NULL row. Retained spool
files do not invent an active receiver. Native retention pins are not C physical
slots, so `slot_name` remains NULL. `conninfo` describes the successful native TCP
connection using escaped libpq syntax and actual startup parameters, including
`replication=database` and `sslmode=disable`; it omits libpq-only default options
that this custom client does not implement.

Scalar composite functions in FROM now materialize once per input row before
OUT-field expansion. This uses a single-record intermediate in the existing
projection pipeline, preserving scalar NULL behavior and existing publication
SRFs. A call-count test verifies one receiver snapshot for all 15 columns.
The three temporal fields use timestamptz OID 1184/length 8, PostgreSQL epoch
microseconds and ISO text output. Current output supports UTC aliases only;
other session time zones fail explicitly with 0A000. General timestamp input,
arithmetic and complete temporal/time-zone behavior are unfinished.

The receiver probe exposed zero send-time placeholders in shared native sender
helpers. XLogData and keepalive frames now use `postgres-current-timestamp`, as
C walsender does. Signed timestamp decoding reuses `pg-client-i64`. The real
`restore_command` SIGHUP setting has its C empty default and normal provenance;
nonempty commands are rejected because archive recovery is not implemented.

Pinned Patroni 4.1.5's ordinary 16-column `cluster_info_query` now runs unchanged
on primary, bootstrap standby, active/paused standby and stopped follow. The
probe uses installed query builders with synchronous mode and slot queries
disabled, without starting an HA loop. C evidence: primary
`/tmp/pg-c-receiver-primary.wChXzU` and active/paused streaming standby
`/tmp/pg-c-receiver-standby.cP7OK0`. Native source/standby acceptance, including a
bounded one-shot live control-REPL connection barrier, passed in
`/tmp/pg-physical-follow.4olH7C`. All 167 interop and 131 physical tests pass;
1,598 legacy checks and
both real Lisp/C logical-replication directions pass after the sender timestamp
fix (`/tmp/pg-receiver-logical.log`). Formatting and strict compilation pass.
All three manual logical switchovers pass: Lisp-to-Lisp
`/tmp/pg-switchover.I1a0jX`, Lisp-to-C `/tmp/pg-switchover.hukijp` and C-to-Lisp
`/tmp/pg-switchover.hVenyW`. These are logical handoffs, not physical promotion.
Sol medium's decoder refactor was applied; Sol high reviewed the final lifecycle
and descriptor changes without a defect. Luna max's retained-end reconnect
finding is fixed and re-reviewed.

Standby-leader control-checkpoint reporting, enabled slot/JSON and synchronous
sender-statistics variants of Patroni's query remain unfinished, as do promotion,
follow/rejoin across child timelines, synchronous commit waits and rewind. This
checkpoint adds no coordination and establishes no C physical WAL/disk compatibility.
The exit-flushing gap identified in this audit is implemented and tested in the
newer receiver session exit checkpoint above.

### Boolean execution and Patroni TL_LSN checkpoint

AND/OR now follow `execExprInterp.c` short circuit: false AND and true OR skip
the right operand; NULL still evaluates it and combines with SQL three-valued
logic. AND/OR/NOT analyze every operand through Boolean coercion first, matching
`coerce_to_boolean` for supported types: unknown literals use Boolean input,
known non-Boolean types fail with 42804, invalid literals fail with 22P02, and
set-valued Boolean inputs are rejected. Runtime skipping does not suppress
analysis errors. A shared native/C wire probe checks all nine input pairs,
Boolean OIDs, unknown-input coercion, type errors and volatile sequence calls
to prove both skipped evaluation and required evaluation after NULL.

The installed pinned Patroni 4.1.5 `Postgresql.TL_LSN` constant now runs unchanged
with both write and flush variants. Its five result types and values are checked
on native primary, bootstrap-only standby, receiving/replaying standby, and
paused standby with receipt ahead of replay, including live reload. Primary
evaluation skips the recovery-only pause function; standby CASE branches skip
primary-only functions. C primary evidence: `/tmp/pg-c-patroni-tl.Tj7slu`;
C streaming standby and paused-standby evidence: `/tmp/pg-c-tl-standby.94mF1E`.
Boolean C wire evidence: `/tmp/pg-c-boolean-final.tQobwJ`. Native direct-start/reload
evidence: `/tmp/pg-patroni-start.J0oHad`; final complete physical follow/crash/
reconnect evidence: `/tmp/pg-physical-follow.Atal5o`. All 160 interop and 131
physical tests pass. Formatting and strict compilation pass. Sol medium found
no useful refactor; changed production functions remain below 120 lines.
All 1,598 legacy checks pass. Luna max found no executor/coercion defect; Sol high's
type-name diagnostic and test-prerequisite findings are fixed and re-reviewed.

This verifies the five-column timeline/LSN fragment, not Patroni's larger
`cluster_info_query`, which still needs receiver statistics, postmaster/control
metadata and additional configuration interfaces. Promotion, synchronous commit
waits, slots and coordination are unchanged and remain outside this checkpoint.

### WAL filename reporting checkpoint

`pg_catalog.pg_walfile_name(pg_lsn)` now follows pinned `xlogfuncs.c` and
`XLogFileName`: strict NULL propagation, unknown-input LSN coercion, text result
OID 25, and recovery rejection with 55000. Exact segment boundaries name the
segment starting there, including zero. The native segment size is 16 MiB and
the only supported primary insertion timeline remains 1; promotion must update
this reporting together with the writer before enabling child-timeline writes.
These computed SQL names do not change native WAL filenames or establish C
physical WAL/disk compatibility.

The same wire probe passes on pinned C (`/tmp/pg-c-wal-filename.kfPk4U`) and the
native server (`/tmp/pg-patroni-start.KJG2Ng`): 106 boundary/random locations,
metadata, NULLs and input errors. Both new Rove tests and all 156 interop tests
pass; the owned native server also passes actual Patroni startup/config/reload/
shutdown smoke checks. Strict compilation passes.
Luna's recovery message/hint finding is fixed and covered by explicit condition
field assertions. Sol high found no other defects; Sol medium recommended no
refactor. Formatting and 1,598 legacy checks pass. The full Patroni timeline/LSN
fragment required lazy Boolean evaluation on a primary; the newer checkpoint
above now verifies that fragment.

### Fixed-bit cast checkpoint

Patroni's `('x' || substr(filename,1,8))::bit(32)::int` expression now uses actual
fixed-bit casts. `bit` reports OID 1560 with variable length and C typmod metadata.
Binary and hexadecimal input, B/X literals, explicit left-prefix truncation/right
zero-padding, integer low-bit truncation/sign-fill, and destination-width signed
decoding follow `varbit.c`. Short bit strings do not sign-extend when converted
to integers. Oversized strings raise 22003 even when their numeric value is zero.
Bit input with a supplied assignment typmod validates length before digits
(22026); expression casts read unbounded input then resize explicitly.

Bare unquoted `::bit` defaults to one bit, while quoted/qualified unmodified casts
retain unspecified length. Declared lengths accept C's 1..83,886,080 range;
no-op casts to unspecified bit also clear result typmods. Raw numeric type
modifiers retain their spelling through integer input, so fractional/exponent
modifiers raise 22P02, and unary-plus/expression modifiers raise 42601. Numeric
tokenization follows C radix/underscore and decimal/exponent spelling rules;
decimal SQL expressions remain unsupported outside type-modifier parsing.
Integer literals beyond signed bigint now resolve as integral numeric values,
preventing them from bypassing the catalog's explicit int4/int8-to-bit cast routes.

Canonical binary strings preserve values through existing comparisons, grouping
and spill. The new signed-cast property passes 200 trials with seed 42. All 154
interop cases and 131 physical cases pass. The unchanged native/C wire fixture
covers 100 synthetic timeline patterns, signed roundtrips, metadata, NULLs and
error precedence; synthetic filenames never promote a server. Pinned C evidence:
`/tmp/pg-c-bit.GHAtRT`. Native final acceptance passed in
`/tmp/pg-patroni-start.h7eQWK`, including bit decoding after live reload. Forced
strict compile, formatting and 1,598 legacy checks pass; final logs use
`/tmp/pg-bit-accepted-`. Luna max's fractional-typmod finding was fixed; Sol high
reviewed the implementation and follow-up without further findings. Sol medium
found no useful LOC-saving refactor; all changed production functions remain
under 120 lines.

Varying-bit types, bitwise operations, bit concatenation and bit table storage
remain unfinished. WAL filename reporting is covered above; the complete Patroni
status query still needs receiver/control reporting. This milestone adds no coordination.

### String status-expression checkpoint

Text `||` and `substr(text,int4[,int4])` are implemented for the remaining string
part of Patroni's TL_LSN expression. Concatenation follows C precedence (below
arithmetic, above comparisons), text/anynonarray casts and SQL NULL propagation.
Array concatenation is explicitly unsupported. Substring counts Unicode
characters, adjusts zero/negative starts, rejects negative lengths with 22011,
and matches C's end-overflow-to-tail behavior. int2 positions widen implicitly;
int8 positions do not narrow implicitly. The shared coercion helper preserves
these overload boundaries.

The boundary probes also fixed raw integer negation to match `gram.y:doNegate`
before type selection, so -2147483648 resolves as int4. SQL integer arithmetic
now checks int2/int4/int8 results (including unary negation) and raises 22003 on
overflow before a string function can consume an invalid value. Remainder uses
truncation toward zero, matching C for negative operands.

The new character-partition property passes 200 trials with seed 42. All 146
interop cases and 131 physical cases pass. The same Python wire fixture passes
pinned C initialized with UTF8 (`/tmp/pg-c-string.YjnmoO`), including 106
substring cases, coercion/NULL/error checks and arithmetic boundaries. Initial
native acceptance passed in `/tmp/pg-patroni-start.2MVxhc`; final acceptance
passed in `/tmp/pg-patroni-start.ZbwX2I`, including string queries after live
reload. Final strict compile, formatting and 1,598 legacy checks pass; logs use
`/tmp/pg-string-accepted-`. Luna max found the integer result-range gap described
above; it was fixed and pinned in native/C probes. Sol high reviewed the initial
and arithmetic follow-up changes without further findings. Sol medium's shared
coercion refactor was applied. No coordination is added by this work.

### SUM intermission

The original `COALESCE(sum(a),0)` acceptance query has been restored. SUM now
uses C's overload result types: int2/int4 to int8 and int8/existing integral
numeric expressions to numeric. Transitions start at SQL NULL, skip NULL inputs,
and preserve NULL for empty/all-NULL input. int2/int4 accumulation follows the
pinned C build's int64 wrap behavior (also checked through direct int2_sum and
int4_sum calls); numeric results retain exact wide integer totals.

SUM uses the existing descriptor-driven sorted aggregate path, including bounded
sort spill, mixed COUNT/SUM/array_agg transitions, cleanup, and input evaluation
before spilling. COUNT-only queries keep their hash fast path. New tests cover
metadata, empty/all-NULL input, wide totals, overload/context errors, grouped
SQL execution under forced spill, repeated SUM in ORDER BY, and distinct volatile
SUM inputs. The new cl-spec total/NULL property passes 200 trials with seed 42.
The shared native/C wire probe includes the restored query; no alternate
aggregate is substituted for it. Decimal/float/interval/money support is not
claimed because those input types are not implemented by the server.

Initial native acceptance passed in `/tmp/pg-patroni-start.kzcFYS`, including
aggregate queries after live server reload. Pinned C extended wire checks pass
in `/tmp/pg-c-coalesce.SANVjm`. Final validation logs use `/tmp/pg-sum-`.
Final native acceptance passed in `/tmp/pg-patroni-start.GxDe1L`; all 140 interop
cases, 131 physical cases, 1,598 legacy checks, forced strict compilation and
formatting pass. Luna max and Sol high reviews found no defect; Sol medium
recommended no further refactor. The SQL-level grouped spill and repeated-SUM
ORDER BY checks cover the review's initial coverage observation.

### Lazy COALESCE checkpoint

SQL `COALESCE` is parsed as an expression, resolves and coerces every input to
its common type, rejects set-returning inputs, and evaluates left to right only
until its first non-NULL value. False and zero are non-NULL. Metadata preserves
matching typmods and the `coalesce` column name, including through casts. CASE
and COALESCE now share the post-coercion typmod check. Integral casts to numeric
follow C's implicit int2/int4/int8 rules; general decimal input remains unfinished.

All 131 physical cases, 135 interop cases, and 1,598 legacy checks pass. The new
cl-spec NULL-pattern property passes 200 trials with seed 42. The shared wire
probe passes native Lisp (`/tmp/pg-patroni-start.ETETve`) and pinned C
(`/tmp/pg-c-coalesce.QkypaH`), covering type/name/typmod metadata, lazy errors,
coercion errors, SRFs, nullable rows and supported aggregate results. Strict
compile/format and actual standby receipt/reload/crash-follow checks pass; final
logs use `/tmp/pg-coalesce-`. The standby check exercises the actual receive-LSN
fallback before receipt and after durable progress. Luna max and Sol high reviews
found no production defect; Sol medium's shared typmod refactor was applied.

### Exact WAL LSN difference checkpoint

`pg_wal_lsn_diff(pg_lsn, pg_lsn)` now returns an exact integral `numeric`
(OID 1700, variable length), matching `xlogfuncs.c` and `pg_lsn_mi` rather than
silently narrowing to signed int64. Unknown SQL literals resolve through the
existing pg_lsn input routine; explicitly typed text and integer arguments are
rejected with 42883. NULL arguments produce a typed SQL NULL. Lisp bignums retain
both signs through the full uint64 input range. Explicit numeric-to-integer
casts check their target range and raise 22003, including numeric-to-bigint;
comparison with integer expressions preserves exact values.

This supplies an integral numeric producer, not general decimal numeric input,
storage, arithmetic, or typmods. Numeric arithmetic is explicitly rejected with
0A000 rather than routed into the existing integer arithmetic path. The parser
does not claim general numeric cast syntax. Future decimal support must preserve
C rounding, scale, special values, and output contracts before expanding this.

Rove checks full unsigned subtraction and cast bounds. A cl-spec property runs
200 cases with seed 42 over uint64 pairs, proving exact reconstruction and
antisymmetry. `t/interop/wal-sql.py` is shared unchanged between Lisp and C: all
49 boundary pairs and 50 seeded random pairs, numeric OID/Decimal decoding,
typed NULLs, valid int64 endpoints, overflow and bad overload/input checks pass.
Native evidence: `/tmp/pg-patroni-start.NOVr1a` and
`/tmp/pg-wal-sql-native.log`; pinned C: `/tmp/pg-c-wal-sql.kuaQ0H`.
The native run also passes the existing real Patroni connection, timeout/gate,
history, process, startup and reload checks. All 127 physical cases pass.
After review, literal conversion reuses `coerce-in-constant`, and diagnostics name
numeric/pg_lsn argument types correctly. Final native acceptance passed in
`/tmp/pg-patroni-start.GtrfoZ` (`/tmp/pg-wal-diff-final-native.log`); strict compile,
formatting, 135 interop cases and 1,598 legacy checks pass. Final logs use the
`/tmp/pg-wal-diff-final-` prefix. Luna max and Sol high found no blocking defect;
Sol medium suggested the applied literal-coercion simplification.

The pinned `pg_walfile_name()` uses `XLByteToSeg`, so an exact segment boundary
names the new segment. Do not import older PostgreSQL's previous-segment rule.
The complete Patroni TL_LSN expression additionally needs actual timeline
filename reporting and receiver/control statistics. These remain unfinished.

### Replay pause control checkpoint

The four `xlogfuncs.c` recovery controls now resolve as native SQL functions:
`pg_wal_replay_pause()` and `pg_wal_replay_resume()` return `void` (OID 2278,
empty non-NULL text output); `pg_is_wal_replay_paused()` returns Boolean and
`pg_get_wal_replay_pause_state()` returns text. All reject a primary with 55000;
failed recovery remains fenced with 57P03.

Native redo holds exclusive operation admission. Pause runs under ordinary
admission, so it acknowledges a complete applied boundary synchronously and
reports `paused` immediately. C instead requests pause asynchronously and can
temporarily report `pause requested`. Native pause can wait for the current
replay pass to finish; the C nonblocking request latency is not implemented.
The pause flag survives code reload and receiver lifecycle changes, but not a
process restart. Startup must still replay the required durable prefix.
Replay checks pause after closing admission and before any storage changes;
the follow worker skips repeated gate closures while paused. Receipt, flush,
feedback, and read-only SQL remain available. This adds no coordination or
promotion authority. Promotion-trigger rejection remains part of the unfinished
promotion implementation.

Rove covers primary rejection, state transitions, idempotence, unchanged replay
progress, void typing, and failed-recovery precedence. `t/physical-follow.sh`
also exercises receipt advancing with apply fixed, SQL-visible data staying at
the paused frontier, pause across live reload, resume, and crash recovery from a
received-but-unapplied prefix. Native acceptance passed in
`/tmp/pg-physical-follow.wYFiWq` (`/tmp/pg-pause-follow-final.log`). All 125
physical Rove cases and 135 interop cases pass; forced strict compilation and
formatting pass (`/tmp/pg-pause-compile.log`, `/tmp/pg-pause-format.log`). Luna max
checkpoint, Sol high full review, and Sol medium refactor review found no defects
or useful refactor; all changed production functions remain under 120 lines.
An explicit barrier test for requesting pause during active redo remains a
coverage gap; admission serialization was inspected but not raced in that test.

The pinned Patroni 4.1.5 `Postgresql.TL_LSN` expression was rechecked: this closes
its replay-pause predicate dependency, but the full query still needs WAL filename
and receiver/control statistics.
Do not infer complete Patroni monitoring compatibility from the four controls.

| Contract | Current evidence | Required acceptance |
| --- | --- | --- |
| Streaming | Explicit native physical START, separate receive/replay workers and durable-prefix reconnect implemented | Scalable physical slots/restartpoints and full C-compatible control contract remain |
| Identity/history | `identify-replication-system` uses durable catalog identity but literal timeline `1` | Shared bootstrap identity, real timeline/history, wrong-cluster and divergent-history rejection |
| WAL storage | `backend/storage/wal.lisp` has custom versioned full-page after-image records, local durable LSN and recovery state | Audit replay atomicity, catalog/CLOG reconstruction, retention, restart and interrupted-record handling before continuous recovery |
| Physical bootstrap | Durable source pins and a source-pausing coherent copy implemented; online copy remains missing | Consistent replica while primary writes; retention pin until copy/stream handoff completes |
| Standby write exclusion | Static/streaming role guards and coarse reader-conflict deferral implemented; C targeted conflict cancellation remains missing | Recovery owns role; all application mutation paths reject writes, including attempts to disable read-only |
| Promotion/rejoin | Not yet audited comprehensively | Durable promotion boundary/history, restart-safe completion, rebuild/rejoin of old primary |
| Sync commit | C `access/transam/xact.c` calls `SyncRepWaitForLSN` after local commit WAL | Remote write/flush/apply modes, candidate selection, wakeup/cancellation and ambiguous-commit tests |

The custom WAL format is not C physical compatibility. Preserve both directions
of logical C interop while staging physical disk/WAL compatibility separately.
Do not add SQL functions returning guessed standby state to make probes pass.

## Patroni call inventory

Reference version: [Patroni v4.1.5](https://github.com/patroni/patroni/tree/v4.1.5).
The process/driver smoke test pins psycopg2-binary 2.9.11 and psutil 7.1.3;
`t/interop/patroni-requirements.txt` records the complete tested Python environment. This inventory
is incomplete until bootstrap, slots, sync, rewind and driver
modules have also been inspected and their calls exercised. Config and process
call sites below have partial native acceptance; the complete interface matrix
remains unverified.

| Source | Observed contract | Implementation status |
| --- | --- | --- |
| `postgresql/__init__.py`, `TL_LSN` | Recovery state, WAL filename timeline, current/flush LSN, receive/replay LSN differences, replay pause state | Inventory started; backend implementations and exact SQL composition require audit |
| `Postgresql.__init__` | `PG_VERSION`, `global/pg_control`, process discovery and current settings | Native pg-lisp metadata/layout and lifecycle mapping require audit |
| `postgresql/connection.py`, `NamedConnection` | Real psycopg connection, integer server version, parameterized queries, error/reconnect behavior | psycopg2 2.9.11 simple parameterized SQL/version smoke passes; actual NamedConnection queries/reconnect suite remains |
| `ConnectionPool.conn_kwargs` | Startup options for statement timeout and extension-qualified settings; application name and connection timeout | Startup-option handling requires audit |
| `config.py:load_current_server_parameters` | Parameterized `pg_settings` query with `current_setting`, setting provenance/sourcefile and `ANY`/`ALL` array comparisons | Exact SQL/parser, catalog and driver array adaptation require audit |
| `config.py:_get_pg_settings`, `_read_recovery_params` | `name, setting, unit, vartype, context, sourcefile`; distinguishes restart-only from reloadable recovery changes | Real GUC registry and recovery config/restart semantics required |
| `postmaster.py:_read_postmaster_pidfile`, `_is_postmaster_process` | Multi-line PID file, PGDATA, start time, port and listener metadata; PID reuse/start-time safety checks | Real v4.1.5 discovery/start-time rejection and signal_stop smoke pass with owned Linux PID file; full startup/config/shutdown lifecycle remains |
| `postmaster.py:start` | Direct executable launch with `-D`, `--config-file=...` and options; process survives launcher | Real pinned direct start passes for the implemented configuration subset; complete generated configurations remain unsupported |


Primary references:
[Postgresql control layer](https://github.com/patroni/patroni/blob/v4.1.5/patroni/postgresql/__init__.py),
[connection layer](https://github.com/patroni/patroni/blob/v4.1.5/patroni/postgresql/connection.py).

## Implementation order and completion evidence

1. Complete C/Patroni inventory and record explicit version pins and deferred
   compatibility. Audit local WAL replay and bootstrap consistency boundaries.
2. Implement persistent recovery state and a continuous physical data path,
   preserving transaction, catalog, page and CLOG invariants across restart.
3. Implement truthful SQL/control role and progress reporting, recovery write
   exclusion, timeline/history and promotion. Exercise manual promotion/reseed.
4. Implement synchronous durability semantics and validate against an independent
   client acknowledgment ledger under crashes and delayed remote flush/apply.
5. Exercise real pinned Patroni control interfaces without DCS, election or
   automatic failover; retain native logical interop and switchover regressions.

Each milestone needs native runtime evidence and the requested reviews. A passing
logical row comparison does not establish physical correctness, and received WAL
does not prove remote durability. Do not claim zero acknowledged-commit loss until synchronous
commit and eligible-candidate semantics are implemented and verified. Rewind and cross-C physical compatibility
must have explicit staging decisions and tests before they are claimed supported.

## Intake checkpoint

`backend/replication/physical.lisp` now assembles native WAL records across
arbitrary byte fragments with bounded header/body allocation and CRC validation.
A failed decoder cannot be reused; reconnect starts a new decoder at the owning
receiver's durable record boundary. Its next-LSN is only an intake cursor; it
does not write WAL, replay batches or send acknowledgments. Segment rotation is
checked for format plausibility, not proven omission-free: native records have
no predecessor link, so continuity also depends on the ordered sender protocol.

`pg-physical-tests` runs deterministic fragmentation, corruption, invalid-header,
position, reconnect and rotation cases, plus a cl-spec fragmentation property
(200 trials, seed 42). This is not evidence of a running physical standby.

### Recovery and bootstrap audit findings

- Startup currently performs `wal-init`, `xact-init`, `wal-recover`, slot loading,
  `catalog-init`, orphan removal and XID-wrap completion. Standby mode must be
  checked before initializers can synthesize identity, write catalog upgrades,
  clean relations or perform local wrap maintenance.
- `wal-replay-finish-batch` applies page images and the validated terminal
  operation; incomplete batches remain staged. Live replay must serialize the
  complete batch with readers and publish replay progress only after applying
  pages, CLOG and catalog state. Startup redo writes the catalog file but does
  not refresh live `*catalog*`; reuse alone is insufficient.
- `global/pg_control` is currently ASCII next-XID reservation, not C control data.
  A copied reservation becomes stale while the source allocates more XIDs.
  Promotion must recover a safe FullXID, including wrap/CLOG-reset semantics;
  do not reinterpret this file as Patroni-compatible binary control metadata.
- Concurrent copying of relfiles, catalog, CLOG and independent control files is
  not a consistent base backup. The initial coherent bootstrap now drains operations through the reload
  quiescence gate and pauses SQL for a checkpoint/copy while retaining source
  WAL. Online copying with continuing source writes remains an acceptance
  requirement, not a completed capability.
- Existing retention slots are logical/plugin-specific. Physical bootstrap needs
  a durable cluster-wide retention point before copying, and must require reseed
  when retention is lost. The startup-oriented `wal-scan` can truncate tails;
  it must not be blindly used on live incoming bytes.

C anchors for implementation: `replication/walreceiver.c` write/flush ordering,
`access/transam/xlogrecovery.c` replay publication,
`access/transam/xlogreader.c` bounded header/CRC validation,
`backup/basebackup.c` backup consistency, and `access/transam/varsup.c` next-XID.

Patroni configuration and process references:
[config.py](https://github.com/patroni/patroni/blob/v4.1.5/patroni/postgresql/config.py),
[postmaster.py](https://github.com/patroni/patroni/blob/v4.1.5/patroni/postgresql/postmaster.py).

Intake review: Luna max's framing-contract and missing segment-boundary cases
were addressed. Sol high found a partial-input uint64 LSN overflow; intake now
rejects an out-of-range input end before copying bytes, with a regression.
Current focused suite has seven Rove cases including the 200-trial property;
135 logical interop Rove cases and 1,598 legacy checks pass. Strict forced
compilation passes with a task-local ASDF cache. These checks cover the decoder
and existing behavior, not physical storage/replay or Patroni runtime integration.

Sol medium completed the post-implementation refactor review: no additional
macro/helper abstraction was justified; invariant checks and reset paths are
already centralized and all new production functions are below 120 lines.

## Durable receive-store checkpoint

`backend/replication/physical-store.lisp` adds a dedicated native WAL receive
spool with a stable local ownership lock and a CRC-protected manifest binding
system identity, timeline, initial record boundary, acknowledged flush LSN and
a checksum of the exact ordered raw-record prefix.
The decoder now optionally delivers exact validated raw record bytes to its
storage callback; supported older versions are preserved verbatim.

Intake, complete-record write and manifest-backed flush positions are separate.
The store fsyncs rotated/current WAL segments and their directory entries before
atomically replacing and syncing its manifest, and publishes flush in memory
only after that succeeds. Any intake/I/O error latches failure and requires
reopen; close does not acknowledge pending writes. A post-rename error may have
persisted a newer manifest, so reopen is authoritative for the resulting prefix.

Reopen validates every acknowledged record and its CRC before discarding any
unacknowledged tail. Missing/corrupt control, missing/corrupt acknowledged WAL,
wrong identity/timeline and concurrent local ownership fail closed. Creation
requires a preexisting empty dedicated directory and syncs its name in the
existing parent; the caller must supply a durable parent directory hierarchy.
The timeline is fixed for this store; history transitions are not implemented.

Tests cover partial input, raw-byte preservation, no flush on write alone,
nonzero bootstrap boundaries, segment rotation, bad prefixes without destructive
truncation, tail removal, duplicate ownership, and errors before/after manifest
publication. Eight task-owned subprocesses are killed with SIGKILL around segment
fsync, manifest fsync, rename and directory fsync, then reopened. These test
process death and local-lock release, not power loss or storage hardware behavior.

The receive spool is not yet wired into startup, transport, live replay or
promotion. It does not itself create a consistent base backup or authorize a
standby to serve queries. Next integration must bind it to retained source WAL,
a coherent base copy and persistent recovery mode before opening SQL listeners.

Sol high identified a restart hole in checking only record CRCs: truncating an
acknowledged whole record near rotation could leave an apparently valid segment
gap. The v2 spool manifest now stores an FNV-1a-64 prefix checksum; reopening
recomputes it before any tail deletion. The concrete omission regression failed
before the fix (including unwanted truncation of the later unacknowledged tail)
and passes afterward. This checksum detects accidental damage, not malicious
rewriting. Sol high re-reviewed the fix and found no remaining blocker.

Current physical suite: 20 Rove cases, including eight SIGKILL boundaries and
the original 200-trial fragmentation property. The local ownership lock is a
single-spool concurrency guard, not distributed coordination or writer fencing.

A real ten-page after-image WAL batch, larger than the 64 KiB reopen read
buffer, also survives fragmented receipt, durable flush and reopen with its
page checksums intact. Final forced compilation, 135 logical interop tests and
1,598 legacy checks pass. Luna max checkpoint, Sol high full review/fix review
and Sol medium final refactor review are complete for the receive store.

## Source-retention checkpoint

`backend/replication/physical-source.lisp` persists named bootstrap WAL holds.
Creation captures the current durable checkpoint under the global writer and WAL
locks, validates its record, and atomically persists a marker before publishing
the in-memory recycling horizon. It retains the explicit checkpoint record start,
so a checkpoint ending exactly on a segment boundary keeps its own record.
Only legacy empty records derive start as end minus the 32-byte header. The
zero-checkpoint case retains from zero.

The marker binds cluster identity, initial timeline 1, checkpoint and restart
coordinates with a CRC. Startup loads and validates all markers after catalog
identity initialization and before cleanup, workers or SQL listeners. Required
segments and the checkpoint record must still exist and validate. Release unlinks
and fsyncs the marker directory before permitting recycling.

These holds share the existing recycler's EQ retention map with snapshot pins;
no second recycling implementation was added. Each physical pin owns its own
registration, and loading/releasing them preserves unrelated snapshot pins.
The native API is a bootstrap hold, not a completed SQL physical replication
slot: acknowledgment advancement, retention-cap invalidation and slot statistics
remain outstanding. Holds currently retain WAL until explicitly released.

Source-retention unit cases cover exact-segment-boundary retention, reload,
release, unrelated snapshot pins, duplicate names, foreign identity and corrupt
checkpoint records. `t/physical-source.sh` exercises real SQL writes, multiple
WAL segments, checkpointing, SIGKILL, startup restoration and release/recycling
through the source server's private control REPL. This establishes WAL retention,
not a coherent base copy or a running physical standby. C anchors are
`replication/slot.c:ReplicationSlotReserveWal`, the existing native checkpoint
recycler, and `backup/basebackup.c:SendBaseBackup` for the additional consistency
work still required during copying.

The native source test passes with full ordered-row comparison across crash and
release, not just counts. It additionally corrupts a durable pin while stopped
and verifies startup refusal. Latest artifacts: `/tmp/pg-physical-source.4oTOnG`.
The physical unit suite now has 24 Rove cases, including a dedicated
zero-checkpoint retention/release boundary case. Luna max checkpoint, Sol high
full review and Sol medium refactor review found no blocking retention issue;
no extra macro extraction was justified. Coherent copy, standby startup/write
exclusion, physical streaming/replay, promotion/history, synchronous durability
and Patroni runtime interfaces remain incomplete.

Final retention checks: strict forced compile, 135 logical interop Rove cases,
1,598 legacy checks, shell syntax and whitespace checks pass.

## Coherent gated base-copy checkpoint

`physical-base-backup` is a native control operation with two literal string
arguments: an existing empty target directory and a new source pin name. It
serializes with reload, drains active SQL/control operations, and temporarily
closes operation admission. Transactions that still hold XIDs and pending XID
wrap cause refusal; a plain idle BEGIN without an XID is harmless. Optional
Swank must be off because its evaluation bypasses this gate. Eligibility is
rechecked after draining, so an already-admitted control request cannot enable
Swank during that interval and evade the check.

While quiescent it checkpoints, creates a durable source pin, and marks the
separate empty target `recovery.required` before writing database files. It
copies only implemented durable cluster state and catalog-referenced relation
forks, preserving exact catalog identity. The last WAL file is capped at the
captured durable end. File contents are copied in bounded chunks and atomically
synced; the complete manifest records identity, checkpoint/end, FullXID state
and per-file lengths/checksums only after the image is durable. Source pins stay
retained through success or post-pin failure until explicitly released after a
future durable receive handoff. Invalidated logical-slot cleanup runs after the
operation gate reopens, avoiding a cleanup-versus-admission deadlock.

Target startup now validates the recovery guard/control/manifest before storage
initialization and serves the verified image as a static read-only standby, as
described below. Remaining integration must replay retained/received WAL in
terminal batches, refresh live catalog state, handle reader conflicts and advance
safe FullXID state before promotion. The source-pausing copy does not satisfy
the final online-copy acceptance gate.

`t/physical-bootstrap.sh` passes with an idle writer transaction refusal, a
concurrent SQL client observed waiting at the closed admission gate, manifest/
file checksums and copied checkpoint validation, and an independent source SQL
versus read-only copied heap/CLOG row comparison. The rolled-back tuple and a
later concurrent insert are excluded. The test refuses writable target startup,
SIGKILLs the source after a copied file is durable, and restores both source
pins on restart while the partial target remains recovery-required. Latest
artifacts: `/tmp/pg-physical-bootstrap.Z8AkfL`. These are process-crash tests,
not hardware power-loss simulations.

Luna max and Sol high reviews found no remaining blocking ordering defect after
the Swank recheck and independent row oracle were added. The focused physical
suite now has 30 Rove cases, including startup-guard, target-path, bounded-copy,
strict-prefix, gate-ownership and eligibility checks.

Final bootstrap checks pass: 30 physical Rove cases, 135 logical interop cases,
strict forced compilation, the native copy/concurrency/SIGKILL script, existing
control-REPL reload tests and existing Swank listener regressions. The final
native script also requires the source crash exit to be SIGKILL (137).
Latest native artifacts: `/tmp/pg-physical-bootstrap.edqhA0`. Sol medium's final
refactor review recommends no extra macros/helpers; all new production functions
remain below 120 lines.

## Verified static standby checkpoint

A complete newly produced image now starts as a static read-only standby. Before
any mutable storage initializer, `physical-recovery.lisp` checks bounded manifest
bytes and CRC, literal syntax with reader evaluation disabled, schema, safe paths,
file sizes/checksums, catalog/system identity, checkpoint/end, XID reservation and
exact inventories of `base/`, `global/` and `pg_wal/`. Runtime root files and query
temp namespaces are outside that durable inventory; abandoned query temp cleanup
still runs. Missing or corrupt durable state is never synthesized during recovery.

The base-copy producer writes a pending `physical-recovery.control` before copying
any database files. First startup persists a static recovery phase bound to the
verified manifest digest, system identity, timeline, applied checkpoint and safe
XID reservation. Marker/manifest loss cannot silently become primary startup while
that durable control remains. SQL users cannot alter recovery role. Filesystem
owners and explicitly enabled arbitrary-Lisp debugging interfaces remain privileged
host actors, rather than SQL session settings or an external fencing mechanism.

The static branch opens CLOG for input and avoids primary WAL initialization,
local recovery-tail promotion, catalog upgrade/creation, orphan deletion and XID
wrap maintenance. The immutable base is revalidated on restart. Actual streaming
replay must persist a new recovery phase before changing those files; it cannot
skip original-base validation merely because one startup previously succeeded.

Recovery write exclusion is independent of backend read-only flags and applies to
SQL command dispatch, XID allocation, catalog saves, sequence functions nested in
SELECT/COPY/EXPLAIN ANALYZE, transaction mode changes and logical replication state
changes. Unknown databases are rejected at connection startup. Table readers use
negative virtual relation-lock identities instead of allocating real XIDs, and
release those identities on transaction/session cleanup. `in_hot_standby` and
`transaction_read_only` report the role. `pg_is_in_recovery` is true;
`pg_last_wal_replay_lsn` reports the verified applied base boundary, while receive
LSN is NULL until a real receiver exists. Primary WAL-position functions reject
recovery. Local primary startup progress records the last applied terminal,
excluding durable records in an incomplete batch.

The manifest producer now canonicalizes strings before readable printing: SBCL
otherwise emits some dynamic BASE-CHAR strings as #A arrays. The decoder retains
its strict non-evaluating literal grammar; it does not enable reader evaluation.
Older experimental #A manifests require a newly generated native copy. The new
cl-spec roundtrip property covers dynamic, Unicode, quoted and #.literal strings
(200 trials, seed 42).

`t/physical-standby.sh` passes real table/sequence/DDL/maintenance/function write
rejection, failed-transaction SQLSTATE precedence, logical replication mutation
rejection, role/progress reporting, private control reload, SIGKILL/restart, full
row/sequence comparison and immutable-base checksums. Separate damaged copies
with lost marker+manifest, missing/corrupt recovery control, missing XID control
or a corrupt page all refuse startup; missing metadata is not recreated.
Latest artifacts: `/tmp/pg-physical-bootstrap.Xk0ntp`.

Current focused physical suite: 39 Rove cases. Continuous WAL transport/replay,
recovery snapshot conflicts, timeline/history promotion/rejoin, synchronous commit
and the complete Patroni runtime interface contract remain unfinished. The current
copy still pauses the source, so online-copy acceptance also remains outstanding.

Static recovery verification: 39 focused Rove cases and 135 existing logical
interop cases pass after formatting. Strict forced compilation passes. Sol high
and Sol medium reviews found no blocker or worthwhile new macro extraction in
this static milestone; the explicit SQL fault model excludes filesystem-owner
rewriting of all provenance and arbitrary privileged debugger evaluation.
Automatic review rejected temporary unpaired guard-removal edits during an
error-precedence cleanup; atomic whole-form replacements preserved the guard
throughout, and the failed-transaction regression passes.

Final static checkpoint evidence: the legacy suite passes 1,598 checks with zero
failures after preserving the original primary-path lexical `get-current-xid`
call in the relation-lock macro. The recovery branch alone selects virtual reader
identities. The final strict compile and native standby reload/crash/damaged-copy
matrix pass; latest native artifacts: `/tmp/pg-physical-bootstrap.sdD0pU`.

## Durable replay-input traversal

`physical-store-map-durable-records` exposes only the manifest-backed flushed
prefix to a future replayer. An exclusive resume cursor must be the receive base
or an actual durable record end, including the frozen flush position. The entire
prefix checksum and cursor membership are validated before any caller callback.
Segment padding is skipped; a prior record end resumes at the next segment's
first record. Merely written records remain invisible. Traversal returns the
captured flush boundary and never publishes applied progress.

The traversal reuses the bounded prefix validator in two passes rather than
retaining all WAL in memory. Callback errors latch the store; prior callbacks
are not rolled back. Callbacks run under the receive mutex and must not reenter
it. Repeated whole-prefix scans and holding this mutex while applying records
are not the final continuous replay architecture: an incremental cursor and an
explicit writer/catalog lock-order design remain necessary for integration.

The audit also found and fixed a reopen edge case: if the first received record
rotates immediately after the base cursor, its preceding receive segment need
not exist. The decoder still verifies that the record could not fit there, and
the ordered-prefix digest still checks omissions. Tests cover restart at this
boundary, rotation-gap cursors, unflushed exclusion, prefix corruption before
callbacks, and callback errors. After formatting and a clean FASL rebuild,
45 physical Rove cases and 135 existing interop Rove cases pass. C references:
`walreceiver.c:XLogWalRcvFlush` publishes flush after fsync;
`xlogrecovery.c:ApplyWalRecord` publishes replay after successful redo.

Next integration must separate bounded receive reads from the reader-conflict /
writer/catalog application barrier. Persist a streaming recovery phase before
the first base-file change; restart must then verify its durable progress and
redo retained complete batches instead of comparing changed files against the
original immutable-base checksums. Apply only complete native terminal batches,
refresh live catalog/cache state, and sync affected relations, catalog and CLOG
before persisting an applied boundary. Incomplete batches and receiver flush
must never advance replay SQL reporting. The source FullXID reservation can
advance without a committed transaction, so received page/commit records alone
do not prove a safe promotion counter. These remain implementation requirements,
not behavior supplied by the traversal API.

Traversal milestone checks: strict forced compilation and whitespace checks pass.
Luna max checkpoint and Sol full review report no blocking issue. Sol medium's
final review recommends retaining the explicit two-pass production code: the
existing store macro centralizes locking/failure, and the prefix validator
centralizes decoding/digest checks. Both production functions are below 120
lines. A possible test-only open/close wrapper is optional and was not introduced
as a prerequisite to the replay integration.

## Live complete-batch replay checkpoint

`physical-replay.lisp` now applies durable native WAL to a running read-only
standby. `physical-recovery-enable-streaming` creates a local receive spool and
persists recovery phase 2 before database mutation. It can reuse an empty valid
spool left by a crash between spool creation and phase publication. This is a
native control API, not a completed network receiver or Patroni follow command.
`physical-recovery-receive` and `physical-recovery-flush` keep receipt separate
from `physical-recovery-replay`; the last operation owns the admission gate.

After draining active operations, replay defers if any backend transaction or
snapshot remains. Admission reopens so those clients can commit/roll back; it
never waits for an idle transaction while holding the closed gate. This is
conservative delay, coarser than C's targeted standby conflict resolution.
Optional Swank must be off during application, as for the gated base copy.

The existing native batch decoder stages partial batches. Replay rereads any
unfinished batch from the preceding applied terminal on the next call. Complete
batches update pages/CLOG/catalog with readers excluded, fsync relation streams,
CLOG and directory entries, persist applied progress, and refresh the live
catalog and page-cache state before admission reopens. Any error or nonlocal
exit during live application latches recovery failure; SQL and replication
queries then reject with 57P03 until process restart. Reload preserves the spool,
role, progress and failure latch.

The base checkpoint remains the permanent redo floor; source checkpoint records
are not local restartpoints. Every phase-2 restart validates marker/control/
manifest provenance, unchanged base WAL/control/version files, required mutable
metadata, and the full durable receive prefix. It then replays all complete
post-base batches before admitting SQL, checks that previous applied progress
was reached, and verifies referenced durable files. Changed base pages/catalog/
CLOG cannot be compared to original image hashes. Retained WAL must not be
recycled until a separate restartpoint implementation is proved sound.

Read snapshot FullXID advances from full record XIDs, v4+ page contributor XIDs,
and CLOG-reset next-XID boundaries. Older page records without contributors are
rejected for streaming redo. This observed counter is not a promotion-safe source
reservation: allocation without emitted WAL remains a promotion requirement.
Standby `IDENTIFY_SYSTEM` now follows C `GetStandbyFlushRecPtr`, exposing the
greater of applied and same-timeline received durable progress. Receive SQL can
advance while replay remains at the preceding terminal.

`t/physical-replay.sh` runs real source/standby SQL servers and private control
REPLs. Test-only helpers transport native frames manually, with no network WAL
or coordination claim. It verifies partial-batch exclusion, successive inserts/
updates/deletes, DDL and sequence state, deferred repeatable-read transactions,
live reload, SIGKILL after page/CLOG redo followed by full-prefix restart, and an
injected Lisp redo error that blocks SQL until restart. Ordered rows are compared
against source SQL. Latest passing artifacts: `/tmp/pg-physical-replay.6icWSh`.
The existing static standby/corrupt-image matrix also passes:
`/tmp/pg-physical-bootstrap.5LCoOb`. Current unit checks: 50 physical Rove cases,
135 logical interop Rove cases, strict compilation, shell syntax and whitespace.
Continuous transport/worker/reconnect, scalable replay reads/restartpoints,
targeted conflict cancellation, promotion/history/rejoin, synchronous replication
and Patroni runtime interfaces remain unfinished. Coordination remains excluded.

Review fixes: Luna found that an empty table had no native file-creation redo;
the new regression first failed during base copy of a never-read empty table.
Primary `create-table*` now durably creates its heap inside the rollback handler.
Native catalog terminals imply creation on the heap's first appearance in replay
history; the history starts at the base inventory and resets for full restart
redo. Established live heaps are never silently recreated by unrelated catalog
changes, and index contents are never synthesized. This adapts C's explicit
`RelationCreateStorage` / `smgr_redo` semantics to the existing native catalog WAL
format rather than claiming C physical compatibility. Empty tables before and
after bootstrap now survive crash/restart.

Sol high found a second integrity hole: redo could create a lost original heap
using only post-base page images and leave untouched base pages absent. Before
redo, phase 2 requires manifest-declared base forks to exist with at least
their original length unless complete validated durable catalog WAL proves
their deletion/replacement. The mutable disk catalog no longer grants that
exemption (see the later history preflight milestone). Native same-ID shrinking
is not implemented; VACUUM FULL replaces relfilenodes and TRUNCATE retains pages. Damaged-clone
tests reject missing/truncated live base heaps before listeners; latest full
native artifacts: `/tmp/pg-physical-replay.TTj2tD`. Sol high re-reviewed the fix
and reports no remaining high-confidence blocker. Arbitrary corruption of
already changed heap/catalog bytes cannot be compared to the original base
hashes; this milestone primarily verifies process crash recovery, not exhaustive
disk-corruption repair.

Sol medium's final refactor review found all new production functions below 120
lines and recommended a future shared macro for the ten replay-state special
bindings in primary `wal-recover` and standby replay. That optional extraction
was deferred: cl-mcp's structural reader rejects the valid `#.` forms preceding
`wal-recover`, including with the standard-readtable override. The valid WAL
source was left intact rather than using a nonstructural Lisp edit. Admission
and failure cleanup remain explicit because setup and application have different
ownership semantics.

Final live milestone evidence: strict compile and the static standby matrix pass
after the reviewed base-fork fix (`/tmp/pg-physical-bootstrap.ORsV3N`). The live
matrix also passes with a 201-row, multiple-page base so post-base page WAL does
not contain every original heap page; missing/truncated base clones reject
before redo (`/tmp/pg-physical-replay.Qu5Cix`). All 1,598 legacy checks pass, as
do all three manual logical switchovers and both native C logical interop crash/
feedback matrices. Pinned C revision remains
`9c0909b34939f2d5a37b82b9d1efc8b08e310252`; reference clients require their matching
`libpq` via `LD_LIBRARY_PATH` in this relocated installation. Initial C readiness
failures were caused by loading system `libpq` (missing `PQfullProtocolVersion`),
and the corrected rerun passes. Switchover artifacts:
`/tmp/pg-switchover.Szdfs2`, `/tmp/pg-switchover.st140b`,
`/tmp/pg-switchover.pb1XUP`; native interop artifacts:
`/tmp/pg-logical-interop.Rn6Rof`, `/tmp/pg-logical-interop.SXAJ4e`.

## Continuous native transport checkpoint

`physical-stream.lisp` and `physical-follow.lisp` connect the existing durable
spool/replayer to a real TCP source. They reuse CopyBoth/XLogData/keepalive/status
envelopes but require explicit native format negotiation; plain C physical START
syntax is refused. This is native Lisp WAL, not C physical WAL or disk support.
The current client uses the existing database-scoped replication frontend;
standard physical startup/slot/control interfaces remain in the Patroni audit.

The native request binds a source bootstrap pin, system identifier, timeline 1,
base boundary, requested record end and cumulative received-prefix checksum:

```
START_REPLICATION SLOT pin PHYSICAL lsn
 (native_format 'pg_lisp_wal_7', system_id '...', timeline '1',
 base_lsn '...', prefix_digest '...')
```

Source acquisition validates identity/base and claims the pin under the WAL
mutex; pin release rejects active senders. The durable pin remains after network
loss for reconnect. Source reads are bounded to one complete CRC-checked record
and the captured durable end, with no destructive startup `wal-scan`. Network
frames are at most 64 KiB of raw WAL. A walk from the persisted confirmed floor
and checksum proves the requested record end and ordered suffix before sending
any WAL bytes; an unconfirmed bootstrap pin starts that proof at the base.
After identity/base validation, CopyBoth permits keepalives during that proof,
avoiding a fixed startup deadline for long prefix verification. Prefix hashing
checks for keepalives between bounded chunks. This checksum detects accidental
prefix/history mismatch; it is not a cryptographic authentication protocol.

The receiver flushes the spool before acknowledging flush, and reports write,
flush and applied positions independently. A separate replay thread invokes the
existing admission/conflict policy, so idle transactions can defer replay while
receipt continues. The current mapper still pauses receipt during actual batch
application and rescans retained WAL; scalable incremental replay remains work.
Source feedback is monotonic and bounded by actually sent bytes, never merely
the keepalive's WAL-end hint. Sender/receiver socket I/O, timeouts, retry delays
and joins run outside admission and storage locks.

Transport loss resets the same healthy store object to manifest-backed flush:
verify the prefix, discard unacknowledged bytes, reset the partial decoder and
written checksum, then reconnect at flush. I/O/decoder/replay failures do not
clear their failure latch through reconnect. Wrong peer identity, unsupported
timeline, invalid source base/prefix or malformed input stop the worker visibly.
Temporary connection loss, shutdown, pin contention and socket timeout retry.
In particular, a sender timeout closes the connection without translating it
into a terminal XX000 response at the receiver.

`physical-follow-start` persists a bounded versioned CRC/literal configuration
after streaming role setup, then starts one receiver/replayer pair. Startup
first recovers the durable prefix, then starts that saved configuration. Reload
preserves workers/state. A separate command mutex serializes config/start/stop;
workers are released from a readiness barrier only after both are created.
`physical-follow-stop` durably disables autostart, closes the owned connection
and joins outside admission, preserving standby role and durable data. These
are local backend lifecycle controls, with no election or promotion policy.

`t/physical-follow.sh` verifies continuous SQL changes, a fragmented 1,000-row
batch, deliberate TCP loss midway through a record, independent receive versus
replay while RR is idle, catch-up after COMMIT, live source/target reload, source
SIGKILL/reconnect, target SIGKILL/autostart, active-pin release rejection, stop,
and terminal wrong-cluster rejection. It also delays feedback for 11 seconds:
SQL remains usable, the sender times out, and the receiver retries and catches
up. Latest formatted run passes: `/tmp/pg-physical-follow.qNmrrT`.

Luna max checkpoint and Sol high full review found availability/lifecycle gaps
that were fixed: source I/O deadlines, retryable timeout classification, proof
heartbeats, linear retained-segment validation, serialized follow commands,
paired worker startup, and explicit-transaction rejection for physical START
(C `walsender.c:2299`). Current focused checks: 57 physical Rove cases and 135
logical interop Rove cases. Automatic edit review once mistakenly treated a
new test file as existing; direct file/Git checks proved it absent, and a minimal
creation followed by structural test insertion preserved all existing work.

Important remaining acceptance gaps: source holds now advance and obey caps as
described below, but the full physical slot SQL/control contract is unfinished.
Local restart still retains the whole post-base prefix, so receiver retention
can grow without bound. Incremental replay, safe spool compaction and durable
local restartpoints remain necessary before calling this a production HA path.
Timeline/history/promotion/rejoin, online base
copy, synchronous candidate/commit-wait semantics and the actual pinned Patroni
interface matrix also remain unfinished. No zero-acknowledged-commit-loss claim
or C physical compatibility claim follows from the native transport tests.

Final transport verification: after the review fixes, strict compilation, the
continuous TCP timeout/partial-record/reconnect matrix, manual replay/crash
matrix and all 1,598 legacy checks pass. Sol medium recommended a checked-feedback
helper; it now centralizes socket-read versus admission ordering and monotonic
status validation for fragment, idle and resume-proof paths. Its follow-up review
found no invariant regression; all new production functions remain below 120
lines. The 57 physical and 135 interop Rove tests pass after this extraction.
Both native C logical interop/crash/feedback matrices pass against the pinned
reference: `/tmp/pg-logical-interop.fgqV9T`, `/tmp/pg-logical-interop.CLpiOJ`.
Latest continuous transport artifacts: `/tmp/pg-physical-follow.rF0lSj`.

## Advancing source retention checkpoint

Native source pins now retain three distinct facts: immutable bootstrap boundary
and identity, a confirmed complete-record flush boundary with its cumulative
prefix checksum, and terminal retention-loss state. Version-2 marker files are
bounded and CRC protected; legacy v1 bootstrap markers remain readable. Before
the receiver handoff, the original checkpoint record remains pinned. After a
verified handoff (including a confirmation exactly at the base), retention moves
to the receiver's durable record boundary and no longer needs that old source
checkpoint record. The bootstrap coordinate is never repurposed as a live floor.

Resume proof begins at the persisted confirmed floor/checksum, not at the original
base. The request still proves the receiver's cumulative prefix through its own
flush. Proof-stage keepalive replies cannot advance retention; publication starts
only after the requested prefix matches. Subsequent feedback proves each newly
acknowledged interval once and rejects interior record positions. Only `flush`
advances retention, independently of `write` or `apply`.

Receipt and cap invalidation both copy the pin, atomically save/fsync/rename/sync
the candidate, then update the stable live pin and recycler registration. A failed
save retains the conservative live floor and fails the WAL operation. This is
stricter persistence than C `PhysicalConfirmReceivedLocation`, which marks a slot
dirty and can conservatively lose recent advancement on crash. The stricter native
publication is needed because its persisted checksum is the new suffix anchor.

Checkpoint maintenance now applies the existing per-slot retained-byte-span cap
to physical pins as well. It durably marks an over-limit pin lost before removing
only that pin's own retention registration; unrelated snapshots remain intact.
An active sender rechecks loss before reading or advancing. Lost markers load
without requiring already recycled WAL, so they do not prevent primary startup.
They remain lost after restart and native follow returns a reseed-required error.
This is checkpoint-time per-pin byte-span behavior, matching the existing native
logical cap; it is not a hard bound on total directory size or a claim that the
full C physical slot SQL/statistics contract is implemented.

`t/physical-retention.sh` passes with 1,800 SQL commits and over 30 MiB of real
WAL. While a target transaction delays replay, durable receipt advances source
retention, a checkpoint removes the original bootstrap segment, and reconnect
no longer scans that missing prefix. It SIGKILLs the source immediately after a
new confirmed marker is durable but before live publication, then verifies source
and target restart plus ordered-row equality. A second SIGKILL immediately after
the lost marker is durable verifies primary admission and terminal reseed refusal.
Latest artifacts: `/tmp/pg-physical-retention.E9t027`.

The larger test exposed two transport problems and one latency issue beyond the
small earlier matrix. SBCL timeouts are not ordinary `error` conditions: a timeout
escaped the receiver thread and terminated the server while request construction
waited for replay admission. Request construction now occurs outside the network
timer, and the retry loop explicitly catches SBCL timeouts. A focused Rove test
reproduces the non-error condition and proves the retry loop continues. Test polls
now detect dead owned servers rather than repeatedly connecting after a crash.

The live source and standby control REPLs also showed Nagle enabled on the
replication connection. Changing TCP_NODELAY on those actual sockets improved
progress without restarting Lisp. Saved setup now matches C `pq_init` for backend
TCP sockets and libpq `connectNoDelay` for the receiver client. Receiver writes
and flushes are batched separately (1 MiB while busy, flush on keepalive while
caught up), following C walreceiver's write-then-flush cycle. Neither a keepalive
WAL-end hint nor merely written bytes become a durable acknowledgment. Rove
coverage proves the batching/keepalive distinction. The large restart still
replays the full local prefix; its test allows measured startup time beyond the
small-fixture default without weakening listener ownership checks.

Current focused suite: 63 physical Rove cases, including v1 migration, saved-floor
reload after recycling, pre/post-save failures, durable cap loss, independent
snapshot pins, receiver flush batching and SBCL timeout recovery. Luna max found
no source-publication ordering defect; its requested publication-boundary SIGKILL
coverage was added. Sol medium found no worthwhile new macro and all new
production functions remain below 120 lines.

### Local restartpoints and spool floors: design requirements

Source advancement is independent of local retention. With immutable base `B`,
source confirmed cursor `S`, local restartpoint `R`, applied `A` and receive flush
`F`, require `B <= R <= A <= F` and a reconnecting receiver's `S <= F`. At the
source-retention checkpoint `R = B`; the explicit local restartpoint milestone
below now advances R and recycles the receiver's older complete segments.

Astra high's design audit requires an immutable restartpoint generation binding
`R`, cumulative checksum at `R`, original manifest digest/identity/timeline,
observed read FullXID, exact catalog snapshot, and every live fork's minimum
durable length, including relations created or grown after bootstrap. The live
catalog can be ahead after a crash and cannot substitute for this snapshot.
Publish synced data and generation, then a synced restartpoint pointer, then the
receive manifest's floor/seed, and only then delete obsolete complete segments.
New pointer plus old spool floor is a valid crash state; spool floor newer than
its published restartpoint is invalid.

Restart must validate the retained suffix and its complete catalog transitions
before restoring the restartpoint catalog and replaying it. A missing fork from
`R` is permitted only when complete, validated retained history proves its later
drop/replacement; an incomplete catalog batch or the current on-disk catalog is
insufficient proof. Surviving forks need their `R` minimum lengths checked before
redo can synthesize holes. Chunked catalogs, empty tables, relfilenode history and
CLOG resets must be covered. Receive control needs separate floor, floor seed,
flush and cumulative flush checksum so compaction preserves reconnect identity.
The local primitive implementation is recorded below. Automatic scheduling,
promotion/history/rejoin, synchronous durability, online base backup and the
pinned Patroni interface matrix remain active goal work.

Final source-retention checks pass: strict forced compilation, 63 physical Rove
cases, 135 logical interop Rove cases, all 1,598 legacy checks, native continuous
follow (`/tmp/pg-physical-follow.QMVmLk`), and the original unconfirmed bootstrap
pin/restart/release regression (`/tmp/pg-physical-source.ZZD8xi`). Sol high's full
review found no remaining production blocker after comparing C source retention,
slot invalidation, receiver flush ordering and TCP socket setup. Both native C
logical interop matrices and all three manual switchovers also pass after the
global TCP_NODELAY setup change. Switchover artifacts:
`/tmp/pg-switchover.eNcHld`, `/tmp/pg-switchover.OWtfXt`,
`/tmp/pg-switchover.1nTK89`; C-to-Lisp interop artifacts:
`/tmp/pg-logical-interop.Vmaxpf`. Shell syntax and whitespace checks pass.


### Local restartpoint prerequisite: validated checksum anchors

The receive store now exposes `physical-store-prefix-digest` at an exact durable
record end or its original base. The store derives the cumulative checksum from
validated spool bytes, never a caller-supplied seed. The existing full-prefix
validator captures the anchor during its ordinary checksum pass and returns it
only after validating the entire acknowledged prefix, including records after
the requested anchor. The durable replay mapper uses the same boundary check.
Rotation padding, interior offsets and unflushed positions are rejected and latch
the store. This API certifies neither a complete terminal batch nor fsynced data.

Three new Rove cases cover exact checksums across rotation and reopen, unflushed
input, invalid boundaries, and corruption beyond the requested anchor. The
existing immediate-first-record rotation case also checks the base seed and
final checksum directly. All 66 physical Rove cases pass; strict compilation
and formatting pass. Sol high found no correctness issue in this change.

At this checksum-anchor checkpoint, local R still remained at B. The later
history and explicit restartpoint milestones below add the catalog/fork validation,
publication ordering, receive control v3, compaction and suffix restart.

The native replay regression also passes after mapper integration
(`/tmp/pg-physical-replay.caIznx`). Luna max found no checkpoint blocker; its
README test-discovery observation is fixed by documenting the physical ASDF
suite alongside the interop suite. The invariant/LOC review found no useful
additional abstraction; all receive-store production functions remain under
120 lines.


### Local restartpoint prerequisite: complete catalog history before redo

Phase-2 startup now validates the receive spool and scans its complete native
batch history before opening CLOG for writes or applying any database redo.
`physical-history.lisp` checks batch transitions, page images, logical chunks and
all terminal payloads with the existing pure native decoders. Only a complete
catalog terminal, including reassembled chunked images, certifies its explicit
relfilenode delete list. An unfinished batch proves no deletion. A later catalog
reference revokes earlier deletion proof. Original base main/visibility forks
without such proof must exist and meet the manifest's minimum durable length.

The catalog proof reader requires one form, a matching cluster identity and
unambiguous property keys. It collects every serialized table/index reference
before catalog hash-table construction, so duplicate names cannot hide a live
fork. Sol high and Luna max identified a circular-reader-label startup hang;
the reader now rejects label definition/reference dispatch and read-time
evaluation while preserving native vector and SBCL readable string-array forms.
Sol high verified the fix. The shared `with-physical-replay-staging` macro resets
the same batch state for the proof reader and applying replay. The final
invariant/LOC review found no additional useful abstraction and all production
functions below 120 lines.

All 73 physical Rove cases, 135 interop Rove cases and 1,598 legacy checks pass;
strict forced compilation and formatting pass. The expanded native replay test
passes at `/tmp/pg-physical-replay.3V1rOW`: it recovers a real post-base DROP,
replays after process crashes, and rejects missing/truncated base heap clones,
including one whose mutable disk catalog has been damaged to omit the heap.
The native harness also reloads saved code through the actual task-owned control
socket. These are process-crash tests, not power-loss evidence.

This history milestone initially left local R at the immutable bootstrap base.
The restartpoint milestone below adds generation/pointer publication, receive
manifest v3 floor/seed, suffix restart and spool compaction using this validator.
Automatic restartpoint scheduling was still unfinished at that checkpoint;
the later automatic-worker milestone below completes it.

Continuous native follow/reconnect and source/target crash restart also pass
with the preflight enabled (`/tmp/pg-physical-follow.oOby6g`).


### Explicit local restartpoints and receive-spool compaction

`(physical-recovery-restartpoint)` is a literal, no-argument private control API.
It shares replay's admission/serialization barrier, defers for retained reader
transactions, and never promotes. With a validated checksum at the last applied
terminal R, it syncs relation streams, CLOG, the catalog and data directories;
writes a generation that will never be overwritten; syncs a pointer naming that
generation; publishes receive control v3; and only then deletes complete spool
segments strictly before R's segment and syncs the directory. It preserves
complete unflushed writes and a partially decoded record. The floor-only control
update saves H(F), even when W > F; it never promotes H(W) to durable receipt.
Obsolete generations are removed only after pointer/floor/deletion durability.

A generation binds original base B, cluster identity/system ID/timeline, original
manifest digest, R/H(R), observed read FullXID, the exact catalog bytes, every
catalog-referenced main/visibility fork's minimum durable length, and relation
identity history. Raw catalog references drive coverage checks. Every catalog
terminal now records index identities as well as heaps in replay history. Native
metadata is bounded to 16 MiB; a generation including catalog bytes is bounded to
1 GiB, and publication enforces the same bounds used by its reader. This remains
a native format, not PostgreSQL's `pg_control` or C WAL format.

Receive control v3 preserves immutable B separately from local floor R and seed
H(R), durable F and cumulative H(F). Existing v2 controls open with floor B and
the original seed, and upgrade on their next publication. Startup validates the
authoritative pointer/generation and receive suffix, requires
`floor <= R <= old-applied <= F`, checks H(R), complete terminal history and every
surviving R fork's minimum length, then restores the R catalog before suffix redo.
A new pointer with the old floor is valid; a newer floor without its generation
is rejected. Corrupt or missing authoritative state never selects an older image.
Startup progress persistence cannot lower the pre-crash required applied/XID
watermarks when replay revisits earlier terminals, including after a second crash.
Original immutable bootstrap provenance files and base WAL are still retained on
the receiver; this milestone compacts its receive spool only.

Astra high reviewed the design against C `CreateRestartPoint` (xlog.c:8641,
8720, 8732 onward). Luna max checked publication windows, Sol high reviewed the
full implementation, and Sol medium found no useful additional invariant/LOC
refactor. No production function exceeds 120 lines. The deliberate native
difference is using a locally captured complete terminal/catalog/fork snapshot,
where C selects a replayed checkpoint record. Interrupted publication fails
recovery closed, including nonlocal exits before the pointer is published.

All 87 physical Rove cases and 135 logical interop Rove cases pass, as do strict
forced compilation, formatting, shell syntax and whitespace checks. Tests cover
checksum/floor migration, new-pointer/old-floor recovery, F > A > R and an
unfinished tail, unflushed/partial input preservation, missing or corrupt pointer
and generation, R-fork minimum lengths, complete later DROP exemptions, and
monotonic startup minimums. The first two-restartpoint native replay run passes
(`/tmp/pg-physical-replay.W3c1nN`). The >30 MiB continuous native test also passes
(`/tmp/pg-physical-retention.uKw01D`): it advances source retention independently,
publishes local R, proves the receiver's original segment was deleted, reloads
the task-owned live image, SIGKILLs the receiver and resumes from its compacted
spool. It also retains the source receipt/lost-pin crash cases. Publication-window
SIGKILL cases are run separately below. These tests establish process-crash
behavior, not power-loss durability or C physical compatibility.

Remaining work includes automatic/periodic and shutdown restartpoints, online
bootstrap, real promotion/timelines/history/rejoin, synchronous commit semantics,
and the pinned Patroni backend interface matrix. Coordination remains excluded.

The final native replay run passes with intentional SIGKILL after pointer
publication (new pointer, old receive floor) and after receive-floor publication
(before deletion/GC), followed by fresh-process recovery and full SQL comparison:
`/tmp/pg-physical-replay.9xJYy1`. The final 1,598 legacy checks also pass
(`/tmp/pg-restartpoint-unit.log`).

Both pinned native C logical interop directions pass after restartpoint
integration: `/tmp/pg-logical-interop.FJGu2n` (Lisp-to-C) and
`/tmp/pg-logical-interop.ImqVR6` (C-to-Lisp). These remain logical compatibility
regressions; they do not establish physical WAL or disk compatibility.

All three manual logical switchovers also pass after the final restartpoint
changes: Lisp-to-Lisp `/tmp/pg-switchover.6oOXHy`, Lisp-to-C
`/tmp/pg-switchover.IpjB8v`, and C-to-Lisp `/tmp/pg-switchover.ucVyil`.


### Automatic standby restartpoints and SQL CHECKPOINT

Streaming recovery now starts exactly one local maintenance worker, independently
of native following. Startup and first streaming enablement share a mutex around
worker creation. The worker requests R after 300 seconds or 1 GiB of applied WAL
since the receive floor, only when apply has advanced. It checks floor/apply under
the receive-store mutex, leaves ordinary admission before requesting replay
quiescence, and rechecks due status inside the barrier. Retained transactions
still defer background publication with a one-second retry delay. Reload preserves
the worker and settings; each iteration calls a replaceable step function. The
native thresholds are presently Lisp globals, not completed PostgreSQL GUC or
Patroni configuration support.

SQL CHECKPOINT is now allowed in recovery, including inside read transactions.
C utility.c:247-256 classifies it as strictly read-only specifically so recovery
can request restartpoints; utility.c:947-949 and checkpointer.c:1000-1044 impose no
transaction-block ban. The Lisp command now routes to the shared publication core
under its existing SQL admission, without reacquiring the reload mutex or closing
its own gate. Admission pins apply and excludes replay; the store mutex serializes
publication. A reader snapshot can remain open because publication does not alter
MVCC data. An immutable static base with no receive spool returns success without
creating an R generation. Primary CHECKPOINT also no longer rejects a transaction
block; the native test verifies that it does not commit a pending INSERT.

The shared core keeps Swank exclusion and the nonlocal-exit failure latch over
publication and generation cleanup. An automatic checkpoint error fences SQL with
57P03 until restart. The worker stops on recovery failure. Luna max identified a
startup/control worker-creation race, fixed by the start mutex; Sol high found no
remaining verified blocker. Sol medium found no useful additional macro/LOC
refactor, and all production functions remain under 120 lines.

The native automatic test passes at `/tmp/pg-restartpoint-auto.x94csv`: static
CHECKPOINT, timer-only and WAL-distance triggers, a retained repeatable-read
snapshot, SQL CHECKPOINT in a transaction, single-worker live reload, an injected
background publication failure, SQL failure fencing, and fresh-process recovery.
The continuous >30 MiB test also passes with automatic byte-triggered compaction
(`/tmp/pg-physical-retention.rzXzpZ`), receiver segment deletion, reload, SIGKILL,
reconnection and source retention/lost-pin crash regressions. All 93 physical
Rove cases (including the new 200-trial cl-spec scheduling property, seed 42),
135 interop Rove cases and 1,598 legacy checks pass. The scheduling property checks
LSN translation invariance, monotonicity and no-progress suppression across large
coordinate ranges. Strict compilation, formatting, shell syntax and whitespace
checks pass.

Orderly standby restartpoints now run from proper main-thread finalization
above, following C ShutdownXLOG's CreateRestartPoint call (xlog.c:7422). The
earlier direct last-backend exit was unsuitable: SBCL exit hooks unwind admission
first, run in unspecified order and swallow hook errors, so an exit-hook solution
would not provide the required completion/error semantics. Native durable recovery
state is now recorded above; full C control metadata remains unfinished.
Promotion/timelines/history/rejoin, online bootstrap, synchronous commit and the
pinned Patroni interface matrix remain active goal work; coordination is excluded.

The static-standby acceptance run also passes with CHECKPOINT removed from its
mutation rejection list and exercised inside a transaction as a successful no-op:
`/tmp/pg-physical-bootstrap.ZKEdza`. Other mutation, recovery-role and immutable
base checks remain in that regression.


### Local PGDATA ownership and pinned Patroni process discovery

`backend/postmaster/pidfile.lisp` claims PGDATA immediately after argument/path
setup and before recovery preparation, spill cleanup or any storage initializer.
It validates directory type, effective UID and the C 0700/0750 permission policy;
a missing final directory is created atomically with mode 0700, and existing
permissions are never silently repaired. Physical test copy directories now use
0700 explicitly. A stable, dedicated regular `.pg_lisp.lock` inode provides an
exclusive local flock for Lisp contenders and stays open until OS process exit.
No lock symlink, hardlink or special-file target is accepted. This is local data
ownership, not a distributed lease or writer-generation mechanism.

Under that lock, startup uses a bounded regular, nonblocking/no-follow PID reader,
C's live-PID and ancestor/grandparent rules, and O_EXCL creation of a mode-0600
`postmaster.pid`. Empty/malformed/oversized or nonregular PID metadata fails
before database initialization. The eight C-layout lines contain the actual PID,
canonical PGDATA, Linux process creation timestamp from /proc, port, blank Unix
SQL socket and SysV shared-memory fields, actual TCP listen address, and padded
status. Status starts as `starting`; `ready   ` is synced only after successful
SQL listener bind. No listen address is invented before bind. Normal exit removes
only the still-owned PID inode; the stable flock remains held through all exit
hooks. SIGKILL releases the kernel lock and leaves a reclaimable stale PID file.
Startup errors after PID creation remove that process's PID file. Reload preserves
ownership and start time.

C references are `miscinit.c:296-383,1159-1453` and `include/utils/pidfile.h`.
The private flock serializes cooperating Lisp servers only. Native C does not
honor it; the shared C-style PID check does not eliminate C's own read/unlink
stale-reclaim race when incompatible implementations are started simultaneously
on the same PGDATA. Neither cross-C physical data compatibility nor protection
against a same-UID actor replacing directory entries is claimed. Do not start C
against native pg-lisp data. PID cleanup's inode check handles an already-replaced
file, but cannot make check-then-unlink atomic against external pathname mutation.
This implementation is Linux-specific; non-Linux process-time support, stopping/
standby status transitions, postmaster.opts, direct Patroni start/config arguments,
GUC/config reload and proper orderly shutdown finalization remain incomplete.

The real Patroni 4.1.5 `PostmasterProcess` reader discovers the task-owned server,
validates its process start time, and fast-stops it with `signal_stop`. The test
verifier is a sibling process because Patroni deliberately rejects its own direct
child as an already-running postmaster. The pinned psycopg2-binary 2.9.11 driver
connects, reports server_version 200000 and executes parameterized SQL; this does
not exercise the full Patroni SQL matrix or erase native PG_VERSION/control-file
format differences. `t/interop/patroni-requirements.txt` pins all installed packages.
No Patroni HA controller, DCS or election process is started by this test.

Initial native acceptance passes at `/tmp/pg-postmaster-pid.WMB1i2`; the final run
adds wrong-start-time rejection, PID FIFO refusal and the final lock/endpoint
checks. Five new Rove PID cases cover metadata width/permissions, cleanup after
inode replacement, duplicate local lock acquisition, permission refusal without
repair, and symlink non-following. All 98 physical Rove tests, 135 interop Rove
tests and 1,598 legacy checks pass. Strict compilation and formatting pass.
Luna max checked ownership/startup boundaries and the native-C concurrency limit;
Sol high reviewed normal lifecycle correctness and same-UID pathname caveats.
The dedicated-lock symlink risk was fixed, and Sol medium found no useful further
macro/LOC refactor; all production functions remain under 120 lines.

The attempted shutdown checkpoint edit remains blocked specifically by cl-mcp's
reader rejecting valid #. syntax in postmaster.lisp. A dry-run structural edit
confirmed the restriction again; no valid C-shaped reader forms were rewritten
and no unsafe exit-hook substitute was introduced. Other active goal work
continues independently of that tooling limitation.

Final PID/Patroni acceptance passes at `/tmp/pg-postmaster-pid.8oqxb7`, including
wrong process-start-time rejection, special-file refusal, live reload, regular
lock-file guards and fresh ownership after SIGKILL. The first final run exposed
an anonymous SBCL descriptor stream that cannot answer FILE-LENGTH; the PID reader
now uses FSTAT/STAT-SIZE, with a direct stale-reader regression assertion. This
was a real stale-reopen failure, not missing Lisp forms or parentheses.

Existing static-standby/bootstrap acceptance passes with ownership acquired before
recovery preparation (`/tmp/pg-physical-bootstrap.xZLCvj`), including source
SIGKILL and stale PID reclaim. Existing shutdown acceptance also passes after
PID cleanup integration (`/tmp/pg-pid-shutdown.log`: smart/fast/immediate,
escalation and administrative cancel/terminate). This verifies existing shutdown
behavior and PID removal. Standby shutdown restartpoint integration was added
later, as recorded above.

Continuous native follow/reconnect also passes with PGDATA ownership enabled
(`/tmp/pg-physical-follow.uAARci`). Descriptor inspection in the live cl-mcp image
showed the newly opened ownership/PID FDs lacked close-on-exec flags; they now
open atomically with Linux O_CLOEXEC so future executed helpers cannot retain the
server's ownership descriptor. The metadata Rove case asserts both descriptor
flags in addition to its stale-reader coverage.

The final close-on-exec build passes strict compilation and the complete pinned
Patroni/PID native smoke at `/tmp/pg-postmaster-pid.c4adKw`.


## Direct startup and configuration checkpoint

The pinned Patroni 4.1.5 `ConfigWriter`, `get_postgres_version` and
`PostmasterProcess.start` now exercise the actual native executable. Configuration
supports `port`, `listen_addresses`, `data_directory`, `checkpoint_timeout`,
`max_wal_size` and `max_slot_wal_keep_size`, plus the immutable `config_file`
location. Includes resolve relative to the lexical parent path, including symlink
paths; extensionless includes and sorted nonhidden `.conf` include directories
work. Precedence is defaults, primary/includes, actual PGDATA auto.conf, then CLI.
Unknown parameters and CLI arguments fail visibly rather than being ignored.

Parsing follows pinned C `guc-file.l`, `conffiles.c` and `guc.c`: literal quoted
strings, doubled quotes and C/octal escapes never invoke Lisp READ; numeric
conversion uses libc and C unit rounding. Native C executable comparisons cover
hex, ties and intermediate unit rounding. SHOW reports installed values. SQL SET
and RESET reject server-context parameters with C SQLSTATE 55P02.

SIGHUP validates the full configuration before installing reloadable values.
Restart-only changes keep active values and record pending restart internally.
A malformed or unsupported setting retains the previous configuration. CLI
settings persist across reload. Reload now restores configured restartpoint
thresholds; tests that temporarily override private variables must reinject them
after reload. Boot compilation output goes to stderr so `--version` has clean
stdout even with an empty FASL cache.

The native smoke passes at `/tmp/pg-patroni-start.wdb6zm`, including live reload,
restart-only behavior, failed reload retention, session-context errors, direct
process cleanup and the pinned C numeric oracle. All 105 physical Rove tests
pass. Luna max and Sol high reviewed this milestone; reported include-path,
lexer and escape defects were fixed. Sol medium found no useful additional
macro/invariant refactor; production functions remain below 120 lines.

This is a subset, not general Patroni compatibility. Full `pg_settings` and
`current_setting` SQL/provenance, recovery parameters, startup connection options,
NamedConnection queries and utilities still need implementation and acceptance.
Empty or multiple listen addresses are explicitly unsupported. Native defaults
remain port 5433 and a 1 GiB slot retention cap; C defaults differ. Private native
byte-limit options can override the displayed slot limit. Configuration reload
is atomic for this subset; C can apply some valid entries alongside semantic
errors. Primary automatic and shutdown checkpoint parity remain missing;
native standby shutdown restartpoints are now integrated above.
No HA controller, DCS, election or failover coordination runs in these tests.

Final validation also passes strict forced compilation, all 135 interop Rove
tests and the automatic restartpoint native regression
(`/tmp/pg-restartpoint-auto.OmEFO7`). Formatting and `git diff --check` pass.


## Typed settings catalog checkpoint

`pg_catalog.pg_settings` now exposes real installed rows for the supported
startup GUCs, with typed name/setting/unit/context/vartype/source/sourcefile/
sourceline/pending_restart columns. Numeric settings use base-unit text rather
than SHOW's human formatting, following `guc_funcs.c:GetConfigOptionValues`.
Only supported settings are present. The selected config and data paths have
source `override` and NULL file metadata, matching `SelectConfigFiles`; the data
path remains locked on reload. Removal of a file-sourced restart-only setting
marks pending restart even if its old value equals the default. SQL `set_config`
now uses the same server-context rejection as SET/RESET.

The pinned C initdb catalog reports pg_settings OID 12109. Virtual catalog
attribute introspection now uses each descriptor's original C attribute numbers,
matching RowDescription, while executor row slots remain dense internally.
`relnatts` spans the highest exposed original attribute. Unimplemented columns
remain absent rather than being populated with invented metadata.

The native byte-cap option is a documented extension: when used, its row reports
unit B and its actual command-line/environment source, avoiding silent fractional
megabyte truncation. Ordinary GUC values retain C's fixed MB units. Full C view
columns and all existing SHOW-only session GUC rows remain unfinished, as do
`current_setting`, `lower` and the complete pinned ConfigHandler SQL call path.
Next acceptance must invoke `_get_pg_settings` and
`load_current_server_parameters` themselves through psycopg2 with adapted arrays;
a named-column catalog smoke does not establish that those calls work.

Acceptance passes at `/tmp/pg-patroni-start.9Yzhjw`: base units/provenance,
pending restart, selected data-path override, catalog attribute numbers and
SET/RESET/set_config context errors are checked through real psycopg2 SQL.
All 107 physical and 135 interop Rove tests pass; strict forced compilation
passes. Sol high's catalog-origin mismatch was fixed; Sol medium found no
useful further refactor and verified the reviewed functions are below 120 lines.

## Patroni configuration SQL checkpoint

`current_setting(text[, boolean])` and C/POSIX `lower(text)` now run through the
ordinary function analyzer and evaluator. SHOW and current_setting share one
value lookup, including per-session values and human unit formatting. Missing
settings report 42704 or NULL with missing_ok; strict NULL arguments propagate.
Unknown Boolean string literals resolve through Boolean input, matching C
function overload resolution. Lowering is ASCII-only under the supported C/POSIX
collation; other collation providers remain unimplemented.

The real pinned `ConfigHandler._get_pg_settings` and
`load_current_server_parameters` methods pass against the native server with
psycopg2 2.9.11 adapted arrays. The harness supplies an actual SQL connection
without constructing a Patroni HA controller; it does not test the full
ConfigHandler constructor or its generated configuration. The latter query
exposed missing scalar-array ALL support, now shared with ANY and compared with
C `ExecEvalScalarArrayOp` for empty-array identities, NULL results and decisive
short circuit. One-dimensional text-array literal input follows C
`ReadArrayToken` escaping, quotes, whitespace and unescaped NULL rules. This
also handles psycopg2's empty-list representation. Explicit dimensions and
multidimensional arrays reject 0A000; general numeric-array inference remains
unfinished.

### Scalar-array inference correction

A late Luna checkpoint identified that bare string array literals must resolve
their element type from the scalar operand before input conversion. The analyzer
now applies the selected scalar input function to the complete literal array
and stores a private scalar-array constant. This covers numeric literals,
internal char first-byte input and name clipping without pretending that all
standalone array types are implemented. Invalid later elements and invalid
literals in unreachable CASE arms fail during analysis; explicitly typed text[]
does not gain numeric coercions. Pinned C standalone execution confirms the
numeric/char/empty examples and rejects the bad integer with 22P02.

The correction passes native acceptance at `/tmp/pg-patroni-start.HC2GXL`, all
111 physical and 135 interop Rove tests, strict forced compilation and formatting.
Sol high reviewed the final eager-input representation; Sol medium found no
useful refactor and verified the function length limit.

### NamedConnection and timeout intake

The actual pinned ConnectionPool/NamedConnection connects and reconnects against
the native server (`/tmp/pg-named-connection-audit.WYmAMk`). Its default startup
options are `-c statement_timeout=2000 -c pg_stat_statements.track=none`. This
audit found both options silently ignored: current_setting with missing_ok
returns NULL, and the task-owned server's control REPL shows an empty session
settings table for `Patroni audit`. This is an outstanding behavioral defect,
not successful timeout acceptance.

The next implementation must follow C postinit.c pg_split_opts and
process_startup_options: backslash escapes, whitespace separation, literal quote
characters, client source, and first-equals assignment splitting. Dotted custom
GUC placeholders can hold pg_stat_statements.track without claiming extension
execution or emitting a pg_settings row. statement_timeout needs a typed ms
USERSET setting and a startup RESET baseline independent of transaction undo.
Arm before query parsing after admission, reset between statements, clear on
every exit, and report 57014 with C's timeout message. Existing B-tree waits are
unbounded, so adding only a deadline check would leave a false timeout promise;
bounded interruptible lock waits are part of this milestone. Validate blocked
locks, idle gaps, multiple statements, rollback/RESET, cancellation precedence,
and reload-gate/drain behavior before claiming the NamedConnection contract.

## Session startup options and statement timeout checkpoint

The ignored NamedConnection options identified above are now implemented.
Startup `options` follow C `pg_split_opts` whitespace/backslash splitting and
`-c`/long assignment parsing; additional startup GUC fields follow those options.
Invalid names, invalid timeout values and attempts to change server-context GUCs
reject startup with FATAL errors. Dotted custom GUCs are inert placeholders, so
`pg_stat_statements.track=none` can be stored/read/reset without claiming that
the extension runs or showing placeholders in pg_settings.

statement_timeout is a typed per-session millisecond setting. SQL SET, RESET,
set_config, SHOW and current_setting share its value path; pg_settings reports
the actual base-unit value and client/session/default source. A separate startup
baseline survives RESET ALL and transaction rollback. Default restoration keeps
client provenance, including mixed-case startup names. Custom placeholders also
retain their empty default after reset or rollback, as verified against pinned C.

Deadlines arm after reload admission before parsing, reset between statements
within one simple-query frame, and clear on errors and frontend cleanup. The
drain-lane parse-only pass can carry its first-statement deadline into execution.
Existing interrupt checks report 57014 with C's statement-timeout message.
Transaction/schema waits and B-tree relation waits poll the remaining deadline
with bounded condition waits, preserving mutex reacquisition and waiter cleanup.
The backend structure gained reset/deadline slots; existing Lisp images require
a restart for this layout change.

Native acceptance at `/tmp/pg-patroni-start.R9Kc1e` calls the actual pinned
ConnectionPool/NamedConnection with its unchanged default options. It checks
client values/source, hidden placeholders, rollback/RESET/default provenance,
reconnect, invalid startup rejection, a blocked UPDATE timing out, successful SQL
after that error and an idle gap, and two statements taking over one total timeout
budget while each stays within its own budget. A private control-created worker
also holds reload admission closed beyond the client timeout, verifies that the
waiting backend has no deadline, and releases it for successful SQL. A file start
barrier lets the originating control session disconnect before the worker closes
admission; ordinary control evaluations themselves require admission.

The same NamedConnection timeout/reset/reconnect helper passes against the pinned
C executable (`/tmp/pg-c-session-timeout.XynlxD`). All 115 physical and 135 interop
Rove tests and 1,598 legacy checks pass. Strict forced compilation and formatting
pass. New generated option escaping coverage runs 200 cl-spec trials
at seed 42, alongside the previous array/recovery properties. Sol high's two
provenance findings were fixed; Sol medium found no useful refactor and verified
production functions stay below 120 lines.

Luna initially reported blocked output as a timeout gap, then withdrew that
finding after checking the pinned implementation. Astra high independently
confirmed C `secure_write` waits indefinitely and `ProcessClientWriteInterrupt`
handles only ProcDiePending. Ordinary statement cancellation is deliberately
deferred while a partial output frame is blocked. No SBCL IO timeout/unwind/retry
was added: that could corrupt a partially transmitted frame. Deadline handling
uses safe interrupt points, not asynchronous exceptions inside storage code.

This does not complete the full GUC contract: SET LOCAL, file-configured USERSET
defaults, complete session catalog coverage, recovery GUCs and additional utility
and process interfaces remain unfinished. More cancellation/drain-boundary and
stalled-output regression coverage remains useful; this checkpoint introduces no
HA controller or coordination.

The broad native regression initially caught an actual Lisp-to-C subscription
startup regression: C libpqwalreceiver appends `datestyle=ISO`,
`intervalstyle=postgres` and `extra_float_digits=3`. These now pass explicit typed
validation. DateStyle uses the existing C-style SplitIdentifierString parser,
including spaces and quoted tokens, and supports ISO/MDY; IntervalStyle supports
postgres. Other formatting modes reject visibly. extra_float_digits uses C's
integer range -15..3 and default 1; native SQL float types remain unsupported.

Final results include 116 physical tests, 135 interop tests,
NamedConnection/config/reload acceptance at `/tmp/pg-patroni-start.ByD03d`,
native Lisp-to-C logical acceptance at `/tmp/pg-logical-interop.yahQHc`, and C-to-Lisp acceptance at
`/tmp/pg-logical-interop.WNBJTi`. All manual switchovers pass: Lisp-to-Lisp
`/tmp/pg-switchover.Zc6rC1`, Lisp-to-C `/tmp/pg-switchover.8ksuPe`, and C-to-Lisp
`/tmp/pg-switchover.l6Q2cN`. The final NamedConnection helper also passes pinned C
at `/tmp/pg-c-session-final.bBKOKB`, including startup FATAL rejection and custom
placeholder rollback. This preserves logical interoperability, not C physical
WAL or disk compatibility.

## Promotion intake and history interfaces

Promotion remains unimplemented. C `StartupXLOG`, `FinishWalRecovery`,
`XLogInitNewTimeline` and `CreateEndOfRecoveryRecord` establish the ordering
requirements: choose an unused timeline, preserve the valid ancestor segment
prefix, publish history and durable end-of-recovery/checkpoint state, initialize
writer/XID state, then expose the end of recovery. Native receive WAL lives in a
separate spool and restartpoint compaction may have removed its old prefix;
received flush may also exceed the last applied terminal. Neither copying the
remaining spool nor clearing recovery.required can perform a safe promotion.

The next writer milestone needs timeline-aware primary WAL paths/control/scans
and backup inventories, durable promotion intent and a positive committed-primary
state, recovery worker shutdown/join, exact-terminal replay and receive-tail
discard, XID allocation reservation, a validated child segment prefix, history,
end-of-recovery record and checkpoint before reopening writes. Preserve the
immutable bootstrap manifest. Crash injection must cover every durable step.
Ancestry and numeric TLI alone cannot reject independently promoted sibling TLI2
branches; verify system identity and exact prefix/branch identity as well.

The required WAL structural edits are currently tool-blocked: a fresh cl-mcp
dry-run on wal-init rejects the file's valid read-time `#.` forms with
`*READ-EVAL* is false`. That is not a parenthesis error. No valid reader forms
were rewritten and no direct-file-edit or live function-shadowing workaround was
used to sidestep the requested structural editing workflow. Independent backend
interfaces continue to progress while this specific limitation remains.

`backend/access/transam/timeline.lisp` now provides the history prerequisites.
Parsing uses the exact C scanf conversions/low-word width from `timeline.c`,
preserves increasing ancestor identifiers and newest-first intervals, and ignores
reason text as C does. A separate forward-lineage validator rejects backward
switchpoints before native safety checks. WAL point lookup uses half-open
intervals; a prefix cursor ending at a fork may still belong to its parent.
The latter predicate proves only an ancestry position, never equality of bytes
or writer authority. Missing non-1 history fails closed here; C's general reader
can instead assume no parents. Native history reads also impose regular-file,
16 MiB file and 1023-byte line bounds.

The replication protocol implements TIMELINE_HISTORY with C's filename/content
text columns and exact raw file bytes. Missing files return 58P01 and explicit
transactions reject the command. History and SHOW remain read-only on a standby.
Startup replication=true is now accepted for physical inspection: IDENTIFY_SYSTEM
returns a NULL database, ordinary SQL is rejected, and database-dependent logical
decoding is refused. Database-scoped replication keeps its SQL fallback. SHOW is
a replication command in both modes, including C's distinction between plain
SHOW and a leading SQL comment. Leading comments/semicolons are not skipped by
the replication classifier; normal drain classification keeps its SQL-trivia
handling. IDENTIFY_SYSTEM retains the pinned C tree's int8 timeline column.

This is inspection/history support, not C physical streaming or promotion.
Primary WAL, recovery controls, native source pins and follow still support only
timeline 1. The test writes a clearly synthetic timeline4 history fixture to the
owned throwaway cluster, calls the unchanged pinned Patroni Postgresql.get_history
and get_connection_cursor methods, then removes that fixture; IDENTIFY_SYSTEM
continues to report the actual timeline1. No active timeline or recovery role is
changed. Native acceptance passes at `/tmp/pg-patroni-start.TGeU11`.
Identical assertions pass pinned C at `/tmp/pg-c-history-final.xpMQ4v`.
All 123 physical and 135 interop Rove tests pass, including 200 generated fork
boundary cases at seed42. Luna's SHOW/comment dispatch findings were fixed;
Sol high found no further defect in the implemented slice, and Sol medium found
no worthwhile refactor or function exceeding 120 lines.

Existing continuous native follow/reconnect acceptance also passes after this
startup/dispatch change (`/tmp/pg-physical-follow.p5p7xz`), including live reload
and owned source/target crash/restart. All 1,598 legacy checks, strict forced
compilation, formatter checks and `git diff --check` pass.

Native acceptance passes at `/tmp/pg-patroni-start.KVANBR`, including empty
parameter lists and current_setting after live reload. All 111 physical and 135
interop Rove tests and 1,598 legacy checks pass. A new cl-spec array input/output roundtrip checks 200
cases at seed 42, with zero rejections. Strict forced compilation and formatting
pass. Pinned C standalone execution independently confirms ASCII lower with
non-ASCII preservation, empty ANY/ALL identities and NULL/missing-setting cases.
Luna max's unknown-Boolean coercion finding was fixed. Sol high found no further
defect within the supported settings/text-array scope; Sol medium found no useful
refactor and verified the production function length limit.

Remaining configuration acceptance includes all supported session GUC catalog
rows, complete view columns, recovery parameter persistence/restart rules,
startup connection options and the actual NamedConnection/reconnect path. Those
requirements remain in the active goal alongside promotion/timeline, sync commit,
online backup and process/utility work; this checkpoint does not complete Patroni
integration and introduces no coordination.
