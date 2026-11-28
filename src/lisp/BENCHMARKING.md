# Benchmarking pg-lisp

[Active TODO](TODO.md) · [Completed development work](COMPLETED.md)

Run the commands from the checkout root unless a harness specifies otherwise.
Use a pinned C PostgreSQL build and record the Lisp revision and SBCL runtime.
Verify query results and plan selection before comparing timing. Use the same
data, SQL and sample counts for each engine, and pin both engines to one CPU
for controlled CPU comparisons. Alternate engine order where the harness
supports it. Record which process supplied peak RSS, especially when comparing
the whole Lisp server with one C backend.

Run allocation collection separately when it uses the control socket.
Keep warm-cache measurements, fresh-server RSS, and holdouts identifiable in
the logs. Compact runtime settings below are historical profiles for specific
workloads. Check server and replay acceptance at the selected heap and GC limits.

The workload sections retain the original reproduction settings and measurements
for comparison with future runs.

## Shared page cache

Compare resident dirty hits, spilled dirty reads, warm clean hits, and large
streaming scans separately. Measure copies, physical reads, allocation,
retained page versions, WAL-lock wait and writer latency.

A local SBCL microbenchmark compared the existing owned buffer API with
scoped reads over the same relation. The warm working set was 32 pages and
the streaming working set was 2,048 pages. The clean cache held 512 pages.
The warm clean and resident dirty samples returned the shared vector on
every scoped read. The suspended scan test separately verifies retirement
and release of old page versions.

| Workload | Reads | Physical reads, owned → scoped | Bytes allocated per read, owned → scoped |
| --- | ---: | ---: | ---: |
| Warm clean | 64,000 | 64,000 → 0 | 16,495 → 96 |
| Streaming clean | 16,384 | 16,384 → 16,352 | 16,493 → 8,339 |
| Resident dirty | 16,000 | 0 → 0 | 8,256 → 49 |
| Spilled dirty | 16,000 | 0 → 0 | 24,767 → 16,560 |

The physical-read column counts relation reads. Each spilled dirty read
still reads its WAL scratch slot. In a separate run with two clean readers
and one staging writer, mean WAL mutex wait per acquisition fell from
6.88 to 0.90 microseconds for readers and from 7.71 to 2.13 microseconds for
the writer. Writer median staging latency was 2 microseconds in both
samples, and the 95th percentile fell from 53 to 23 microseconds.

## Query observability

### P01 — Statement statistics

Initial memory comparison used SBCL 2.6.10.334-ab0880140 and
`t/workload-memory.py --rows 2048 --repeat 3 --runs 3 --case point_reads --case scan --case sort --case mixed`,
once with `--statement-statistics` and once without it. Medians across fresh
servers are below. The short-query optimization target is under 5%
throughput loss with collection enabled.

| Workload | Tracked calls | Allocated bytes off → on | Peak RSS bytes off → on | Post-GC heap bytes off → on |
| --- | ---: | ---: | ---: | ---: |
| Indexed point reads | 192 | 12272576 → 12894688 | 114667520 → 115367936 | 47524656 → 47562016 |
| Aggregate scan | 0 | 4230640 → 4231232 | 106733568 → 106688512 | 47496416 → 47533616 |
| Sort | 0 | 6218112 → 6218032 | 108695552 → 108695552 | 47520560 → 47550320 |
| Four-client mixed read/write | 384 | 375916656 → 381088256 | 479100928 → 484274176 | 47846768 → 47853024 |

The initial aggregate scan and sort rows had no identity coverage.
Point reads took
68.5 → 75.8 ms per sample and mixed took 1.076 → 1.094 s.
Fixed-width hashing preserved the version-one query ID. In the REPL,
hashing a prebuilt point-read shape 10000 times reduced measured
allocation from 21917424 bytes to zero for that fixnum-valued query ID.
Five fresh-server point-read samples per mode measured 12272560 bytes
off and 12471552 bytes on, with 192 calls tracked per enabled sample.
The enabled allocation premium fell from 622112 to 198992 bytes.
Median elapsed time was 69.9 → 73.1 ms, about 4.5% lower throughput.
After adding ordered SELECT fingerprints, the three-sort sample
tracked all three calls. Across three fresh servers per mode, elapsed
time was 30.9 → 31.3 ms and allocated bytes were 6217952 → 6207408.
Peak RSS and post-GC heap each differed by less than 5 KiB in this run.
After aggregate fingerprints were added, the plain aggregate scan and
grouped aggregate each tracked all three calls. Median allocation was
4230480 → 4254784 bytes for the scan and 5938592 → 5939792 bytes for
the grouped aggregate. Peak RSS differed by less than 80 KiB for each.
The benchmark also reports process CPU time and client SQL throughput,
median latency, and p95 latency. CPU time has Linux scheduler-tick
resolution. Client latency spans each simple-query protocol round trip,
including transaction commands in the mixed workload. Peak RSS is
sampled every 5 ms. GC time covers the server workload window, with
forced heap snapshots outside that window.

A fresh off/on comparison after adding join, builtin, and set-operation
identities used three fresh servers per mode and the same workload
sizes as the initial run. Each cell shows the sample median, off → on.

| Workload | Tracked calls | SQL queries/s | Median / p95 latency (ms) | Server CPU (s) |
| --- | ---: | ---: | ---: | ---: |
| Indexed point reads | 192 | 2558 → 2695 | 0.374 / 0.577 → 0.360 / 0.459 | 0.04 → 0.04 |
| Aggregate scan | 3 | 458 → 461 | 2.006 / 2.555 → 2.042 / 2.518 | 0 → 0 |
| Sort | 3 | 98.9 → 93.4 | 9.997 / 10.510 → 10.660 / 10.880 | 0.01 → 0.01 |
| Four-client mixed read/write | 384 | 702 → 704 | 4.510 / 16.230 → 4.470 / 16.280 | 0.89 → 0.88 |

| Workload | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
| --- | ---: | ---: | ---: |
| Indexed point reads | 12268192 → 12466560 | 114651136 → 115003392 | 47518976 → 47590160 |
| Aggregate scan | 4230480 → 4254784 | 106704896 → 106647552 | 47514736 → 47522832 |
| Sort | 6217952 → 6207424 | 108605440 → 108556288 | 47484640 → 47523424 |
| Four-client mixed read/write | 380327456 → 378746672 | 483598336 → 482037760 | 47848176 → 47932000 |

All samples reported 0 ms median process GC time. The point-read
allocation premium was 198368 bytes (1.6%). Sort throughput moved
in opposite directions across runs of similar size, so the three-query
samples do not establish a steady-state throughput difference.
A later warm point-read run after the portal and statistics-store
fixes measured 46486176 → 47252944 allocated bytes, 149389312 →
150274048 bytes peak RSS, and 47658752 → 47701600 bytes post-GC
heap. It used three fresh servers per mode, 2048 rows, and 768
tracked calls per enabled sample, with planning tracking off.
The allocation premium was 1.65%. A concurrent unit-suite run
makes the elapsed-time comparison unsuitable for a throughput claim.
A separate warm run without the unit suite used four fresh servers
per mode in alternating two-server groups, with 1024 lookups per
sample. Median throughput was 2741 → 2597 queries/s (5.3% lower
with tracking), median client latency 0.351 → 0.372 ms, and p95
0.454 → 0.488 ms. Allocation was 61872456 → 62937392 bytes (1.7%),
peak RSS 164784128 → 165941248 bytes, and post-GC heap
47696136 → 47721968 bytes. This run is slightly above the 5%
short-query throughput budget.
In an isolated renderer check with prebuilt point-read tokens,
writing single-digit placeholders directly reduced median CPU time
per 100000 renders from 41.4 to 38.3 ms. Allocation stayed near
32 MB. This measures only the renderer.
A subsequent alternating off/on run after the renderer change used four
fresh servers per mode and 1536 warmed lookups per sample. The medians
were 2861 → 2580 queries/s (9.8% lower with tracking), 0.339 → 0.380 ms
client latency, 0.428 → 0.461 ms p95, and 0.295 → 0.320 s server CPU.
Allocation was 92796128 → 94402856 bytes (1.7%), peak RSS was
195796992 → 197328896 bytes, and post-GC heap was 47693168 → 47689416
bytes. Each enabled sample counted 1536 calls. Enabled throughput
ranged from 2058 to 2862 queries/s. The rerun still exceeds the
throughput budget and does not isolate the cost of the renderer change.
A separate SB-SPROF CPU run sampled 8000 warmed lookups per mode at
2 ms intervals. It collected 393 samples with tracking off and 416
with tracking on. The enabled profile attributed three inclusive
samples to query fingerprinting, two to representative text, and
one to the statistics store update. The sample counts do not isolate
a single hot tracking function. Server CPU time was 1.57 → 1.66 s.
A follow-up ran four fresh servers per mode with 1536 warmed point
reads per sample. In disposable servers only, the control REPL
substituted either the template's representative string or a fixed
query ID after setup. Both substitutions kept the statement store
active, and every sample counted 1536 calls with zero evictions.
Normal tracking, fixed text, and fixed fingerprint used median
allocations of 94402856, 93930160, and 93746552 bytes. Median
throughput was 2633, 2723, and 2778 queries/s, and server CPU was
0.310, 0.315, and 0.305 s. Peak RSS differed by under 0.3 MB and
post-GC heap by under 18 KB. These substitutions estimate the
individual costs on a single query shape. They do not qualify a
general-purpose replacement, and the elapsed-time effect is larger
than the CPU-clock difference in these short samples.
Short simple-query messages now defer normalization until a statistics
entry is missing. The renderer runs outside the shard lock, then
publication rechecks the key. Extended Parse and simple messages over
4096 characters render eagerly, bounding how much scanner input a
deferred renderer can retain. The store retains only bounded strings.
A Rove fixture checks that a repeat hit skips rendering and that a
reset during rendering permits publication into the current entry.
In the first four-sample rerun with 1536 lookups per sample, enabled
median allocation fell from 94402856 to 94003488 bytes after deferring
text. Every enabled sample still recorded all calls with zero evictions.
A second, balanced-order run used four fresh servers per mode and
2048 lookups per sample. Tracking off → on, median throughput was
2679 → 2691 queries/s, median latency 0.357 → 0.361 ms, p95
0.447 → 0.454 ms, and server CPU 0.405 → 0.415 s. Measured
allocation was 123634896 → 125209416 bytes (1.27%), peak RSS
226750464 → 228210688 bytes, and post-GC heap 47734712 → 47707840
bytes. The enabled samples all recorded 2048 calls and zero evictions.
The median met the 5% throughput budget in this run. Dropping the
first pair, which contained the slowest disabled sample, yields a
3.3% throughput loss with tracking enabled.
To exercise same-entry contention, `t/workload-memory.py --rows 2048
--repeat 24 --runs 3 --case concurrent_point_reads --warmup` ran the
same 1536 lookups across four connections. With tracking off → on,
median throughput was 3747 → 3691 queries/s, client median latency
0.877 → 0.907 ms, p95 2.049 → 2.039 ms, and server CPU 0.44 s in
both modes. Allocation was 93177088 → 94836784 bytes, peak RSS
196460544 → 198004736 bytes, and post-GC heap 47819376 → 47826496
bytes. The enabled runs recorded all 1536 calls and zero evictions.
In a separate probe run, a temporary nonblocking attempt counted 18
failed immediate acquisitions of the statement-update shard mutex
across 8000 measured and 64 warmup lookups (0.22%). The probe retried
through the blocking lock path and was omitted from timing samples.
A warmed 16384-row aggregate scan exercised 32 queries in three
fresh servers per mode. The serial case used `--case scan --repeat 32`
and the four-client case used `--case concurrent_scan --repeat 8`.
Tracking counted 32 calls with zero evictions in every enabled sample.
The medians below compare tracking off → on. Median process GC time
was zero throughout.

| Clients | Queries/s | Median / p95 (ms) | Server CPU (s) | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 81.4 → 81.2 | 12.234 / 12.791 → 12.142 / 12.955 | 0.39 → 0.39 | 297240416 → 297305136 | 405155840 → 405200896 | 49483520 → 49452720 |
| 4 | 269.5 → 282.7 | 13.356 / 18.732 → 13.162 / 13.861 | 0.43 → 0.41 | 297670768 → 297683728 | 405913600 → 405893120 | 49574336 → 49669680 |

Warmed mixed read/write samples used `--rows 4096 --repeat 6 --runs 3
--warmup` with `--case mixed_serial` (one connection) or
`--case mixed` (four). Each sample ran 384 transactions with a point
SELECT and UPDATE in each, plus the transaction commands. Every enabled
sample recorded 768 calls and zero evictions. The fixture checked the
selected keys and verified the resulting group sum. The table reports
medians, tracking off → on.

| Clients | Queries/s | Median / p95 (ms) | Server CPU (s) | GC (ms) | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 393.7 → 390.1 | 2.372 / 5.083 → 2.403 / 5.076 | 2.10 → 2.09 | 2 → 2 | 1454584240 → 1455514736 | 982302720 → 982401024 | 47909936 → 47935728 |
| 4 | 587.1 → 591.4 | 4.492 / 20.858 → 4.442 / 20.460 | 3.09 → 3.06 | 2 → 3 | 1388598128 → 1387877616 | 965054464 → 965181440 | 48210240 → 48212960 |

A separate enabled serial mixed run hit the 20000-sample cap of
SB-SPROF's allocation profiler. Among the captured replenishments,
the direct caller of the allocator was tuple construction in 24.5%,
heap tuple deformation in 23.9%, and byte-to-string decoding in
25.0%. The UPDATE candidate heap scan accounted for 86.8% of the
sampled stacks. The profile covers only part of the run. It identifies
candidate scanning and tuple decoding as the main opportunities in
the roughly 1.45 GB allocated by either serial mixed mode.
Source-free UPDATE and DELETE with a sole integer column equality
on a table with variable-length columns now screen visible tuples
before full deformation. The screen uses
the heap's existing single-column demand to skip unrelated varlena
decoding. Matching rows still run the normal predicate, and a changed
target version still runs the concurrent-update recheck. The narrow
deformation still walks the row and allocates a values vector, so
the benefit is greatest when many rows with variable-length data fail
the predicate.
Repeating the serial mixed benchmark on three fresh servers per mode
gave the following medians before → after the change.

| Tracking | Queries/s | Server CPU (s) | GC (ms) | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Off | 393.7 → 559.2 | 2.10 → 1.00 | 2 → 0 | 1454584240 → 297242624 | 982302720 → 416665600 | 47909936 → 48029008 |
| On | 390.1 → 517.3 | 2.09 → 1.11 | 2 → 0 | 1455514736 → 298299824 | 982401024 → 417460224 | 47935728 → 47998304 |
| Off, four clients | 587.1 → 638.0 | 3.09 → 2.04 | 2 → 0 | 1388598128 → 231306768 | 965054464 → 335204352 | 48210240 → 48195776 |
| On, four clients | 591.4 → 643.5 | 3.06 → 2.01 | 3 → 0 | 1387877616 → 233101232 | 965181440 → 337272832 | 48212960 → 48201440 |

The four-client rerun used the same fixture and sample count with
the variable-length-column guard in place. Each enabled sample still
recorded 768 calls with zero evictions. Rove checks visibility before
screening and full values on matches. The SQL wire fixture changes
a matching row's key in another transaction and verifies that the
blocked UPDATE leaves that newer version alone.

### P02 — Execution and activity diagnostics

A point-read rerun after adding sort metadata used three fresh
servers per tracking mode and 2048 warmed lookups per sample.
Tracking off → on, median throughput was 2686 → 2658 queries/s,
allocation 123649168 → 125196048 bytes, peak RSS 226631680 →
228241408 bytes, and post-GC heap 47722912 → 47731936 bytes.
Every enabled sample counted 2048 calls and zero evictions.

The memory benchmark's `--warmup` and `--explain-mode` options run the
same read-only queries through plain SQL or JSON EXPLAIN ANALYZE. The
EXPLAIN modes are `rows` (BUFFERS OFF, TIMING OFF), `counters`
(BUFFERS ON, TIMING OFF), and `timing` (BUFFERS ON, TIMING ON).
All use statement tracking off and suppress plan estimates. Compare EXPLAIN modes directly to
see the incremental instrumentation and reporting costs. Plain SQL
can send more data, particularly when returning the full sorted result.

With `--rows 2048 --repeat 12 --runs 3 --warmup`, the medians below
compare plain SQL, rows-only EXPLAIN, counters, and counters with timing
in that order. Each point-read sample ran 768 queries. The other samples
ran 12 queries each.

| Workload | Queries/s | Client p95 (ms) | Server CPU (s) |
| --- | --- | --- | --- |
| Indexed point reads | 2837 / 2332 / 2391 / 2508 | 0.433 / 0.512 / 0.501 / 0.463 | 0.16 / 0.16 / 0.18 / 0.17 |
| Aggregate scan | 535 / 458 / 490 / 471 | 2.143 / 2.631 / 2.367 / 2.424 | 0.02 / 0.02 / 0.02 / 0.02 |
| Sort | 102 / 335 / 332 / 319 | 10.464 / 3.346 / 3.444 / 3.350 | 0.04 / 0.03 / 0.04 / 0.03 |
| Join | 121 / 116 / 117 / 114 | 8.584 / 9.159 / 9.069 / 9.376 | 0.10 / 0.10 / 0.10 / 0.11 |

| Workload | Allocated bytes | Peak RSS bytes |
| --- | --- | --- |
| Indexed point reads | 46572512 / 50843552 / 60022560 / 62583296 | 149426176 / 153718784 / 162930688 / 165478400 |
| Aggregate scan | 14396864 / 14462464 / 14586832 / 14656192 | 117219328 / 117334016 / 117493760 / 117563392 |
| Sort | 22321280 / 18374448 / 18526464 / 18562432 | 125173760 / 121114624 / 121225216 / 121303040 |
| Join | 39373616 / 39542352 / 39750064 / 39812144 | 142282752 / 142462976 / 142684160 / 142761984 |

Median post-GC heaps differed by less than 40 KiB across modes for each
workload. Median process GC time was 0 ms. Counters added 9179008
allocated bytes over rows-only EXPLAIN for the point-read sample.
SB-SPROF attributed 16.8% of sampled allocation to JSON plan
formatting. Writing the seven fixed resource labels as JSON syntax
reduced the counters sample from 60022560 to 59907248 allocated
bytes. This recovered 115312 bytes of the measured premium.
Reserving capacity once for each encoded frontend field and then
copying it in bulk reduced the same sample to 57464288 allocated
bytes and 160432128 bytes peak RSS. This recovered another 2442960
allocated bytes and 2412544 bytes peak RSS. Median elapsed time was
331 → 330 ms for the two runs after the JSON label change.
A later warmed 12-sort rerun on the projected-row path measured
18390144 allocated bytes with rows only, 18488448 with counters, and
18521024 with timing. Peak RSS ranged from 121262080 to 121372672
bytes, and post-GC heap from 47592592 to 47656672 bytes. These samples
used the default work-memory budget and overlapped the unit suite.

The same four-client point-read workload compared the EXPLAIN modes
with tracking off. Rows only, counters, and node timing respectively
used 101799968, 116449200, and 121961824 allocated bytes. Peak RSS was
205160448, 219783168, and 225198080 bytes, and post-GC heap ranged
from 47755360 to 47797392 bytes with 0 ms median GC time. Throughput
was 3947, 4118, and 4072 queries/s, median latency was 0.726, 0.669,
and 0.702 ms, and p95 was 2.106, 2.064, and 2.050 ms. Server CPU
time was 0.43, 0.42, and 0.44 s. These three samples per mode show
stepped memory costs with throughput varying across the modes.

The warmed 16384-row scans also ran all three EXPLAIN modes with
tracking off. Each series is rows only / counters / counters with
node timing, with 32 queries per sample and three fresh servers per mode.
Median process GC time was zero in all six cases.

| Clients | Queries/s | Median client latency (ms) | Client p95 (ms) | Server CPU (s) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 80.0 / 78.0 / 76.6 | 12.442 / 12.766 / 13.021 | 13.113 / 13.284 / 13.769 | 0.39 / 0.40 / 0.40 |
| 4 | 281.9 / 268.6 / 269.9 | 13.312 / 13.564 / 13.875 | 14.395 / 14.690 / 15.744 | 0.41 / 0.43 / 0.44 |

| Clients | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
| ---: | ---: | ---: | ---: |
| 1 | 297374576 / 297689728 / 297880528 | 405303296 / 405659648 / 405811200 | 49459920 / 49521088 / 49451760 |
| 4 | 297751664 / 298189360 / 298330304 | 406110208 / 406462464 / 406593536 | 49629472 / 49729072 / 49760608 |

The activity polling benchmark used 30 admitted connections on one
server, with four clients doing 1024 warmed indexed reads each. The
poller selected PID, state, wait type, wait name, query ID, and query
text at a 10 ms interval. A control returned six comparable fields
from a 30-row table. The regular Python wire client reads type, length,
and body separately from the socket. With that client, both polling
queries reduced measured throughput from about 3692 to about 1610
lookups/s. The buffered rerun received up to 64 KiB at a time and
parsed complete frames from those bytes. Both polls then ran at the
target cadence without the large throughput drop.

| Poll source | Lookups/s | Lookup median / p95 (ms) | Poll median / p95 (ms) | Server CPU (s) | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| None | 6617 | 0.553 / 0.890 | — | 1.24 | 249727392 | 358100992 | 48655536 |
| Activity | 6494 | 0.575 / 0.927 | 0.725 / 1.053 | 1.29 | 253619504 | 362053632 | 48712800 |
| Control table | 6402 | 0.581 / 0.939 | 0.746 / 1.172 | 1.29 | 253367856 | 361701376 | 48679888 |

Each mode used three fresh servers in rotated order. Median activity
and control poll counts were 62 and 63. Every sample tracked exactly
4096 lookups, and median process GC time was zero in all modes.

The final cross-mode run used the same buffered 30-connection
workload with activity polling at 100 Hz in every mode. Statement
collection was disabled, all four workers checked 1024 indexed
lookups each, and each mode ran on three fresh servers in rotated
order. Mode medians follow. Rows only, counters, and timing used
JSON EXPLAIN ANALYZE with COSTS OFF and progressively enabled
BUFFERS and TIMING.

| Mode | Lookups/s | Lookup median / p95 (ms) | Poll median / p95 (ms) | Server CPU (s) | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain SQL | 6613 | 0.556 / 0.905 | 0.665 / 1.040 | 1.24 | 250443568 | 358772736 | 48744112 |
| Rows only | 6612 | 0.529 / 0.881 | 0.621 / 1.011 | 1.15 | 273504416 | 382078976 | 48740960 |
| Counters | 6393 | 0.529 / 0.923 | 0.642 / 0.992 | 1.18 | 313242032 | 421699584 | 48674880 |
| Counters and timing | 6397 | 0.541 / 0.932 | 0.645 / 0.989 | 1.20 | 327919232 | 436260864 | 48741168 |

Median process GC time was zero in all modes. The three EXPLAIN
variants all checked the same actual row count and presence of
the requested fields. A separate disposable-server probe replaced
the blocking activity mutex acquisition at frontend begin and
end with one immediate try before falling back to the blocking
path. With 4096 lookups and 59–62 polls per sample, misses in the
four modes were 152, 133, 137, and 137, respectively, across
roughly 8310 acquisitions per mode (1.6–1.8%). These counts
cover the two frontend activity publication sites and exclude
timing samples. They do not measure lock hold durations.

## Composite range and ordered B-tree scans

The localhost wire smoke test is `python3 src/lisp/t/btree-range.py`.
It covers composite bounds, empty results, ordered LIMIT, NULL placement,
portal suspension, and rebinding. The allocation comparison used three warm
runs with 16,384 rows and 100 ordered LIMIT queries per run.

```sh
python3 src/lisp/t/workload-memory.py --case composite_range --case composite_range_seq \
    --rows 16384 --repeat 100 --runs 3 --warmup
```

| Path | Allocated bytes | Peak RSS bytes | Median SQL latency |
| --- | ---: | ---: | ---: |
| Index with eager leaf decoding | 8,642,240 | 114,536,448 | 0.486 ms |
| Index with leaf images and on-demand decoding | 7,155,280 | 113,049,600 | 0.451 ms |
| Sequential reference | 799,807,840 | 909,606,912 | 12.577 ms |

Three warm runs at 131,072 rows with 20 ordered LIMIT queries each measured
1,841,696 bytes allocated and 0.500 ms median query latency with the index.
The sequential reference allocated 1,937,763,056 bytes at 89.358 ms median
latency. Peak RSS was 111,001,600 and 1,004,949,504 bytes respectively.

One warm run at 1,000,000 rows with three ordered LIMIT queries allocated
353,280 bytes with the index and 2,216,342,848 bytes for the sequential
reference. Median query latency was 0.680 ms and 676.746 ms, respectively.
Peak RSS was 110,612,480 bytes and 1,002,242,048 bytes, respectively.

The skewed range benchmark gives one tenant half the rows and spreads
the remainder over 63 smaller tenants. It checks the same sorted query
at 0.01%, 0.1%, 1%, 10%, and 50% selectivity and LIMIT 10, 100, and
10,000. The sequential, tenant-equality, and composite-index cases use
one data set. The benchmark records the unforced plan before measuring
each controlled path, so a cost-based sequential choice remains visible.
The diagnostic pass counts candidate TIDs and distinct heap blocks in
the index stream. The sequential case visits all heap blocks. The
warm-cache allocation and time samples follow the diagnostic pass
and run with diagnostic counting disabled.

```sh
python3 src/lisp/t/btree-range-memory.py
python3 src/lisp/t/btree-range-memory.py --writer-only
```

| Selectivity and limit | Path | Candidate TIDs | Heap blocks | Rows removed | Allocation MB | Time ms |
|---|---|---:|---:|---:|---:|---:|
| 0.01%, 10 | Sequential | — | 4,425 | 999,900 | 202.58 | 625 |
| 0.01%, 10 | Tenant equality | 500,000 | 4,425 | 499,900 | 358.72 | 1,010 |
| 0.01%, 10 | Composite | 10 | 1 | 0 | 0.16 | 0.8 |
| 0.01%, 10,000 | Composite | 100 | 2 | 0 | 0.23 | 1.4 |
| 50%, 10,000 | Sequential | — | 4,425 | 500,000 | 288.46 | 963 |
| 50%, 10,000 | Tenant equality | 500,000 | 4,425 | 0 | 444.58 | 1,548 |
| 50%, 10,000 | Composite | 10,000 | 89 | 0 | 6.17 | 38 |

The equality allocation check used three warm samples of 64,000
single-column point lookups over 16,384 rows. The pre-roadmap
baseline at `280ecd87618` allocated 4,325,832,832 bytes at the
median. The roadmap worktree allocated 4,340,205,632 bytes before
the planner skipped redundant range comparisons and 4,329,950,688
bytes afterward. The change saves about 160 bytes per lookup and
leaves the measured worktree allocation 0.10% above the baseline.
Range terms and composite equality prefixes still produce range
candidates.

## Partial and expression indexes

In a warmed SBCL REPL, 10,000 parses allocated 20.9 MB for a two-column
definition and 34.0 MB for a two-key definition with a qualified function
and predicate. Neither workload increased live Lisp data after full GC.

The definition analyzer resolves result types and typmods, requires a
Boolean predicate, and collects key and predicate column dependencies.
The initial volatility checks reuse the scalar builtin registry.
PostgreSQL C and the analyzer agree on the mixed timestamp-to-date
definition and six invalid definitions, including a session-dependent
timestamptz-to-date cast. After keeping the per-expression visitor
stack-local, three warmed runs of 10,000 analyses each allocated 16.9 MB
with no increase in post-GC live Lisp data.

The catalog codec persists ordered expression keys, the predicate and
referenced attribute numbers. Ordinary definitions retain the legacy
encoding. The shared row evaluator admits only SQL TRUE and evaluates
keys in definition order. In 10,000 operations, plain and expression
catalog round trips allocated 5.4 MB and 22.2 MB respectively. Plain and
expression row evaluations allocated 0.29 MB and 0.75 MB respectively.

On a 1,200-row table, 100 composite expression lookups allocated 6.1 MB
and took 38 ms with the index, versus 34.1 MB and 121 ms after dropping it.
Planning the same statements allocated 2.6 MB and 2.2 MB respectively.

Typed parameters can supply probe values for composite expression
indexes once independent query conjuncts prove the partial predicate.
The wire test holds a scan open, verifies DROP INDEX waits, then
replaces the index with a different predicate and rebinds the same
prepared statement. Over 100 prepared lookups, the indexed path
allocated 5.6 MB and took 39 ms. Dropping the index raised this to
34.8 MB and 122 ms.

On a 1,200-row table with 1% predicate selectivity, 100 descending
LIMIT queries allocated 5.4 MB and took 38 ms through the partial
index. After dropping it, they allocated 19.3 MB and took 92 ms.
The wire suite also checks that an excluded predicate falls back and
returns the non-indexed rows.

Across three 2,000-row builds at 10% selectivity, ordinary and concurrent
creation allocated a median of 6.24 MB and 6.21 MB and took 27 ms and
39 ms, respectively. Peak RSS was about 109 MB for both and post-GC
Lisp heap was about 48 MB. Moving the standalone build onto the shared
catalog publication path reduced ordinary build allocation from 11.0 MB
to 6.24 MB. The concurrent index still enforced uniqueness after
immediate restart.

With a live native subscriber, three 200-row builds at 10% selectivity
allocated a median of 1.38 MB for ordinary creation and 1.89 MB for
concurrent creation on the source. The subscriber allocated 1.49 MB and
1.52 MB respectively. Source and subscriber peak RSS was about 105 MB
in both cases. Median time to observe the valid index on the subscriber
was 124 ms and 160 ms, including polling.

The 2,000-row logical benchmark initially waited over 90 seconds for
bootstrap. The native client now sets `TCP_NODELAY` as libpq does.
Bootstrap completed in 1.96 seconds and allocated 5.86 MB on the
source and 192.93 MB on the subscriber. Sampled peak RSS was 105 MB
and 295 MB, with about 48 MB of live Lisp heap on each server after GC.
In three runs after bootstrap, ordinary and concurrent index creation
allocated 7.28 MB and 7.57 MB on the source and 7.43 MB and 7.66 MB on
the subscriber. The logical wire fixture now copies all 2,000 rows
before exercising ordinary and concurrent index DDL.

The expression-index benchmark seeds 10,000 rows, verifies 20 warm
point lookups, then inserts 1,000 more rows. It uses the same
`abs(key)` expression and lookup SQL across sequential, full,
and partial builds. Build allocation and time include the ordinary
catalog and WAL publication. Insert WAL numbers are the change
in durable WAL position.

```sh
python3 src/lisp/t/expression-index-memory.py
```

| Index | Index KiB | Build ms | Build allocation MB | Insert WAL KiB | Insert allocation MB |
|---|---:|---:|---:|---:|---:|
| Sequential baseline | 0 | — | — | 56.6 | 16.1 |
| Full expression | 408 | 362 | 224.6 | 120.8 | 46.5 |
| Partial, 1% | 16 | 38 | 6.3 | 72.6 | 16.5 |
| Partial, 10% | 48 | 64 | 21.6 | 88.7 | 19.9 |
| Partial, 90% | 368 | 331 | 200.2 | 120.8 | 43.8 |
| Partial, 100% control | 408 | 363 | 224.7 | 120.8 | 46.5 |

The indexed lookup reported one native clean-cache hit and removed
no rows. The sequential plan reported 55 hits and removed 9,999
rows. Twenty lookups allocated 32.9 MB sequentially,
1.34 MB with the full index, and 0.95–1.34 MB with the partial
indexes. Times were 129–136 ms sequentially and 7–9 ms indexed.
The all-qualifying control holds key count and size fixed. Its build
allocated about 24 kB more than the corresponding full index build,
and its insert WAL and allocation matched the full index in this
sample. The integer predicate added little allocation at equal
coverage.

At 1% coverage over 1,000 rows with external 6,400-byte payloads,
TOAST fetches fell from 1,000 to 10 per build. Median ordinary build
allocation fell from 89.0 MB to 2.23 MB and time from 91 ms to
19 ms. Concurrent build allocation fell from 89.2 MB to 2.37 MB
and time from 104 ms to 32 ms. Peak RSS fell from about 202 MB to
112 MB in both modes. Each build produced a 16 KiB index. The
10,000-row inline control at full coverage still allocated about
225 MB and produced a 408 KiB index for both full and partial indexes.

```sh
python3 src/lisp/t/expression-index-memory.py --external
```

## Spilling hash joins

Three warmed 10,000-row inner joins allocated 4.7–4.9 MB and produced
10,000 matches each. Hashing reads key components directly from row
vectors, avoiding a temporary composite key per row. Both inputs must
present keys in the same resolved equality domain.

At the memory boundary the iterator partitions both inputs into eight
initial batches. An oversized batch that cannot split streams bounded
build blocks and replays its probe spool for each block. This retains
all duplicate matches under skew. An oversized build row gets its own
replay block.
One decoded row can coexist with a charged block, and a single row can
exceed the workspace target. Temporary files remain subject to the
query's temp-file quota.
Replay releases the initial hash table and sizes a new table for its
build blocks. The peak build counter covers retained rows and their
estimated table overhead. Hash spill now uses 512-byte executor buffers
for each input, writer, and on-disk match map. This replaces SBCL's
32 KiB per direction, plus its character input cache. The initial build
reserves space for eight writers. Repartitioning runs without a retained
build block and opens at most eight writers and one reader. Replay
reserves two readers and, for probe-preserving joins, a disk match map.
The reservations include an allowance for stream metadata. Tiny
internal test budgets exercise the requested row cap with a separate
fixed buffer allowance.
Sealed replay now reads records sequentially. In three 100,000-row-per-side
runs at 64 KiB, removing the redundant seeks reduced median runtime from
3.22 s to 2.15 s. Median allocation stayed near 428 MB and peak RSS near
538 MB. All runs returned 100,000 matches.
The first executor-buffer implementation took a median 5.76 s with
1 KiB buffers. Reducing these to 512 bytes preserved an existing
in-memory build case, and the ASCII fast path cut median spill time
to 4.46 s. The latter runs allocated 430 MB with peak RSS of 539 MB.
Using typed byte-buffer state reduced the median to 3.06 s. Writing
disk-only partitions without copying their input rows, and returning
the rows already owned by the decoder, reduced median allocation to
324 MB and peak RSS to 434 MB. Median runtime was 3.03 s. The native
streams reached 2.15 s with their buffers outside the workspace charge.
With skewed keys and external text, 100,000 probes against 96 build
rows returned 140,325 matches in every sample. The copy and buffer
changes reduced the 64 KiB median from 0.93 s to 0.64 s, allocation
from 142 MB to 114 MB, and peak RSS from 252 MB to 222 MB.
On a 10,000-by-10,000 integer join, the 4 MiB, 256 KiB and 64 KiB budgets
all produced 10,000 matches. Allocations were 5.0 MB, 24.6 MB and
21.1 MB. The two spill runs took 92 ms and 152 ms respectively.

Ten 1,200-row-per-side SQL joins allocated 11.5 MB and took 41 ms in
memory, and allocated 28.3 MB and took 155 ms at a 64 KiB budget. Writing
one equality operand as `l.a + 0` kept the equivalent query on the
nested-loop path, where ten runs allocated 2.09 GB and took 7.93 s.

SQL work_mem now supplies executor workspace budgets and supports SET,
SET LOCAL, RESET, and pg_settings reporting. Unadorned values use
kilobytes. The wire tests verify session isolation and transaction restore.
At SQL settings of 4 MiB and 64 KiB, ten joins allocated 11.5 MB and
28.3 MB respectively. Peak temporary space was zero and 183,660 bytes.

With a 100,000-byte TOAST payload at a 64 KiB workspace target, the
join returns the same count and payload length as PostgreSQL C. Ten
runs allocated 57.4 MB after reusing owned replay rows, down from
58.5 MB. Peak temporary space was 293,253 bytes.

LEFT hash joins retain unmatched and NULL-key probe rows. A spilled
partition tracks probe matches by spool ordinal and emits each unmatched
row after the final build block. Match tracking can spill independently.
The fit and spill wire workloads both matched PostgreSQL C, including
an ON residual and a post-join WHERE filter. Ten runs allocated 15.3 MB
at 4 MiB and 37.0 MB at 64 KiB, with peak temporary space of zero and
195,744 bytes respectively.

RIGHT and FULL hash joins mark build rows only after the key and ON
checks pass. An unmatched build row is emitted after its block has
scanned the full probe partition. NULL keys and disjoint partitions
receive the same null-extension and source-presence treatment.
PostgreSQL C agreed on the RIGHT and FULL wire results in memory and
during spill. Ten FULL joins allocated 21.0 MB at 4 MiB and 42.9 MB at
64 KiB, with peak temporary space of zero and 196,004 bytes respectively.

The wire suite checks early close of a suspended spill portal, DDL
admission after close, quota failure cleanup, and startup cleanup after
an immediate stop with live spill files. The iterator now unlinks each
finished partition pair before opening the next one. At 64 KiB, a
1,200-row-per-side workload used 193,260 bytes of peak temporary space.
After the first one-row portal pull, 170,220 bytes remained on disk,
and closing the portal removed them. Ten runs allocated 29.9 MB.

The wire suite also sends CancelRequest after observing spill files,
checks SQLSTATE 57014 and verifies that rollback leaves the session
usable. Disconnecting a suspended portal releases its spill files.
A suspended join continues to return its captured rows after another
session updates and deletes build-side tuples. The initial hash-table
flush checks for interrupts between rows. Repeating the ten-run spill
workload preserved the 29.9 MB allocation and 193,260-byte temp peak.

With 10,000 probe rows, 512 build rows and 64 KiB of work_mem,
uniform keys used 8 batches and 8 blocks (65,454 peak build bytes,
765,644 temp bytes, 8.2 MB allocated, 112 MB peak RSS, 52 ms).
Skewed keys used 8 batches and 8 blocks (65,518 peak build bytes,
760,216 temp bytes, 13.5 MB allocated, 118 MB RSS, 72 ms). One key
used one batch and three blocks (65,455 peak build bytes, 746,352
temp bytes, 425.3 MB allocated, 530 MB RSS, 1.48 s). Candidate and
result counts agreed between the 64 KiB and 4 MiB runs at 512,
67,698 and 5,120,000 respectively. The single-key case also
allocated 411.2 MB without spill. SBCL allocation profiling attributed
99.6% of samples to vector concatenation in hash join row assembly.
Reusing the last immutable source-presence vector reduced single-key
allocation to 247.3 MB in memory and 261.4 MB during spill. Peak RSS
fell from 516 MB to 355 MB in memory and from 530 MB to 369 MB
during spill. The candidate count, peak charged build bytes and
temporary bytes agreed with the baseline. The join still allocates
an independently owned value vector for each emitted row.

At 100,000 probe rows and 512 build rows, uniform keys returned 512
matches and skewed keys returned 675,901 at both budgets. At 64 KiB
the respective spill peaks were 7.43 MB and 7.27 MB, allocation was
74.5 MB and 107.0 MB, and peak RSS was 183 MB and 216 MB. At 4 MiB
neither spilled and allocation was 13.2 MB and 45.7 MB. The uniform
case shows the cost of writing probe rows whose keys have no build
match when their partition contains at least one build row.

Spilling joins with a non-preserved probe side use a fixed 8,192-bit
build-hash filter during partitioning. Equal join keys must share a
hash, so a clear bit rules out a match. A set bit sends the row
through the existing batch and equality checks. The initial build
reserves space for the filter, and replay restores the full build
budget. Small budgets and joins that emit unmatched probe rows
skip the filter. The wire regression checks matched rows and
temporary writes for both INNER and LEFT joins.
In the same 100,000-probe-row benchmark at 64 KiB, the filter cut
uniform temp use from 7.43 MB to 0.53 MB, allocation from 74.5 MB
to 17.8 MB, peak RSS from 183 MB to 129 MB and time from 506 ms to
69 ms. Skewed temp use fell from 7.27 MB to 5.36 MB, allocation
from 107.0 MB to 91.0 MB, peak RSS from 216 MB to 203 MB and time
from 686 ms to 564 ms. Both distributions retained their candidate
and result counts.

Run the hash-join memory workload from the checkout root. It starts
a disposable pg-lisp instance, verifies query results against counts
computed independently of the executor, and prints JSON lines with
the plan counters, spill reads and writes, peak temp bytes, elapsed
time, allocations, and RSS. The external case checks that the payload
was stored out of line. The comparison option selects the loop
paths in the disposable instance so it can use the same SQL and
memory budgets; index creation is outside the measured window.

```sh
python3 src/lisp/t/hash-join-memory.py --probe-rows 1000000
python3 src/lisp/t/hash-join-memory.py --probe-rows 1000000 --build-rows 1000000 --work-mem 4MB
python3 src/lisp/t/hash-join-memory.py --distribution skew --payload external
python3 src/lisp/t/hash-join-memory.py --build-rows 256 --compare-loops
```

The one-million-row narrow uniform run used 512 build rows and
returned 512 matches at both budgets. At 4 MiB it took 384 ms and
allocated 202.8 MB, with 319 MB peak RSS and no spill. At 64 KiB it
took 667 ms and allocated 241.6 MB, with 356 MB peak RSS, 4.75 MB
of temporary data, eight batches and blocks, and a 64,394-byte
peak build block.

With 100,000 uniform rows on each side, the fixed-partition
executor returned 100,000 matches at both budgets and wrote
14.78 MB of temp. At 4 MiB it used eight build blocks and
allocated 177 MB in 1.07 s. At 64 KiB it used 408 blocks and
allocated 2.09 GB in 14.65 s, with 980 MB peak RSS. This isolates
the cost of repeatedly reading probe partitions for large
splittable build batches.
The balanced one-million-row baseline at 4 MiB returned a million
matches, with eight spill batches and 64 build blocks. It peaked at
4,194,303 charged build bytes, 149.8 MB of temp and 992 MB RSS,
and allocated 4.42 GB in 29.23 s.

At 100,000 rows per side and 64 KiB, repartitioning kept all
100,000 matches. Allocation fell from 2.09 GB to 428 MB, peak RSS
from 980 MB to 539 MB, and elapsed time from 14.65 s to 2.90 s.
Peak temp rose from 14.78 MB to 15.70 MB while a parent and its
children coexisted. Spill batch counts now include populated parent
and child partitions, while build blocks count leaf replay work.
In this run, the 584 partitions produced 512 blocks. The temporary
I/O counters recorded 44.33 MB of spill reads and writes each.
The balanced million-row run still returned one million matches.
At 4 MiB it allocated 2.95 GB in 19.70 s, versus 4.42 GB in
29.23 s before repartitioning. At 64 KiB it completed in 37.89 s
and allocated 5.48 GB, with 4,096 build blocks and a 65,399-byte
peak build block. Peak temp was 159.1 MB at both budgets. Spill
reads and writes were 299.6 MB each at 4 MiB and 599.1 MB each
at 64 KiB. Peak RSS was about 991 MB, with about 53 MB live
after forced collection.

With the serialization buffers included in the 64 KiB workspace,
the million-row run returned all one million matches in 43.06 s.
Sizing repartition fanout reduced build blocks from 32,768 to 8,192
and allocation from 5.56 GB to 5.12 GB compared with the fixed
eight-way split at the same budget. Peak charged build bytes were
59,094, with 986 MB peak RSS and 53 MB live after collection.
Peak temp was 159.1 MB, with 748.9 MB of spill reads and writes each.

The compact spill format stores a signed 32-bit key and one source identity
in nine bytes. Other rows use the general codec. At 64 KiB the million-row
join returned one million matches in a median 4.40 s across three runs,
with 2.68 GB allocated and 955 MiB peak RSS. Spill I/O fell to 90 MB in each
direction, and peak temp fell to 19.1 MB. In isolation, one million-row
`count(*)` heap scan took a median 0.41 s and allocated 219 MB.
Decoding single-column integer tuples directly into SQL rows cut the
scan to 0.26 s and 123 MB. Computing the page checksum without copying
the page cut scan allocation further to 86 MB, with a 0.25 s median
and 190 MiB peak RSS. At the same settings the full join still took
4.11 s and allocated 2.41 GB, with 934 MiB peak RSS.
Reading non-NULL keys into reusable 256-element integer batches took
0.088 s per million-row input and allocated 37 MB in an isolated
scan. The scan verified the key count and sum in four samples under a
registered snapshot.

On the same one-million-row-per-side query, five warmed samples at
`work_mem = '64kB'` verified one million matches each. PostgreSQL C
20devel ran serially with JIT disabled and its default
`hash_mem_multiplier = 2`. The median elapsed time and largest sampled
RSS were:

| Engine | Median elapsed | Peak RSS | Spill read | Spill written |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL C | 0.198 s | 96.2 MiB, query backend | 47.7 MB | 47.7 MB |
| pg-lisp | 0.273 s | 110.7 MiB, whole server | 16.0 MB | 16.0 MB |

The Lisp run allocated 12.2 MB per warmed query, used 8.1 MB peak
temporary space, and probed one million keys across 512 build blocks.
To reproduce the Lisp case, run `python3 src/lisp/t/hash-join-memory.py
--probe-rows 1000000 --build-rows 1000000 --work-mem 64kB --samples 5`.

### Performance holdouts

`python3 src/lisp/t/hash-join-holdout.py` verifies every result and reports
CPU time, cumulative allocations, and sampled whole-server RSS. It runs
each fixture in a fresh server, warms the query once, and collects three
samples. CPU time and allocations below are the sample medians divided by
the number of executions. Peak RSS is the largest sampled process RSS
across the three samples. Each scan sample contains seven executions and
each join sample contains three.

The controlled comparison runs the same roadmap checkout with the typed
scan and count path disabled, the copy-based page checksums restored,
and hash spill records restored to text serialization with a quota
writer per record. This keeps spilling hash joins available on both
sides of the comparison. The pre-roadmap commit `280ecd87618` is a
separate reference and runs nested loops for these joins.
Run the controlled baseline with
`--control-prelude src/lisp/t/hash-join-holdout-ablation.lisp`.
Pass `typed_skew typed_sparse` to select the two large typed holdouts
outside the six default cases.

| Holdout | Allocation, MB | Peak RSS, MiB | CPU, ms |
| --- | ---: | ---: | ---: |
| 100k nullable int4 scan | 20.97 → 11.37 | 244.4 → 179.9 | 45.7 → 31.4 |
| 40k mixed int/text filtered scan | 8.75 → 8.75 | 160.3 → 160.2 | 28.6 → 27.1 |
| int4 join with sum of projected keys | 2.20 → 1.45 | 106.0 → 103.4 | 16.7 → 3.3 |
| two-column int4 join with a residual | 1.94 → 1.80 | 105.1 → 104.5 | 13.3 → 13.3 |
| text join with length aggregation | 2.49 → 2.35 | 106.7 → 105.9 | 13.3 → 16.7 |
| LEFT JOIN with 6,400-byte external text | 23.85 → 23.79 | 172.0 → 171.8 | 43.3 → 40.0 |
| 5,200 by 4,400 nullable, skewed int4 count | 15.80 → 0.70 | 145.3 → 101.6 | 113.3 → 3.3 |
| 14k by 12k nullable, sparse signed int4 count | 38.00 → 0.72 | 209.6 → 102.6 | 336.7 → 6.7 |

Against the committed baseline, the text join dropped from 207 MB and
383 ms CPU per query to 2.35 MB and about 15 ms. The wide LEFT JOIN
dropped from 8.40 GB and 2.70 s CPU per query to 23.79 MB and 43 ms.
These larger differences include the change from nested loops to hash
joins. Pass `--engine-root /path/to/baseline/worktree` to rerun that
comparison. The large typed holdouts use the controlled comparison.

CPU samples for the smaller joins were coarse because Linux exposes
process CPU in ticks. A separate 30-execution comparison isolated a
planner change that skips a hash join's ON evaluation when the hash keys
cover every conjunct. It reduced median elapsed time from 4.15 to 3.50 ms
for the int4 projection and from 14.45 to 13.75 ms for the text join,
with allocations and peak RSS within one percent. Residual conditions
keep their ordinary evaluation path. The TOAST-heavy join stayed around
43 ms. A 30-execution rerun against the full controlled baseline also
put the text join at 13.7 ms CPU versus 14.3 ms. The residual join
stayed within one CPU tick per batch, and the TOAST-heavy join fell
from 44.7 to 43.0 ms. The exact million-row workload retained both
2× stretch thresholds on rerun.

### Fixed-width join keys

The heap layout registry has ten scalar types in the 4-, 8-, and
16-byte scope. The current key contracts are:

| Width | Type | Equality key requirements |
| --- | --- | --- |
| 4 | `int4`, `date` | Signed scalar. Generic hash equality is available. |
| 4 | `oid` | Unsigned catalog scalar. SQL table DDL rejects `oid`, so it has no user-table benchmark. |
| 4 | `float4` | Canonicalize signed zero and NaNs before hashing. |
| 8 | `int8`, `time`, `timestamp`, `timestamptz` | Signed scalar. Generic hash equality is available. |
| 8 | `float8` | Canonicalize signed zero and NaNs before hashing. |
| 16 | `interval` | Canonicalize the duration used for SQL comparison, including month/day equivalents. |

The heap stores `timetz` in 12 bytes, and UUID has no registered heap
layout. Both fall outside this inventory. The fused count path handles
the signed integer and temporal domains, including interval, plus both
float widths. Float keys normalize signed zero and NaN representations
before partitioning. Integer, date, time, and both timestamp types retain
their exact heap bits.

The generic spill codec now stores wide signed integer keys with a
single source identity in 13 bytes. The existing 9-byte frame continues
to cover values in signed int32 range. With 100,000 distinct `int8`
keys per side starting at 2^40 and 64 KiB of work memory, median time
fell from 2.862 s to 0.315 s, allocation from 279 MB to 171 MB, peak RSS
from 371 MiB to 268 MiB, and spill bytes from 49.8 MB to 7.8 MB in each
direction. The comparable PostgreSQL C result was 0.020 s. The generic
path still materializes rows, so this codec change alone falls short
of the runtime and RSS targets.

The fused eight-byte path reads each key as two signed 32-bit words,
preserving the full range without boxing a SQL value. It spills eight
bytes per key and uses the same partition lifecycle as the four-byte
path. Its frequency table has 4,096 slots at 10 bytes per slot. Mixed
leaves split before they reach 2,731 build rows, so a 16-bit frequency
is sufficient. Single-key leaves multiply their full input counts.

Interval comparison uses whole days and a nonnegative subday remainder.
This represents the exact PostgreSQL comparison span even when adding
the month, day, and time fields in microseconds would exceed signed
64 bits. The private spill record uses ten bytes. The 48 KiB frequency
table, up to sixteen 672-byte split writers, and a reusable 4 KiB reader
fit within the 64 KiB workspace target for the table and buffers.
Temporary file metadata is additional. If a split sends every build
key to the same child, its next split starts at the highest bit that
varies across the build keys. This bounds redundant rewrites for skewed
keys that share the mixed 64-bit hash.

Run `python3 src/lisp/t/hash-join-fixed-width.py --reference-bin /path/to/pg/bin`
to reproduce the one-million-row-per-side comparisons. Pass type names
to select cases. The harness warms each query, checks the input and
result counts, and emits timings, CPU, memory, and the hash plan as JSON.
The runs use 64 KiB work memory and serial execution. C uses its default
hash memory multiplier and JIT is disabled. The reference build was
PostgreSQL C 20devel. Times are medians of three
Lisp and five C executions. RSS is the largest whole-server sample for
Lisp and the largest query-backend sample for C.

| Key | Lisp s | C s | Time ratio | Lisp MiB | C MiB | RSS ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `int4` | 0.271 | 0.292 | 0.93× | 117.4 | 93.1 | 1.26× |
| `int8` starting at 2^62 | 0.338 | 0.202 | 1.67× | 111.8 | 93.2 | 1.20× |
| `float4` | 0.286 | 0.205 | 1.40× | 117.0 | 93.3 | 1.25× |
| `float8` | 0.380 | 0.207 | 1.84× | 118.0 | 93.5 | 1.26× |
| `date` | 0.268 | 0.194 | 1.38× | 110.7 | 93.4 | 1.19× |
| `time` | 0.343 | 0.204 | 1.68× | 118.0 | 93.4 | 1.26× |
| `timestamp` | 0.355 | 0.205 | 1.73× | 118.1 | 93.2 | 1.27× |
| `timestamptz` | 0.356 | 0.205 | 1.74× | 118.1 | 93.1 | 1.27× |
| `interval` | 0.420 | 0.221 | 1.90× | 112.2 | 109.1 | 1.03× |

The float fixtures vary the significand across both signs. The time
fixture spans almost 24 hours, and both timestamp fixtures start in
year 280000, beyond SBCL's signed fixnum range in epoch microseconds.
The interval fixture uses two billion base months and represents each
additional 32 days as one month plus two days on one side. All million
keys match despite the different stored fields. Packing the key and
reusing the reader reduced allocations from 24.2 MB to 13.6 MB and
spill traffic from 48 MB to 40 MB in each direction.

The holdout comparison used `a06eadacd44` as the baseline. Run
`python3 src/lisp/t/hash-join-holdout.py --repeats 20 int4_projection int4_residual text_join wide_left typed_skew typed_sparse int8_rows int8_residual`
and add `--engine-root` for a baseline checkout. Allocation and CPU are
medians per query across three batches. RSS is the highest sample over
the three batches and includes the whole Lisp server.

| Holdout | Allocation MB, before → after | Peak RSS MiB, before → after | CPU ms, before → after |
| --- | ---: | ---: | ---: |
| `int4_projection` | 1.43 → 1.43 | 126.45 → 126.48 | 3.5 → 3.5 |
| `int4_residual` | 1.77 → 1.77 | 133.04 → 133.27 | 14.5 → 14.0 |
| `text_join` | 2.33 → 2.33 | 143.79 → 143.84 | 14.0 → 14.0 |
| `wide_left` | 22.63 → 22.63 | 553.38 → 553.39 | 43.5 → 44.5 |
| `typed_skew` | 0.69 → 0.69 | 112.39 → 112.59 | 3.0 → 3.0 |
| `typed_sparse` | 0.70 → 0.70 | 113.63 → 113.83 | 4.5 → 4.5 |
| `int8_rows` | 3.13 → 2.74 | 159.10 → 151.52 | 17.5 → 7.0 |
| `int8_residual` | 1.98 → 1.98 | 137.13 → 137.36 | 16.0 → 16.0 |

The spill codec improvement carries over to row-producing joins on
single-column wide integer tables. The residual fixture spills wider
rows and continues to use the general frame. The other holdouts show
small variations and keep their prior execution paths.

### Random varchar primary-key reads

Run `python3 src/lisp/t/varchar-point-read.py --reference-bin /path/to/pg/bin`
for the idempotency-key workload. The fixture inserts one million unique
URL-safe ASCII keys, 60 to 127 characters long, into a `varchar(127)`
primary key. Each warmed batch checks 20,000 seeded random lookups over
one persistent connection. Hits fetch and verify an integer payload.
Misses use keys generated outside the inserted row domain.

The first allocation profile showed a heap scan for the bound equality.
The planner now uses the existing runtime probe for a compatible
single-column index. Bind values alone cannot prove a partial-index
predicate. In the next profile, decoding internal separator entries
accounted for 32% of sampled allocation. Plain separators are now
compared in place. The equality cursor still includes the child before
the first equal separator, where duplicates can survive a split.

| Prepared batch | Hit median s | Miss median s | Hit peak RSS MiB | Hit allocation per lookup |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL C 20devel | 2.946 | 2.701 | 153.6 | unavailable |
| Lisp with bind equality probe | 7.758 | 7.244 | 936.9 | 95.1 kB |
| Lisp with in-place separators | 7.176 | 6.972 | 940.1 | 63.1 kB |
| Lisp with one-plan Bind and startup GC cap | 7.662 | 7.082 | 253.8 | 51.0 kB |
| Lisp with binary equality searches | 6.354 | 5.829 | 254.1 | 51.1 kB |

RSS is the C query backend or the whole Lisp server. The startup GC
cap bounds SBCL's allocation interval to 128 MiB and preserves a lower
runtime-selected value. The uncapped interval on the measured build was
about 819 MiB. The one-plan Bind validates the Parse signature against
the execution plan and admits relation readers before capturing the
read-committed snapshot. This preserves the Bind-time view when a
concurrent TRUNCATE holds the relation. With planning tracking enabled,
a completed plan can be counted even if Bind subsequently rejects
the result descriptor or format. No execution is counted in that case.

The final equality cursor binary-searches ordered leaf keys and
internal separators. It includes the preceding child at an equal
internal separator and scans the equal run. A probe of an unused slot
falls back to linear traversal. Each million-row timing used five
samples after warming, with every result checked. The harness confirms
that the prepared query opens an equality index cursor before timing.

Using the faster C baseline above, Lisp hits were 2.16× C elapsed and
1.65× C peak RSS. Misses were 2.16× elapsed and 1.64× peak RSS. Median
server CPU was 3.25 s for Lisp hits and 2.91 s for misses, versus
0.50 s and 0.39 s for C. Lisp allocated 40.3 kB per miss. A second fresh
C run on the shared host measured 4.166 s for hits and 3.910 s for
misses with similar peak RSS. The faster denominator gives the
conservative target comparison.

For holdouts, use `--rows 50000 --reads 3000 --samples 3` with the
options below. Add `--engine lisp --engine-root /path/to/baseline` to
compare an earlier checkout. The unprepared baseline was `a06eadacd44`.
Each table cell reports median elapsed seconds, median server CPU seconds,
and peak RSS MiB. Allocation is Lisp kilobytes per lookup.

| Holdout | Lookups | C elapsed / CPU / RSS | Lisp elapsed / CPU / RSS | Lisp kB |
| --- | --- | ---: | ---: | ---: |
| `--no-prepared` | hits | 0.591 / 0.15 / 40.1 | 1.156 / 0.57 / 229.0 | 43.3 |
| `--no-prepared` | misses | 0.595 / 0.15 / 40.1 | 1.123 / 0.52 / 201.4 | 33.8 |
| `--min-length 60 --max-length 60` | hits | 0.507 / 0.09 / 36.0 | 1.086 / 0.54 / 211.7 | 37.3 |
| `--min-length 60 --max-length 60` | misses | 0.471 / 0.09 / 36.0 | 1.013 / 0.47 / 185.0 | 28.0 |
| `--min-length 127 --max-length 127 --key-type text` | hits | 0.498 / 0.07 / 43.6 | 1.080 / 0.56 / 232.8 | 44.6 |
| `--min-length 127 --max-length 127 --key-type text` | misses | 0.478 / 0.07 / 43.6 | 0.955 / 0.48 / 203.9 | 34.6 |

Unprepared hits went from 66.0 to 43.3 kB per lookup, 294.5 to
229.0 MiB peak RSS, and 0.61 to 0.57 s CPU per batch. The elapsed median
rose from 1.045 to 1.156 s on this shared host. The smaller fixtures
produce a much smaller C backend RSS, so their whole-server RSS ratios
are higher than the primary comparison.

### Other join paths

Re-run the loop cases with
`python3 src/lisp/t/hash-join-holdout.py nested_cross nested_full lateral_index parameterized_index`.
The reference checkout was `976722fac52`. Allocation and CPU are medians
per execution, and RSS is the largest whole-server sample across three
batches at 64 KiB.

| Holdout | Allocation, MB | Peak RSS, MiB | CPU, ms |
| --- | ---: | ---: | ---: |
| Cross join with 48k projected pairs | 23.01 → 21.48 | 213.3 → 201.6 | 84 → 84 |
| Full nested loop with inequality | 6.94 → 6.47 | 132.3 → 130.0 | 22 → 24 |
| Indexed LATERAL with 800 outer rows | 411.27 → 5.95 | 921.1 → 139.6 | 1,430 → 22.9 |
| Parameterized index join with 800 probes | 30.01 → 4.52 | 300.4 → 130.1 | 28.6 → 20.0 |

With 20 executions per sample, cross-join CPU moved from 87.5 to
89.5 ms, and inequality full-join CPU moved from 24.5 to 24.0 ms.
Their allocation reductions persisted.

The correlated subquery uses the bound outer column as a runtime index
equality. Forward full-key equality ranges use the point cursor, and
point cursors retain only matching TIDs from each leaf. Heap lookup
deforms tuple bytes directly from the borrowed page. Nested loops and
LATERAL reuse their last immutable source-presence vector across pairs.
The PostgreSQL C differential also checks both predicate orientations
for a correlated indexed probe with NULL outer keys and an inner filter.

The row-producing hash join holdouts covered integer projection, an
integer join with a residual, text equality, and LEFT, RIGHT and FULL
joins. In separate 20-execution samples, the narrow cases stayed within
1 MiB peak RSS and 1 ms CPU per execution of the baseline. The
external-text LEFT JOIN allocated 22.63 MB per execution versus 23.75 MB
and peaked at 553 MiB versus 572 MiB. Its median CPU was 44.0 ms versus
42.5 ms. The original million-row count still met the stretch thresholds
at 0.290 s and 111 MiB in five warmed samples.

In the same-SQL comparison with 10,000 probe rows and 256 build
rows, all three plans returned 256 rows. At 64 KiB, the hash join
processed 256 candidates in 7 ms and allocated 2.0 MB, with 37 kB
of temp and 106 MB peak RSS. The parameterized loop used 256
index probes in 9 ms and allocated 9.3 MB, with no temp and
113 MB peak RSS. The plain loop rejected 2,559,744 pairs in 6.98 s
and allocated 823.7 MB, with 739 kB of temp and 928 MB peak RSS.
At 4 MiB the three times were 4 ms, 10 ms and 6.87 s.

With 10,000 skewed probe rows and 96 build rows carrying external
6,400-byte text, ANALYZE reported a 46-byte mean physical build
tuple. Both budgets returned 14,069 matches and the expected
maximum payload length. At 4 MiB, the hash join used one build
block, took 18 ms and allocated 11.1 MB, with 117 MB peak RSS.
At 64 KiB it used eight batches and 51 build blocks, took 102 ms
and allocated 32.8 MB, with 139 MB peak RSS and 859 kB temp.
With the same 96 external build rows and 100,000 probes, skewed
keys returned 140,325 matches at both budgets. At 64 KiB they used
75 build blocks, 3.46 MB peak temp and 142 MB of allocation in
0.63 s. A single key returned 9.6 million matches at both budgets.
At 64 KiB it stayed in one spill batch with 48 build blocks,
51,810 peak build bytes, 7.62 MB peak temp and 3.29 GB allocated
in 17.50 s. At 4 MiB the same output allocated 1.41 GB in 4.25 s
without spill.

The planner chooses the build side using estimated retained bytes.
ANALYZE records the mean physical width of visible tuples during its
existing scan. The planner adds entry and row-container overhead and
uses TOAST heap pages as a proxy for external payloads. Older statistics
fall back to heap pages and estimated rows. The estimate guides
orientation. The iterator still charges each decoded row against
work_mem.

When the estimated build exceeds work_mem, EXPLAIN charges startup
for scanning and partitioning both inputs. Total cost also includes
reading the spilled data. The cost uses estimated input bytes as a
page proxy and models one write and read pass, following the initial
hash join cost model in PostgreSQL C. For an unordered
constant LIMIT over a bare integer inner equality, the planner can
choose a lazy nested loop when ANALYZE statistics predict that the
requested matches cost less than hash spill startup. ORDER BY,
post-join filtering and whole-input operators leave the existing
selection rule in place.

On a 10,000-by-10,000 uniform integer join with LIMIT 1 and 64 KiB,
the median five-execution sample allocated 0.29 MB and took 2.6 ms.
Forcing the same query onto a hash join allocated 45.1 MB and took
291 ms. The hash plan peaked at 1.57 MB of temp and 150 MB RSS.
The nested loop used no temp and 104 MB peak RSS. Each sampled
execution verified that the returned keys matched.

The iterator restores SQL column order and source presence before
evaluating residual ON conditions and emitting rows. Wire cases cover
all four join types with the left input built, including a spilled
FULL join. EXPLAIN JSON identifies the build side.

The width regression leaves dead pages in a narrow table, restarts
after ANALYZE, and verifies that the planner builds the narrow input
with both inline and external wide payloads on the probe side. Eight
INNER, LEFT, RIGHT and FULL cases matched PostgreSQL C. On ten joins
of 100 rows with 1,500-byte text against 400 integer rows at 128 KiB,
the row-count choice allocated 37.7 MB, took 65 ms and used 186,080
temporary bytes. Selecting by width allocated 8.4 MB, took 15 ms and
used no temporary space. Peak RSS fell from 144 MB to 112 MB.

For ten 200-by-1,201-row INNER joins at 64 KiB, the smaller build
allocated 4.4 MB and took 29 ms. Changing only the estimates to force
the larger build allocated 16.3 MB and took 107 ms. Peak temporary
space was zero and 119,010 bytes respectively.

## Planner statistics and costing

The same harness accepts `--analyze-memory` and an optional
`--engine-root /path/to/baseline`. For batches of 15 ANALYZE calls
on 36,000 rows, allocation fell from 45.65 to 44.56 MB, median CPU
from 390 to 380 ms, and peak whole-server RSS from 148.7 to 143.6 MiB
relative to `153c1ed0f5d`.

Run the same harness with `--range-planning-memory` or
`--join-planning-memory` for batches of 600 EXPLAIN calls. Against
`153c1ed0f5d`, range allocation rose from 15.59 to 18.36 MB, peak
RSS from 114.2 to 116.8 MiB, and median CPU stayed at 90 ms. The
allocation profile was dominated by JSON rendering and plan
bookkeeping. Range interpolation counts full buckets as integers
and only computes a fraction for the bin containing the constant.
Typed join allocation rose from 26.09 to 26.41 MB, peak RSS from
124.3 to 124.7 MiB, and median CPU from 100 to 110 ms.

Standalone scan selection and EXPLAIN now charge filter CPU per input
row using the same function. EXPLAIN uses the fractional candidate
estimate for index recheck cost and caps residual filter output at
the index input estimate. Filter selectivity groups predicates by
column. NULL tests on a range column share the same non-NULL mass,
and predicates on separate columns assume independence. A sequential
filter can estimate a comparable literal outside the indexed key
width. The differential checks a three-conjunct mixed filter, an
out-of-range bigint bound on int4, and a displayed 0.75 cost increment
for rechecking 300 rows against one predicate.

For ANALYZE allocation holdouts, use `--analyze-memory --sample-kind float`
or `--analyze-memory --sample-kind numeric`. In the 15-call float
batch, allocation fell from 50.47 to 49.14 MB, CPU stayed at 390 ms,
and peak RSS rose from 149.8 to 152.0 MiB. The numeric batch went from
48.92 to 47.41 MB and 148.2 to 147.0 MiB, with CPU at 400 ms. The
current 600-call range-planning batch used 18.46 MB, 117.0 MiB peak
RSS, and 100 ms CPU.

Run the same harness with `--execution-memory` to reproduce the
end-to-end holdouts below. Use `--engine-root` for the baseline
`153c1ed0f5d`. Each batch checks every result, records selected plan
nodes, and reports elapsed time, CPU, allocation, and whole-server RSS.

| Holdout | Calls per batch | CPU ms, before → after | Allocation MB, before → after | Peak RSS MiB, before → after |
| --- | ---: | ---: | ---: | ---: |
| Narrow integer range | 200 | 550 → 50 | 110.01 → 10.34 | 204.27 → 109.49 |
| Integer range with residual expression | 200 | 560 → 50 | 110.39 → 10.73 | 204.52 → 109.29 |
| Numeric equality hash join | 30 | 50 → 50 | 16.21 → 16.21 | 114.46 → 114.56 |

## Transactional catalogs

Catalog object addresses include the database, relation OID and column
attribute number. Staged indexes validate their owner and key columns on
insertion and again before publication, without allocating an address per
key. The 100-cycle implicit catalog workload allocated 78.7 MB with these
checks versus 78.8 MB in the prior run.

Retaining those identities reuses the dependency records from staging
and allocates no history for tables without sequence references. The
20-cycle SQL benchmark stayed at 85.4 MB with capture disabled and
89.6 MB with capture enabled, with about 48 MB live after full GC.
In a separate 20-cycle workload that created and dropped a table with
a sequence default in each transaction, allocation was 0.79 MB locally
and 1.67 MB with logical capture, with about 48 MB live in both cases.

The native protocol can create a `pg_lisp_transactional` slot and
requires `transactional_ddl 'true'` before streaming it. The slot
retains its plugin and confirmed position across restart, using the
existing slot file format. Standard publication streams reject the
private option. The capability requires modern slot provenance.
The wire fixture confirms initial copy with the binary bytea profile,
rejects mismatched START options in both directions, and resumes the
confirmed slot after an immediate restart. In a 10,000-round-trip
PGSLOT6 codec benchmark, the legacy and transactional images allocated
95.7 MB and 90.3 MB and took 112 ms and 116 ms, respectively. The sample
slot image grew by 14 bytes.

Across three runs of 20 create/insert/index/update transactions, with
50 rows per transaction, ordinary publication allocated 85.4 MB and
opt-in capture allocated 89.2 MB. Peak RSS was 194 MB and 199 MB, with
about 48 MB of live heap after full GC in both cases. Median elapsed
time was 484 ms and 520 ms. The local-only path skips replay AST
construction and avoids an extra catalog lock acquisition.

For a separate 200-row subscriber workload with 20 alternating index
creates/drops and row updates, issuing DDL as standalone commands
allocated 77.6 MB on the source and 79.0 MB on the target. Combining
each pair into one transaction allocated 118.1 MB and 112.7 MB,
with median end-to-end time of 587 ms versus 1,050 ms. Post-GC live
heap was about 48 MB on both servers. Streaming the SQL frame for
large apply payloads reduced sender allocation from 171.7 MB to
25.6 MB over twelve 1 MiB payloads in a REPL sink benchmark. It
increased CPU time for those large messages, so smaller payloads
keep the inline query path.

With the commit-wide check enabled, the same mixed workload allocated
120.3 MB on the source and 113.6 MB on the subscriber and took a median
605 ms. The schema footer transmits unchanged sections once. In a
1,000-table serialization benchmark with one added index, this reduced
each footer from 294 KB to 147 KB and allocation over 30 encodes from
97.7 MB to 52.2 MB.

After enabling durable slot negotiation, the live mixed workload
allocated 120.3 MB on the publisher and 113.6 MB on the subscriber,
took 601 ms, and retained about 48 MB of Lisp heap on both after GC.

Wire tests cover cross-backend visibility, rollback, committed uniqueness,
and immediate restart. Crash tests cover the interval before the terminal
record and the interval after WAL flush but before primary fork creation.
On a disposable server, 100 create/insert/create-index transactions
allocated 12.1 MB when rolled back and 77.4 MB when committed and followed
by an autocommit drop. After full GC, live Lisp data changed by +124 KB and
−22 KB respectively. RSS grew by 12.2 MB and 82.5 MB before the final GC.
In a separate cold-server comparison with identical SQL operations, the
explicit transaction reduced allocation from 84.8 MB to 77.4 MB and elapsed
time from 4.30 s to 2.20 s versus committing each statement separately.

Transactional CREATE TABLE stages the empty indexes for column and table
PRIMARY KEY and UNIQUE constraints. They enforce writes in the owning
transaction and recover with the table. In 100 create/insert/drop cycles
with two constraints, the explicit transaction allocated 105.1 MB and
took 2.42 s, compared with 122.5 MB and 3.41 s for autocommit. Post-GC live
Lisp data changed by −77 KB and −4 KB respectively.

Eligible multi-statement Simple Query messages stage table and ordinary
index DDL alongside row writes and TRUNCATE. A later statement error rolls
back the block. The commit and rollback cases also pass against PostgreSQL C.
Messages containing utilities outside the implicit-block allowlist, such
as CREATE SEQUENCE, still use the existing per-statement path.
In 100 create/insert/index/drop cycles, the implicit
message allocated 78.8 MB and took 1.95 s, compared with 79.0 MB and
2.01 s for the corresponding explicit transaction. Post-GC live Lisp
data changed by −43 KB and −47 KB respectively.

Mixed logical DDL uses the transactional native slot and its schema
contract. Admission checks slots when staging a catalog command and
again under the WAL lock before commit. The commit check remembers
effective staging even when create and drop cancel out in the final
catalog. A wire regression starts a standard slot during such a
transaction and verifies that commit fails and the original table's
rows stay unchanged. Without slot interest, the same local sequence
commits on the existing row-only path. In three warmed samples of 20
such transactions, median allocation was 3.75 MB, peak RSS was
107 MB, and elapsed time was 132 ms. The workload verified all
durable row inserts and the same catalog generation after the run.
With a standard slot present, four conditional DDL no-ops can commit
alongside an INSERT. The corresponding effective commands still fail
admission. Across 20 warmed transactions, adding the four no-ops raised
allocation from 5.63 MB to 5.99 MB. Both workloads took about 99 ms,
peaked at about 109 MB RSS, and preserved the catalog generation.

## Common stored types

The tuning pass measured 3,200 indexed probes per scalar type and 400
containment scans per integer array type. Shared array decoding and
comparison changes reduced CPU on both array holdouts from 1.15 and 1.11 s
to about 1 s each. The workloads still exceeded the 3× C targets. Native
CPU was about 3.5–4× for indexed probes and 11× for array scans. Peak
process RSS was about 9–11× the C session backend's RSS. The native runs
allocated about 90–102 MB per indexed workload and 537 MB per array workload.

## Column schema evolution

In three 36,000-row runs, reusing the current heap page during index builds
saved about 300 MB of allocation for both rewrite conversions. The integer
and text rewrites still used about 2.7 and 2.6 GB of allocations respectively.
Peak RSS was about 411 MiB for the Lisp server and 29 MiB for the C session
backend. Both rewrites remained above the 3× CPU and RSS targets.

## Foreign keys

  The tuning pass shares a registered visibility snapshot across immediate
  checks within each statement. Median allocations for 1,000 child inserts
  fell from about 16.7 to 16.4 MB for integer keys and from 18.4 to 18.2 MB
  for text keys. Median server CPU was 97 and 99 ms versus 2.1 and 2.2 ms for
  C PostgreSQL. Deleting one referenced row with CASCADE took 1.8 and 2.3 ms
  versus 0.41 and 0.32 ms. Peak RSS was about 411 MiB for the Lisp server
  process and 18–21 MiB for a C backend. These workloads missed the 3× targets.

### P10 performance follow-up

Use [the FK performance harness](t/foreign-key-performance.py) to measure
server CPU, peak RSS and allocations against C PostgreSQL. Target at most 3×
C server CPU, with 2× as a stretch goal. For the compact SBCL runtime, use
provisional fresh-server RSS ceilings of 75 MiB for fixed-width keys and
85 MiB for `varchar(127)` keys with 60–127 ASCII characters.

The compact profile was tested with SBCL revision `ab0880140`, built from its
checkout with `sh make.sh --dynamic-space-size=512Mb --prefix="$prefix"` and
installed with `sh install.sh`. Build the pg-lisp core from the PostgreSQL
checkout and use its launcher for the benchmark.

```sh
export SBCL_HOME="$prefix/lib/sbcl"
profile_dir="$(mktemp -d)"
python3 src/lisp/t/build-performance-core.py "$profile_dir/runtime" --sbcl "$prefix/bin/sbcl"
export SBCL="$profile_dir/runtime/sbcl"
for suite in primary width work holdout
do
  python3 src/lisp/t/foreign-key-performance.py --postgres-bin "$c_postgres_bin" \
    --suite "$suite" --isolate-cases --samples 9 --skip-allocation > "$profile_dir/$suite.jsonl"
done
```

Set `c_postgres_bin` to the directory containing the reference PostgreSQL
binaries. Each isolated case starts a fresh server per engine and reports
per-statement CPU samples and process high-water RSS in JSONL. Compare the
median CPU for each engine and phase. Repeat without
`--skip-allocation` to collect Lisp allocations through the control socket.
For the controlled CPU comparison, run both engines on the same CPU using
`taskset -c "$cpu"` before the harness command.
The compact profile preloads pg-lisp with reduced debug metadata and uses a
72 MiB dynamic space and a 7 MiB nursery. It is optional for regular
development.

- [x] Reduce sparse CASCADE CPU across fixed-width keys. Test isolated point
  deletes on integers, UUID and temporal types and range deletes on
  `date_reverse` and `composite_reverse`. Preserve snapshot visibility and
  concurrent rechecks.
- [x] Bring long ASCII `varchar(127)` inserts and cascades within the CPU target.
  Exercise both short distinguishing prefixes and the long shared prefixes in
  `varchar_prefix_reverse`.
- [x] Bring mixed interval inserts and cascades within the CPU target in
  `interval_mixed_reverse`. Check fractional input, mixed signs and calendar
  units alongside the existing single-unit interval workload.
- [x] Verify the Boolean `bool_reuse` cascade reaches the CPU target consistently
  and retains its allocation reduction across repeated runs. Verify savepoint
  rollback, concurrent child updates, and recovery for page-batched cascades.
- [x] Verify CPU and RSS across the supported key types up to 16 bytes and long
  ASCII `varchar(127)` keys. Run the promoted cases with `--suite work` and
  validate generalization separately with `--suite holdout`, which covers
  permuted nullable composite rows, alternating varchar lengths, and
  fractional timestamp expressions.

The final Linux x86-64 sweep used 11 samples per case, pinned both engines to
one CPU, and repeated the holdout suite three times in fresh server processes.
Across the 21 supported cases and nine additional holdout runs, the maximum
median was 2.89× C CPU for inserts and 2.49× for CASCADE. Fixed-width cases
peaked at 74.93 MiB RSS and long varchar cases at 74.90 MiB. The harness
reports OID separately because pg-lisp does not support it as a stored column.

In the separate allocation pass, the median for 1,000 FK inserts was 2.69 MB
for integers and 5.30 MB for varchar. The 500-row Boolean cascade allocated
0.99 MB. The live fixture also verifies blocked parent changes, savepoint
rollback, concurrent child updates, and crash recovery.

## Views

The first P11 performance pass used 12,000 rows per type, seven CPU samples,
and the preloaded 72 MiB dynamic-space runtime. Run
`python3 src/lisp/t/view-performance.py --postgres-bin "$c_postgres_bin" --suite primary`
for direct, ordinary-view and nested-view scans. Use `--suite holdout` for
the join and ordered LIMIT cases. Both suites cover int4, int8, UUID and
127-byte ASCII varchar. Use `--skip-allocation` for the pinned CPU and RSS pass.

Moving an entirely pushed predicate out of a single-source caller reduced
nested-view median CPU from 12.2–15.7 ms to 9.8–13.0 ms across the four types.
The resulting ordinary and nested scans still used 26.9–32.9× C CPU, while
the direct scans used 24.0–26.3×. The holdout used 5.9–11.3× for ordered LIMIT
and 20.9–25.2× for joins. Peak RSS was 87.1 MiB against 25.7 MiB for C.
Nested scans allocated 2.78–3.37 MB for fixed-width types and 9.15 MB for
varchar. The view and underlying scan costs both need further work.

Reusing the input row for identity projections reduced nested-view CPU by
10–18% and allocations by 8–28% across the four types versus the predicate
pushdown pass. The join holdout improved by 11–14% and ordered LIMIT by
4–11%. The ordinary UUID view was flat in this run.
The ordinary and nested scans used 21.3–27.6× C CPU, and peak RSS across
both suites was 87.2 MiB.

CPU profiling attributed 15–16% of samples to generic SQL comparison
dispatch in direct and nested integer scans. Resolving the comparison
operator and column slot once per iterator reduced ordinary-view median CPU
by 21–29%, nested-view CPU by 18–28%, and join holdout CPU by 11–21% across all
four types. Ordered LIMIT did not improve. Ordinary and nested view scans
still used 17.8–21.2× C CPU.

Choosing the integer datum comparator at scan initialization saved another
6–7% on int4 and int8 view scans. Text and UUID kept the generic comparator
after direct dispatch failed to produce a consistent improvement. The
integer join holdout split, with int8 10.5% faster and int4 2.2% slower in
the measured run.

Sequential scans that need neither TOAST fetches nor datum provenance can
return decoded values directly, avoiding heap tuple objects and redundant
header reads. The compact-runtime comparison improved ordinary views by
7–32%, nested views by 12–22%, the join holdout by 7–31%, and ordered LIMIT
by 6–13%. Direct scans were mixed, including roughly 10% increases in the
UUID and varchar cases. Relative to the previous allocation sweep, view
and nested-view allocations fell 43–58% for fixed-width values and roughly
14% for varchar. Ordinary and nested views still used 13.9–16.7× C CPU,
with peak RSS of 87.0 MiB for Lisp and 25.7 MiB for C.

Removing the redundant outer-loop interrupt check while a heap page reader
is active reduced ordinary-view CPU by 2–8%, nested-view CPU by 1.5–4.3%,
and ordered LIMIT by 1–4%. Join CPU improved in three holdouts and increased
4% for int8. Per-line-pointer checks and checks at page transitions remain
in place. Ordinary and nested view scans still used 13.3–16.4× C CPU.

Precomputing fixed-width tuple offsets and readers saved 18–20% on both
ordinary and nested int4, int8, and UUID views. The fixed-width join and
ordered LIMIT holdouts improved 11–21% and 8–9% respectively. The varchar
view scans were flat. With this change fixed-width views used 10.9–11.9×
C CPU, compared with 15.4–15.7× for varchar.

Decoding verified inline ASCII heap text into base strings dropped varchar
view allocations from roughly 7.25 MB to 2.65–2.68 MB per scan. Varchar CPU
fell about 13% for ordinary and nested views and 10% for ordered LIMIT.
The varchar join improved about 2%, and fixed-width results were mixed.
Mixed-width layout caching alone did not produce a consistent benefit.

SELECT scans that retain compressed-datum provenance now skip tuple identity
fields after the visibility check. Direct-scan median CPU fell 6–11% across
the four types. View and holdout timings were mixed. The initially slower
int8 join did not repeat in a pinned fifteen-sample run, which measured
5.60 ms versus 5.78 ms before the change.

Passing decoded values and compression provenance directly through the
sequential SELECT iterator removed the temporary heap tuple object.
Direct-scan allocation fell to 0.85 MB for either integer width, 1.41 MB
for UUID, and 2.64 MB for varchar. Direct CPU improved for the fixed-width
types, while varchar and view/holdout CPU results were mixed.

Sequential page scans now reuse a committed insertion's visibility for
undeleted rows with the same insertion XID, snapshot, and XID reference.
Fixed-width nested views improved 4–8%, and the join holdouts improved
4–17% across all four types. Varchar direct and ordinary-view CPU improved
about 4–5% in a pinned repeat, but varchar nested-view CPU increased 7%
in that repeat after falling 6% in the first sweep.

The sequential scan now uses a page-local row reader. The outer scan
iterator owns the page handle and visibility snapshot. Median ordinary
and nested view CPU fell 4–8% for integers, while UUID changed by up to
3% and varchar results varied by query. The bounded and join holdouts
changed by at most 5%. The compact run measured 4.7–15.2× C CPU and
3.4–4.0× C peak RSS.

The allocation sweep on the same runtime measured 0.85–1.47 MB per
fixed-width primary query and 2.64–2.65 MB per varchar primary query.
Join holdouts allocated 2.44–4.22 MB, and ordered LIMIT holdouts allocated
3.86–5.63 MB.

Executions that omit plan instrumentation now use the underlying iterators
directly. In a pinned seven-sample comparison, nested views used 10–16% less
CPU, join holdouts 10–17% less, and ordered LIMIT holdouts 2–18% less.
Ordinary integer views improved 12–13%, UUID was flat, and varchar rose 3%.
Across both suites, Lisp used 4.6–13.5× C CPU and 3.4–4.0× C peak RSS.

The cooperative interrupt check now reads the backend once and calls the
delivery handlers only when their flags are set. A pinned seven-sample pass
used 7–24% less CPU for ordinary views and 8–17% less for nested views.
Integer and UUID joins used 9–19% less CPU, varchar joins were flat, and
ordered LIMIT results moved by at most 4%. The pass measured 4.4–12.9× C
CPU and 3.4–4.0× C peak RSS across both suites. Repeating the allocation
sweep put each workload family within 0.02 MB of its previous range.

Sequential and index scans now resolve reordered-join row placement when
opening the scan. Direct CPU fell 6–7% for fixed-width inputs, nested integer
views improved 7–16%, and joins improved 1–7% across all four types. The
ordinary UUID and varchar views were approximately flat, and ordered LIMIT
results were mixed. The pinned pass measured 4.4–12.7× C CPU.

Ungrouped COUNT(*) now reduces the input iterator to a single count without
hash-group state. Ordinary views improved 5–23%, nested views 1–11%, and
ordered LIMIT and join holdouts were mixed. The initially slower int4 join
measured 3.47 ms versus 3.62 ms in a pinned 21-sample repeat. The full
benchmark pass measured 4.3–11.8× C CPU and 3.4–4.0× C peak RSS. After
the placement and count changes, primary queries allocated 0.75–2.55 MB,
joins 2.42–4.19 MB, and ordered LIMIT queries 3.86–5.66 MB.

Eligible fixed-width sequential scans now test the predicate column before
allocating a full row. NULL-bearing and older tuple layouts use the general
decoder. In a pinned seven-sample run, ordinary and nested integer views
used 12–14% less CPU. UUID and varchar view timings changed by about 1%,
and holdouts changed by at most 4%. Integer view allocations dropped 35–37%
to 0.49–0.51 MB per query. The run measured 4.4–12.2× C CPU.

Matching fixed-width rows now reuse the decoded predicate datum. UUID
ordinary and nested view allocations fell 22–24% to about 1.05 MB.
Ordinary UUID view CPU improved 14–17%, and nested UUID view CPU improved
24–27% across two runs. A pinned alternating repeat measured the UUID join
at 4.17 ms versus 4.61 ms and varchar view timings within 3%.

Specializing vector accesses and offset arithmetic in the fixed-width
decoder was tested and reverted. The alternating sweep improved integer
views by 3–5%, but the UUID join shifted from 18% faster in that sweep
to 12% slower in a focused repeat. The apparent slowdowns for ordered UUID
and nested varchar views were within 1% in that repeat.

Text three-way comparison now uses a single mismatch search. A pinned
alternating repeat reduced varchar ordered LIMIT CPU from 17.97 to 16.99
ms and ordinary views from 6.16 to 5.63 ms. The varchar join improved
about 2%. Nested varchar view results differed across runs, with the
alternating repeat rising from 5.36 to 5.79 ms.

Bounded sorts now recycle a candidate item and its key vector, releasing
references when a row is rejected or evicted. Ordered LIMIT allocated
20–29% less across all four types. A pinned alternating run measured
4–12% lower ordered LIMIT CPU. The join control also ran faster in that
repeat despite using no bounded sort, so the CPU delta includes run
variation. The varchar ordered LIMIT case measured 4.0× C CPU.

Bounded heap replacement now moves each displaced child once, following
PostgreSQL's tuplesort implementation. The pinned alternating benchmark
reduced ordered LIMIT CPU by 7–14% across all four types, with varchar
at 3.74× C CPU. The reverse-input checks changed by at most 5%, and
shuffled results were mixed, from 4% faster to 9% slower. These inputs
replace the heap root much less frequently than the original workload.

Single-key bounded sorts now resolve sort direction and NULL placement once
and use one comparator for heap maintenance and final output. The pinned
alternating comparison reduced ordered LIMIT CPU by 6–8% across all four
types, with varchar at 3.58× C CPU. In a focused repeat, int8 ordered LIMIT
fell from 7.53 to 6.99 ms while the int8 join control moved from 3.44
to 3.50 ms.

TOAST demand no longer disables fixed-width sequential decoding. Direct
int4, int8 and UUID queries used 45–54% less CPU and allocated 22–39%
less in the alternating comparison and allocation sweep. CPU measured
4.46–6.32× C for those direct queries. Varchar direct CPU rose 3%, and
the join and ordered LIMIT holdouts changed by at most 7%.

Ungrouped counts over eligible fixed-width filters now receive a match
marker from the scan. They skip decoding the other attributes of a
matching canonical tuple. Direct fixed-width CPU fell 4–12%, to
4.07–5.87× C CPU, and allocations fell 28–64%. Ordinary and nested views
and both holdouts changed by at most 4%.

Ungrouped counts now use the match markers through row-preserving identity
views. Fixed-width ordinary and nested views used 7–13% less CPU and
allocated 26–62% less, with CPU at 4.24–5.65× C. Direct scans changed by
at most 2%. Varchar view timing was mixed and allocation was flat.

Count markers also cover inline variable-width rows on tables without
TOAST storage. The decoder still materializes the predicate value.
The focused alternating varchar run reduced direct CPU by 5% and nested
view CPU by 14%. Ordinary views rose 4%, and the bounded and join
holdouts moved by less than 2%. Direct and nested counts measured
9.57× C CPU.

The scalar scan chooses the text comparison function when planning its
predicate. Direct, ordinary and nested varchar counts used 4–5% less
CPU in an alternating run, and varchar holdouts improved by about 1%.
Fixed-width primary cases changed by at most 4%. The varchar primary
cases used 9.15–10.57× C CPU.

Count-only ASCII text predicates with fixed-width prefixes can compare
the inline bytes on the page. Unicode, NULL-bearing and older tuple
layouts, and compressed or external datums use the general decoder.
The pinned alternating run reduced varchar count CPU by 36–47% and
allocations by 88–91%. Direct, ordinary and nested counts used
5.92–6.18× C CPU. The varchar join control was approximately flat.

A typed octet comparison loop replaced the generic sequence operation.
The pinned alternating run reduced varchar count CPU by another 21–23%,
to 4.63–4.85× C. Both varchar holdouts stayed within 4%, and the fixed-width
primary queries also stayed within 4%.

On x86-64, ASCII validation tests the high bits eight bytes at a time for
long spans, with a byte loop for the tail. Varchar count CPU fell another
27–29% to 3.29–3.47× C, with about 0.26 MB allocated per query.
The varchar ordered LIMIT and join holdouts used 4% and 8% less CPU,
respectively.

Combining four word checks into a 32-byte block was tested and reverted.
The pinned alternating run increased varchar count CPU by 2–8%. The
word-boundary regression now also checks that a high bit immediately
after the span does not affect validation.

Reading the backend XID once per page-reader pull was tested and reverted.
Direct integer counts improved about 2%, while ordinary and nested UUID
views and direct varchar counts slowed 3–4% in the alternating run.

Eligible text filters now reject ASCII rows on the page even when the
caller needs full rows. Matching rows retain their normal decoder. The
varchar join used 30% less CPU and allocated 28% less, at 6.67× C CPU.
The int8 join was slower in both runs, with the difference falling to
9% in the focused repeat. That plan does not use the text predicate path.

Nested loops now rewind their reader when the inner replay is fully
cached in memory. Disk replay and unmatched-right output retain their
separate reader lifetimes. The alternating join run used 21–28% less
CPU and allocated 47–66% less across all four types. Join CPU measured
5.26–6.01× C, and the ordered LIMIT control changed by at most 4%.

Nested-loop replay now reuses a successful integer equality check when
it proves the entire join predicate. The alternating join run used
1–6% less CPU, with ordered LIMIT controls within 4%. Join CPU measured
4.97–5.93× C. Mixed-width keys, NULLs, duplicate matches, full joins and
residual qualifications were checked over the wire.

When all WHERE conjuncts move into one view below inner or cross joins,
the planner drops the redundant filter above the join. The ungrouped
count can then consume markers for matches already proven by replay.
The alternating join run used 4–7% less CPU and allocated 4–10% less,
at 4.51–5.58× C CPU. Ordered LIMIT controls changed by at most 5%.

Replay readers now return the count marker before copying a proven
matching inner row. This reduced join allocations by 6–16% across
the four holdouts. CPU improved 0–4%, leaving the joins at 4.54–5.38× C.

Replay readers now resolve the spool's immutable origin and source-presence
flags when they open. The alternating holdout run changed join CPU by at
most 3%, and varied from 4.49–5.66× C.

Ungrouped counts use the interrupt check in each iterator pull. Removing
the extra check between pulls reduced direct fixed-width count CPU by
1–8% and fixed-width view CPU by up to 4%. Varchar and join results
were mixed. A 160,000-row view count raised the expected statement
timeout and succeeded on the same connection after resetting it.

Subquery iterators now forward the pull after the caller's interrupt
check. Nested view CPU improved 2–5% across all four types. Direct and
join controls stayed within 4%. A timed-out nested view count completed
correctly after the same connection reset its timeout.

Reusing the caller's check for the first pull of a mapping iterator was
tested and reverted. Cancellation after a rejected row worked, but the
alternating run slowed ordinary and nested varchar views by 4–6% and
nested UUID views by 5%. Join controls changed by at most 4%.

Ungrouped counts over a bare integer inner join can count matches from a
small in-memory replay in one pull. The alternating join run used 7–12%
less CPU, at 3.91–5.41× C. Direct and view controls changed by at most 5%.
Wire checks covered empty inner stores, NULL and duplicate keys, and the
32-row batch boundary.

The small inner replay now builds an integer-key frequency table once.
The alternating join holdout used 6–13% less CPU, at 3.38–4.50× C,
and the ordered LIMIT control changed by at most 6%. Additional wire
checks covered bigint extrema and the fallback beyond 32 cached rows.

Count-only UUID filters can compare the field directly on a canonical
fixed-width tuple. UUID count CPU fell 25–28%, to 3.70–4.01× C, and all
three UUID count shapes allocated about 0.20 MB per query. The UUID join
and ordered LIMIT controls changed by less than 3%. Seventy-two wire
queries matched C across all comparison operators, reversed operands,
NULLs and pre-ALTER tuple layouts.

UUID filters can also reject canonical rows on the page when the caller
needs full row output. The alternating run reduced UUID join CPU by 7%
and allocation by 27%, at 3.93× C CPU. The initial slower int4 ordered
LIMIT control did not repeat. Thirty-six UUID output and join queries
agreed with C PostgreSQL.

Signed integer predicates now select the two-argument comparison
operator directly. Primary integer queries used 1–5% less CPU, and
integer join controls improved 1–4%. The C int4 samples varied between
legs, so the comparison uses the pooled C medians, giving 3.56–4.09× C
CPU across int4 and int8 primary queries. One hundred twenty wire
queries matched C across reversed operators, NULLs and bigint extrema.

Compiling a closure around each selected integer operator was tested and
reverted. It improved int4 view CPU by up to 4%, but the int8 cases
were flat or slower in the alternating sweep.

Eligible count-only scans now accumulate a tally per heap page. The
alternating run used 5–13% less CPU for direct and view counts, at
3.02–3.63× pooled C CPU. Join controls moved by at most 5%. Peak RSS
across the suites measured 3.35–4.02× pooled C. Wire checks matched
full-row filtering across pages with zero, some, and all rows qualifying.

The pre-tally profiles found most nested-view samples under the subquery
iterator, with heap visibility and tuple access consuming substantial CPU.
The varchar join still spends time decoding matched rows. The int8 ordered
LIMIT holdout spends about 29% in bounded heap replacement (inclusive)
and 14% in spill-size estimation (inclusive).

Bounded sort memory accounting now sizes the retained fields without
constructing a spill item for each candidate. Ordered LIMIT allocations
fell 17–29% and CPU fell 2–6% across all four types. The varchar case
measured 3.17× pooled C CPU. A regression checks that the direct charge
matches the charge for a constructed spill item.

Bounded heap replacement now uses the collector's backing vector and
computes the last parent position before descending the heap. Ordered
LIMIT CPU fell 7–14% across all four types in the alternating run, with
varchar at 3.09× pooled C CPU. Join controls changed by at most 5%.

Single-key bounded sorts now select the text or UUID comparator at
startup. UUID ordered LIMIT used 8% less CPU. Varchar used 1–3% less
and reached 2.93× pooled C CPU in both the broad sweep and a focused
38-sample repeat. Twenty-four wire queries matched C for text and UUID
ordering, NULL placement and varying limits.

The bounded-sort memory estimator now sizes a projected row and its
environment directly when both are vectors. Integer and UUID ordered
LIMIT CPU fell 2–4%. The initial varchar slowdown did not repeat in
the focused alternating run, which measured 11.81 ms versus 11.94 ms
and 2.87× pooled C CPU. The charge regression covers both row layouts.

Spill-memory accounting now rounds integer bit lengths to whole bytes
with a shift. Fixed-width ordered LIMIT CPU fell 1–3%. Varchar changed
by about 1%, and join controls stayed within 3%. The old and new memory
charges matched for 16,388 signed values near bit-length boundaries.

On x86-64, in-bounds 32-bit reads from simple octet vectors now use one
pinned load. Direct and view count CPU generally improved by up to 5%,
and integer joins by 4–9%. The initial int8 ordered LIMIT and varchar join
slowdowns did not recur in a focused alternating repeat. Unaligned loads
and invalid offsets were verified against byte-wise reads.

On x86-64, 64-bit page reads now share the bounds check and use one
pinned load. Direct and view int8 counts used 9–10% less CPU. The UUID
primary control was 4–5% slower in a focused repeat. In that repeat the
varchar ordered LIMIT case improved 2%, while the varchar join rose 4%.
The page-load regression covers unaligned and invalid offsets for both
word sizes.

A single pinned 16-bit load was tested and reverted. It helped the
varchar join but slowed both int4 view counts and int8 ordered LIMIT
by up to 5% in the alternating run. The new 16-bit unaligned and
invalid-offset regression remains useful for the retained byte reader.

Heap int4 and int8 readers now use signed pinned loads on x86-64. On a
mixed-sign view workload int8 CPU fell 15% and allocation fell from
0.59 to 0.20 MB. Int4 CPU fell 3% in the same workload. The ordinary
benchmark improved integer joins 3–5%, and direct and view integer
counts ranged from 7% faster to 1% slower. Signed extrema and unaligned
offsets were verified directly.

Count-only inner integer equality joins now pass the probe key through
identity view projections to eligible heap filters. The page predicate
checks the payload before the scan skips its SQL value conversion. In the
alternating holdout run, UUID join CPU fell 21% and varchar join CPU fell
17%. Median allocations fell from 819 to 491 KB for UUID and from 1,441 to
557 KB for varchar. Integer join CPU rose 1–4% and allocations stayed
flat. The holdout joins remain at 3.11–4.10× pooled C CPU. The Rove checks
cover key decoding for fixed and inline tuples, and wire checks cover
duplicate keys, NULLs, non-ASCII text, outer joins and computed projections.

Two octet-span comparison experiments were reverted after alternating
primary and holdout runs. Calling libc's memcmp slowed the varchar join
by 6% and the UUID join by 5%. Skipping equal 64-bit words helped the
UUID join by 4% but slowed varchar counts by 5–9% and its join by 5%.
An idle Linux process had 50 MiB resident from the saved Lisp core and
23 MiB in anonymous mappings before the benchmark fixture was installed.

The heap visibility reader now loads adjacent xmin and xmax fields as
one word on x86-64 and returns the two raw IDs separately. This keeps
the snapshot checks and avoids allocating a boxed 64-bit value for
large delete XIDs. A REPL check measured zero allocation in 100,000
high-XID visibility checks, compared with 3.2 MB when the combined word
was returned to the caller. The alternating run improved int4, UUID
and varchar primary queries by 1–7% and int8 by up to 3%. A focused
repeat put the int8 join and ordered LIMIT within 2% of baseline.

Single-key top-N comparisons now order fixnums directly. The integer
ordered LIMIT holdouts used about 10% less CPU and measured 3.55× C for
int4 and 3.80× C for int8. UUID and varchar ordered LIMIT changed by
about 2%. A regression compares the fast and general orderings for both
directions, explicit NULL placement, signed extremes and stable ties.
An alternating holdout with shuffled signed keys, duplicates and NULLs
used 5% less int4 CPU and 2% less int8 CPU. Allocation stayed flat
across the standard ordered LIMIT cases.
Fresh profiles attribute 16% of int8 and 25% of UUID time to replacing
the top-N heap root. Spill-item memory accounting takes about 11% of
the int8 profile.

Top-N root replacement now uses a bottom-up sift after the previous
replacement reached a leaf. The standard ordered LIMIT holdout used
7% less int4 CPU, 10% less UUID CPU and 9% less varchar CPU in the
alternating run. Int8 was flat in that run and improved 3% in a focused
repeat. Shuffled inputs ranged from flat to 4% slower. Workloads that
repeatedly replaced at the root or its child stayed within 2% of
baseline. Unconditional bottom-up replacement was rejected after
shallow placements regressed. A REPL stress check validated the
heap after 66,240 offers across 69 bounds and all four order modes,
including 6,197 changes between replacement strategies.

An eligible identity projection below a top-N sort now shares a
read-only vector between its projected values and its source environment.
This removed about 0.6 MB of allocation per ordered LIMIT query across
int4, int8, UUID and varchar. CPU fell 20–24% for fixed-width values and
7% for varchar in the alternating run. Memory accounting now measures
the shared vector once and charges for both serialized copies. That
saved a further 1–6% of CPU. Ordered LIMIT measured 2.58× C for int4,
2.89× for int8, 3.11× for UUID and 2.48× for varchar. Ten wire queries
matched C at 64 kB and 4 MB work memory, including aliases, computed
targets, reordered projections and NULL placement. A missing descriptor
width takes the ordinary projection path.
Shuffled and shallow-placement holdouts improved 23–27% for integers,
19–20% for UUID and 12% for varchar after both changes.

The resumable heap reader now reuses the immutable page's line-pointer
count for bounds checks. Count-only joins on canonical inline text
tuples read the demanded int4 or int8 key directly when the columns
between the key and text are fixed-width. Other layouts use the
ordinary decoder. The alternating suite improved primary CPU by
3–9% and the varchar join by 25%, to 3.04× C. Joins for int4, int8
and UUID measured 2.95–2.96× C. Separate layouts with the key before
text or after a fixed-width suffix improved about 25% with the key
decoder alone. The extra-varlena layout, NULL keys, non-ASCII text
and rows written before a column addition matched C results.

Inlining the line-pointer decoder at the resumable heap scan call site
cut primary CPU 6–17% across all four types. Those counts used
2.48–2.83× pooled C CPU, and all four joins used 2.55–2.79× in that sweep.
Pre-resolving sort columns was reverted after slowing the integer
ordered LIMIT cases. Charging UUID spill memory from its fixed byte
width improved UUID ordered LIMIT CPU by 6% in an alternating run.
Separate pinned sweeps still place UUID sort CPU on either side of 3×.

Reusing the existing compact SBCL build from P10 brought full-server
peak RSS to 68–72 MiB from 84–88 MiB with the same application code.
With a 2 MiB allocation threshold between GCs the compact server
peaked at 63–66 MiB, or 2.58–3.03× pooled C peak RSS. The smaller
threshold raised ordered LIMIT CPU to 3.07× for int4 and 3.42× for
UUID, so it does not meet both targets together.

A fresh compact-runtime sweep after the catalog and deparser fixes
measured 2.36–2.99× C CPU across all primary and holdout cases at 7 MiB
between GCs. Fixed-width peak RSS was 67–68 MiB, or 3.12–3.21× C.
At 2 MiB, peak RSS was 63–66 MiB, but UUID's ordered LIMIT and join
medians exceeded the CPU target. The joint target remains open.

Keeping the identity projection's vector in the top-N heap removes a
per-input cons. The sorter forms the projected/source pair when it
emits or spills the row and charges for both serialized copies. Median
allocation per ordered LIMIT fell by 181–225 kB across the four types.
In a fresh pinned sweep at 7 MiB between GCs, all eight holdout CPU medians
were 2.29–2.93× C. UUID primary scans were 3.18–3.21× C, and fixed-width
peak RSS was 3.13–3.25× C. At 3 MiB, UUID ordered LIMIT was 3.23× C
CPU and the largest peak RSS ratio was 3.08×. At 2 MiB, the largest
peak RSS ratio was 3.04× and UUID ordered LIMIT was 3.44× C CPU.

The top-N collector now retains the scan's immutable source identities
with bare rows and takes the direct key path for single-key ordering.
UUID comparison skips equal native words on 64-bit SBCL and compares
the first differing octet in unsigned order. Alternating runs at 2 MiB
between GCs lowered shared-prefix UUID ordered LIMIT CPU from 5.50–5.52 ms
to 5.19–5.27 ms. A shuffled, dispersed UUID holdout used 2.84–2.94 ms,
compared with 2.96–3.03 ms before the comparator change and 0.68–0.70 ms
for C. At 3 MiB, the standard shared-prefix UUID sort used 2.48× C CPU,
with peak RSS still reaching 3.07× C on int4.
Process high-water sampling at the 3 MiB threshold put the int4 peak at
63.7 MiB during fixture ANALYZE and the UUID peak at 64.7 MiB at the
same stage. The varchar fixture reached 69.5 MiB during ordered LIMIT.

Fixture profiling found avoidable copying in integer tokenization and
ANALYZE. The lexer now parses validated integer spans in the original
statement, with a temporary string only for embedded separators.
ANALYZE copies a tuple slice only when it admits or replaces a
reservoir entry, while it still counts every visible tuple. Across the
four types, measured allocation fell by 67–107 kB per 500-row INSERT
and by 0.68–2.31 MB per ANALYZE. The compact-runtime sweep at 3 MiB
measured 2.17–2.78× C CPU on the eight holdouts. Fixed-width process
peak RSS reached 3.09× C, and the highest primary CPU median was
3.04× C for the direct UUID count.

The 72 MiB benchmark heap cap exhausted SBCL during the physical-reader
suite's large INSERT. The same application core passed the full suite
with a 1 GiB heap cap. At that cap and a 3 MiB GC threshold, the P11
benchmarks peaked at 89–97 MiB (3.74–4.31× C), and CPU medians ranged
from 2.22× to 3.06× C. Treat the 72 MiB results as constrained benchmark
measurements.

Simple query frames now use compact strings for validated ASCII input.
Decoding and parsing a 1 MiB ASCII INSERT allocated 2.9 MB in the REPL,
down from 9.1 MB. ASCII, UTF-8 and multi-statement wire cases matched C.
The physical-reader suite passed with a 128 MiB heap cap. At 96 MiB it
passed the large INSERT and both replay crash/restart checks, then
exhausted the heap during standby shutdown. At 128 MiB with a 3 MiB GC
threshold, all 20 primary and holdout CPU medians were 2.23–2.82× C.
The largest peak RSS ratio was 3.09× C, on the UUID primary case.
Lowering the threshold to 2.25 MiB brought the largest RSS ratio to
3.03× C, with UUID primary CPU reaching 3.10× C. At 2.5 MiB the maxima
were 3.08× C RSS and 3.07× C CPU. These sweeps leave the joint target open.

The lexer now reuses the fixed punctuation spellings. In the REPL, this
saved about 80 kB per 500-row INSERT tokenization. The regression test
checks token text and positions around a leading-dot numeric literal.

`view-performance.py --suite holdout --shuffled` permutes the insertion
order for every type and spreads UUIDs across their range. It checks the
same result counts before timing. A direct sort-key slot experiment was
reverted after shuffled UUID CPU regressed in the alternating run. With
the retained implementation, shuffled ordered LIMIT used 4.41× C CPU for
UUID and 4.81× for varchar(127). Allocation profiles attributed about 84%
of UUID samples to row and UUID decoding, and 89% of varchar samples to
tuple deformation and string decoding.

UUID decoding now copies simple octet vectors directly with SUBSEQ.
The copied UUID owns its bytes. At a 2.25 MiB GC threshold, alternating
runs reduced UUID ordered LIMIT CPU from 5.20–5.22 ms to 5.01–5.11 ms
on ascending input and from 2.84–2.90 ms to 2.71 ms on shuffled input.
The shuffled case still used 3.93–3.94× C CPU. Eight UUID wire sorts
matched C across 64 kB and 4 MB work memory.

Eligible identity scans now deform into a reusable row vector. The top-N
heap takes ownership on admission and returns the evicted vector to
the scan. After a spill transition, external-run buffering copies the
borrowed vector before the next pull. This cut median allocation by
0.56–0.59 MB per bounded query across int4, int8, UUID, and varchar(127)
in both insertion orders. The alternating runs improved UUID CPU by
17–20% on ascending input and 28–30% on shuffled input. The integer
and varchar CPU medians varied by up to 5% in either direction.

At the 128 MiB heap cap and 2.25 MiB GC threshold, the primary suite
used 2.48–2.94× C CPU and up to 2.98× C peak RSS.
The ordered holdout used 2.16–3.00× C CPU and up to 2.98× C
peak RSS. Shuffled UUID ordered LIMIT used 2.90–2.91× C CPU, while
shuffled varchar ordered LIMIT used 4.87–5.02×. The physical-reader and
transactional catalog suites passed at this configuration.

An exact identity scan can now reject canonical ASCII text or fixed
UUID rows at the page reader once a single-key top-N heap fills. The
cutoff uses the worst retained key and the arrival-order tie rule.
NULLs, unsupported tuple layouts, and non-ASCII text still use the
ordinary decoder. A spill transition clears the cutoff before the
next scan pull. The wire fixture fills the heap with short rows before
introducing wider rows and Unicode, and checks the results with 64 kB
and 4 MB work memory.

The compact benchmark core was built with `--heap-mib 128
--gc-threshold-kib 2048`. Two pinned sweeps over all 28 primary,
ordered holdout, and shuffled holdout cases reached at most 2.92× C
median CPU. Shuffled bounded UUID used 1.55–1.57× C CPU at 0.26 MB
allocated per query. Shuffled varchar used 1.70–1.73× C CPU at 0.43 MB.
Peak RSS still reached 3.10× C on int4. A separate int4 trace put the
high-water mark at 64,536 KiB during the fixture INSERTs, before ANALYZE
and the view queries. The joint target remains open on RSS.

An indexed lexer buffer reduced allocation in a REPL INSERT probe, but
raised process peak RSS in alternating server runs and was reverted.
Literal INSERT batches now reuse value and tuple-layout vectors when
there are no foreign-key probes. The encoded tuples own their bytes,
and foreign-key batches still retain per-row values and varlena data.
Median allocation fell by 90–113 kB per 500-row INSERT across all four
types. A 120 MiB heap with a 2.125 MiB GC threshold passed the physical
reader suite and peaked at 3.06× C RSS. The same sweep reached 3.18× C
CPU. A 112 MiB heap completed both crash/restart checks, then exhausted
the heap while verifying a retained WAL extent during standby shutdown.

Retained WAL checksum verification now continues the CRC over four
zero bytes after hashing the prefix, avoiding a full-size temporary
extent copy. The retained payload still has independent storage.
At a 2 MiB GC threshold both builds passed physical replay with a
112 MiB heap. At 104 MiB the earlier build exhausted the heap during
standby shutdown and the new build passed. With the latter configuration,
peak RSS reached 3.05× C and ordered UUID LIMIT reached 3.50× C CPU.
The shuffled holdout medians all stayed at or below 2.63× C CPU.

The page cutoff now pauses for 64 rows after eight comparable rows
in a row pass it, then checks again. Paused comparisons use the
ordinary decoder and the heap still decides whether each row qualifies.
At a 104 MiB heap cap and 2 MiB GC threshold, alternating runs lowered
ordered UUID LIMIT from 3.30–3.41× C CPU to 3.09–3.19×.
Shuffled UUID and varchar LIMIT medians stayed below 1.79× C, and
int4 peak RSS reached 3.07× C. Physical replay passed at this setting.
Replay exhausted the heap during standby shutdown at the same cap
with a 2.125 MiB GC threshold. The joint target remains open.

Retained-WAL publication can validate a borrowed extent payload from
its fresh file image. Hashing that displaced slice uses the existing
span kernel. A 1 MiB slice matched the direct span checksum with zero
reported allocation in the REPL probe. Restartpoint reconstruction
reuses an exact anchor when the checked spool records agree, and builds
a fresh prefix at the first mismatch.

Physical replay passed with a 104 MiB heap cap at both 2 and 2.125 MiB
GC thresholds. At 2.125 MiB, the 28-case sweep reached 3.01× C peak RSS
and 3.19× C median CPU. At 2 MiB the ordered holdout stayed below 2.91×
CPU, while the direct UUID count reached 3.33×. Specializing the UUID
heap comparator was reverted after the gain failed to repeat. The
104 MiB replay run at 2.1875 MiB between collections failed, and the
joint 3× target remains open.

On x86-64, canonical UUID filters now compare the first octet before
loading the two words of a validated heap span. The word comparison
preserves unsigned byte order and avoids decoding a SQL UUID.
The bytewise comparator handles other architectures. The direct filter
matched C for 216 queries spanning all six operators, both operand
orders, NULLs, and rows written before a column addition.

At a 104 MiB heap cap with a 2 MiB GC threshold, two complete sweeps
reached exactly 3.00× C peak RSS. Every CPU median was within the
target except ordered UUID LIMIT, at 3.05–3.10× C. Both attempts to
change the heap comparator were reverted after their end-to-end runs
failed to improve that case consistently. Physical replay passed at
the selected setting. A 2.0625 MiB threshold at the same heap cap
exhausted the heap during standby shutdown. The joint target remains
open on ordered UUID CPU.

For eligible fixed-width identity scans with a single sort key, the
top-N heap now uses a conservative upper bound for the retained rows'
spill charge. The bound includes NULLs and the serialized row metadata.
If the bound reaches work_mem, per-item charging determines when to spill.
At the same 104 MiB cap and 2 MiB GC threshold, ordered UUID LIMIT used
at most 2.78× C CPU across two sweeps. Occasional varchar CPU and int4
peak RSS misses remained.

At 1,856 KiB between collections, three sweeps of 28 comparisons reached
2.984× C peak RSS. All CPU medians were within 3× except the ascending
varchar join, which ranged from 2.98× to 3.55× C. The same join ran at
2.78× C in isolation. In the interleaved allocation run, the preceding
ordered LIMIT allocated 2.06 MB per query and the join allocated 0.56 MB.
Physical replay, including both crash points and standby shutdown,
passed at this heap cap and threshold.

The benchmark now alternates which engine runs first in each sample
while preserving the query order within each engine. At 104 MiB and
1,856 KiB between collections, all paired CPU medians stayed below
2.78× C. Int4 peak RSS reached 3.07× C. The allocation pass is measured
separately and reports 2.03 MB for varchar LIMIT and 0.59 MB for the join.

Validation-only retained WAL scans can borrow each record's payload
from the private raw record buffer. Scans that pass records to an
observer retain independent payload storage. The physical replay suite
passed with a 102 MiB heap cap and a 1,856 KiB GC threshold after this
change. At that setting, one complete paired sweep stayed within 3×
for both CPU and peak RSS. Another reached 3.69× CPU on the ascending
varchar join and 3.02× peak RSS on the shuffled int4 holdout.

Final retained WAL publication skips copying a prefix when the scan
has no group left to start. This allowed the physical replay suite to
finish with a 100 MiB heap cap at the same threshold. Two paired sweeps
at that setting reached 2.72× C CPU and 3.021× C peak RSS. The int4
RSS high-water mark was established during data and view setup.

The 100 MiB preloaded core at a 1,664 KiB GC threshold passed physical
replay, including standby shutdown. Three paired sweeps each covered
28 comparisons at 31 samples per engine. All CPU medians were at most
2.655× C. Two sweeps were within the 3× RSS target in every case, with
maxima of 2.9965× and 2.9951×. In the remaining sweep seven int4 or
int8 comparisons exceeded the target, reaching 3.014×. Absolute Lisp
peak RSS was at most 66.9 MiB across these runs.
In a separate allocation sweep, per-query medians ranged from 164 to
262 kB in the primary suite, 229 kB to 2.03 MB in the ordered holdout,
and 249 to 557 kB in the shuffled holdout.

To reproduce the setting, use `t/build-performance-core.py` with a
matching compact SBCL executable and core directory, `--heap-mib 100`,
and `--gc-threshold-kib 1664`. Set `SBCL` to the generated launcher
and run the primary, ordered holdout, and shuffled holdout suites with
`--samples 31 --skip-allocation` on a single pinned CPU. Save each suite
to a separate log so the ordered and shuffled comparisons stay distinct.
