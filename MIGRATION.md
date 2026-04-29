# Scylla → S3 Migration Runbook

This document describes how to migrate the SkyAuctions cold archive from
ScyllaDB to S3 (Cloudflare R2 in production / MinIO locally) without losing
data and without enabling deletion until you have manually verified the
archive.

The plan is conservative: we **never delete** anything until the archive
has been verified by hand and the operator has explicitly flipped two
independent feature flags.

## Phases

| Phase | Goal | Flags |
|-------|------|-------|
| 0. Off | Default | all `S3:*` flags `false` |
| 1. Mirror new data | New auctions written to **both** Scylla and S3 | `S3:ENABLED=true`, `S3:MirrorLiveData=true` |
| 2. Backfill old data | Walk Scylla oldest-first, write to S3 | also set `S3:BackfillScylla=true` |
| 3. Shadow validate | Endpoints query Scylla **and** S3, log diffs | also set `S3:ShadowMode=true` |
| 4. Active read | Hot window in Scylla, cold in S3 (already implemented) | tune `S3_MIGRATION:MONTHS_TO_KEEP_IN_SCYLLA` |
| 5. Delete (DRY-RUN) | Walk MariaDB, verify against S3, log what *would* be deleted | `S3:EnableDeletion=true` (registers the worker; deletes still skipped) |
| 6. Delete (ACTIVE) | Uncomment the active block in `DeletingBackGroundService.cs` and `S3:DeletionConfirmed=true` | manual code change required |

**No phase deletes anything until you uncomment the active block in
[`DeletingBackGroundService.DeleteBatch`](Services/DeletingBackGroundService.cs)
and set `S3:DeletionConfirmed=true`.** Both gates are required.

## Configuration reference

```jsonc
"S3": {
  "ENABLED": true,                          // master switch for S3 services
  "SERVICE_URL": "https://...r2.cloudflarestorage.com",
  "ACCESS_KEY": "...",
  "SECRET_KEY": "...",
  "BUCKET_NAME": "sky-auctions-archive",
  "MirrorLiveData": false,                  // Phase 1: dual-write live auctions
  "BackfillScylla": false,                  // Phase 2: walk Scylla oldest-first
  "BackfillMariaDb": false,                 // Phase 2 (optional): walk MariaDB
  "ShadowMode": false,                      // Phase 3: passive S3 reads + metrics
  "ShadowSampleEveryN": 1,                  // 1 = every request, 10 = 10% etc.

  // Deletion is double-gated:
  "EnableDeletion": false,                  // Phase 5: registers DeletingBackGroundService at all
  "DeletionConfirmed": false,               // Phase 6: actually deletes (commented-out path also required)
  "DeletionRequireFullContentMatch": true,  // strict comparison of end/bid/seller/bid-count
  "DeletionBatchSize": 32
},
"S3_MIGRATION": {
  "MONTHS_TO_KEEP_IN_SCYLLA": 3             // anything older is read from S3
}
```

## Phase 1 — Dual-write live data

```jsonc
"S3": { "ENABLED": true, "MirrorLiveData": true }
```

Effect: every new sold auction is also written to
`s3://.../auctions/{tag}/{yyyy-MM}.json.gz`. Scylla remains authoritative.
Live mirroring is wrapped in a try/catch so S3 outages cannot block Scylla
ingestion (see [`Services/BaseBackgroundService.cs`](Services/BaseBackgroundService.cs)).

## Phase 2 — Backfill 2019–2023, oldest first

```jsonc
"S3": { "ENABLED": true, "MirrorLiveData": true, "BackfillScylla": true }
```

The hosted [`ScyllaToS3BackfillService`](Services/ScyllaToS3BackfillService.cs)
runs once at startup and:

1. Lists every `(tag, timekey)` partition in `weekly_auctions_2`.
2. Sorts ascending by `timekey` (oldest first — `timekey` is monotonically
   increasing from 2019-01-01).
3. For each partition reads sold + unsold rows, groups by month, and writes
   one merged blob per `(tag, yyyy-MM)`. Existing blob contents are read,
   merged by UUID (keeping the higher `HighestBidAmount`) and rewritten —
   so the job is **idempotent** and safe to restart.
4. Adds every UUID to the global Bloom filter and player index.
5. Stores progress in Redis as `s3_backfill_scylla:{tag}:{timekey}`. Crash
   resumes from the last completed partition.

Resource usage notes:

- **One partition in flight at a time.** Largest tag-week observed in
  production is roughly 50–80 MB raw / ~6 MB gzipped, so peak per-partition
  RAM is bounded.
- **Player index buffers** are flushed at the end of every partition
  (`FlushAll`) so they don't grow unbounded.
- **Bloom filter** stays resident at ~960 MB (sized for 800M entries / 1%
  FPR). It is loaded once on startup and flushed periodically.

To start with a constrained scope (e.g. only 2019–2022) call the
`BackfillTag` helper from a custom controller or a one-shot job. The
service reads all partitions otherwise.

### Verifying coverage

After the backfill completes, run the existing integration test against
the deployment bucket:

```bash
dotnet test --filter S3ExistingBucketVerificationTest
```

The last verification run on `sky-auctions-dryrun-385a091eb475` reported
`Missing=236443`, `Extras=102389` out of `2,987,163` expected. Both
counters must reach `0` before progressing to deletion. The
[`S3ExistingBucketRepairTest`](Services/S3ExistingBucketRepairTest.cs)
re-walks Scylla and back-fills the gaps.

## Phase 3 — Shadow mode

```jsonc
"S3": { "ENABLED": true, "MirrorLiveData": true, "ShadowMode": true }
```

Every endpoint that does **not** look up by raw auction UUID now also
reads the matching S3 blob in the background and emits Prometheus metrics:

| Metric | Meaning |
|--------|---------|
| `sky_auctions_shadow_match_total{scope}` | Rows present in both stores |
| `sky_auctions_shadow_scylla_only_total{scope}` | In Scylla, missing in S3 (archive gap) |
| `sky_auctions_shadow_s3_only_total{scope}` | In S3, missing in Scylla read window (expected if Scylla TTL'd) |
| `sky_auctions_shadow_not_yet_archived_total{scope}` | S3 blob does not exist yet (backfill still running) |
| `sky_auctions_shadow_error_total{scope}` | Background read failure |

Scopes currently emitted:

- `filter` — `QueryService.GetFiltered` (item search / recent overview)
- `sumary` — `ScyllaService.GetSumary` (price summary endpoint)
- `player` — `ScyllaService.GetRecentFromPlayer` (player auctions endpoint)

The auction-by-UUID endpoints (`GET/POST /api/Auction/{uuid}`,
`POST /api/Restore/{uuid}`) are **intentionally excluded** because S3 has
no efficient index for raw UUID lookups (the only way is bloom-filter
followed by `ListBlobs` and a sequential scan). Their fallback path
through `UuidLookupService` still functions for production use.

Use a low-traffic period to ramp shadow mode by setting
`S3:ShadowSampleEveryN` to e.g. `10` first.

Wait until the gap counters stay flat at 0 across a full day before moving
to Phase 4 / Phase 5.

## Phase 4 — Switch reads to S3 for cold months

This is already wired. Reads for months older than
`S3_MIGRATION:MONTHS_TO_KEEP_IN_SCYLLA` automatically come from S3 via
[`QueryService.GetFiltered`](Services/QueryService.cs) and
[`ScyllaService.GetAuction`](Services/ScyllaService.cs) once `S3:ENABLED`
is `true`. The default is 3 months.

To migrate 2019–2023 specifically:

1. Make sure backfill (Phase 2) has produced blobs for every month in
   that range.
2. Lower `MONTHS_TO_KEEP_IN_SCYLLA` only when the verification test
   reports 0 missing for those months. Typical sequence: 36 → 24 → 12
   → 3.

## Phase 5 — Deletion (DRY-RUN)

```jsonc
"S3": { "ENABLED": true, "EnableDeletion": true, "DeletionConfirmed": false }
```

This *registers* `DeletingBackGroundService` but the active delete block
is **commented out** in [`Services/DeletingBackGroundService.cs`](Services/DeletingBackGroundService.cs).
With these flags the service:

1. Walks MariaDB rows older than 3 years in batches of 128.
2. Groups by `(tag, month)` and reads the matching S3 blob.
3. Strict-compares `End`, `HighestBidAmount`, `Tag`, seller UUID and
   bid count against the archived row.
4. On full match, logs `[dry-run] would delete ...`. On any mismatch,
   increments `sky_auctions_delete_verify_miss_total` and skips.
5. Never touches MariaDB.

Watch the Prometheus counter and the warning log for several days. If
the miss counter is ~0 you are ready for Phase 6.

## Phase 6 — Deletion (ACTIVE)

Manual change: open
[`Services/DeletingBackGroundService.cs`](Services/DeletingBackGroundService.cs),
find the `// ACTIVE DELETION PATH - intentionally commented out.` block
inside `DeleteBatch` and uncomment the `try { await service.RemoveAuction(...) ... }`
block. Then deploy with:

```jsonc
"S3": { "EnableDeletion": true, "DeletionConfirmed": true }
```

Both flags must be `true` *and* the code uncommented for any row to be
removed. The strict-content match in `ContentMatches` still gates each
row individually.

Roll back: comment the block again, redeploy. No state is lost — the
S3 archive retains everything.

## RAM budget in `k8s/main/sky/chart/charts/sky-auctions/values.yaml`

Current limits:

```yaml
resources:
  requests: { cpu: 150m,  memory: 3000Mi }
  limits:   { cpu: 2000m, memory: 8000Mi }
```

Estimated steady-state breakdown after Phase 4:

| Component | RAM |
|-----------|-----|
| .NET 10 runtime + Kestrel + EF Core + Cassandra driver | ~600 MB |
| Bloom filter (800M entries / 1% FPR, see [`BloomFilterService`](Services/BloomFilterService.cs)) | ~960 MB |
| Per-partition backfill working set (one tag-month) | ~80 MB |
| Player index in-memory buffer (until next `FlushAll`) | ~50 MB |
| S3 SDK + AWS HTTP client pool | ~100 MB |
| LZ4 compression buffers (Cassandra) | ~50 MB |
| Headroom for query bursts / GC | ~500 MB |
| **Total expected working set** | **~2.3 GB** |
| **Peak during simultaneous backfill + traffic** | **~3.5 GB** |

The current `8000Mi` limit has comfortable headroom. The `3000Mi` request
matches the expected steady-state and can stay as is. **No values.yaml
change is required for the migration.**

If you ever resize the bloom filter for >1B entries, raise the request
to `4000Mi`.

## Operational checklist

- [ ] Phase 1 enabled, mirror counter > 0 in metrics
- [ ] Phase 2 running, oldest partitions complete first
- [ ] `S3ExistingBucketVerificationTest` reports `Missing=0 Extras=0` for 2019–2023
- [ ] Phase 3 shadow counters flat at 0 for 24h
- [ ] `MONTHS_TO_KEEP_IN_SCYLLA` lowered in stages
- [ ] Phase 5 dry-run log shows expected delete volume, miss counter ~0
- [ ] Manual sample of 100 random `[dry-run] would delete` UUIDs verified in S3 by hand
- [ ] Phase 6: code uncommented, both flags `true`, deploy, watch `sky_auctions_delete_count`
