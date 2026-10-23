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

## Phase 2 (v2 blobs): Scylla sweep

The v1 phases above (monthly gzip-JSON blobs, bloom filter, `ScyllaToS3BackfillService`) are superseded by the v2 archive
(`auctions/v2/{tag}/{yyyy-MM}/{part:D3}.msgpack.br`, core `Coflnet.Sky.Core.Archive`). Phase 1 of v2 runs in SkyIndexer and
archives MariaDB; this phase, `Services/Archive/ScyllaArchiveSweep.cs`, adds the rows that only Scylla has. **SkyAuctions never
writes MariaDB** (its `DBConnection` is a read-only user): it writes R2 objects and Redis markers, and SkyIndexer's
`ScyllaArchiveImportWorker` does every MariaDB write.

- **Scope:** months `ARCHIVE:OLDEST_MONTH` .. `ARCHIVE:NEWEST_MONTH` (defaults 2019-07 .. 2021-03, the months where MariaDB only
  holds leftovers). The sweep never goes past `NEWEST_MONTH`.
- **MariaDB, read only:** the month sentinel (`archived_months` row with `tag_id = 0`: exists? `scylla_at`?), the tag's row
  (parts, rows, sold rows, sha256, source; read as a projection), `auction_tags` ids and `auction_locations` (superseded check).
  At start one cheap sentinel query verifies the connection; failures log
  `Auction archive: MariaDB (DBConnection) unavailable, retrying in 10 min` and retry every 10 minutes.
- **Partition list:** the partition keys of `weekly_auctions_2` come from a token-range scan, because one
  `SELECT DISTINCT tag, timekey` over the 925 GB table times out (production, 2026-10-23). The ring is cut into
  `PARTITION_SCAN_RANGES` (default 4096) ranges, each read with a prepared
  `SELECT DISTINCT tag, timekey ... WHERE token(tag, timekey) > ? AND token(tag, timekey) <= ? BYPASS CACHE` (LocalOne, 1000 keys
  per page, `PAGE_PAUSE_MS` between ranges). A page that times out is retried up to `PAGE_RETRIES` times (2/4/8 s); a range that
  still fails is halved, at most 8 times, before the pass fails. Only partitions that can overlap the months in scope are kept.
  Progress lives in Redis (`auctions_archive:v2:partitions:found` set, `auctions_archive:v2:partitions:next_range` =
  `{next}/{ranges}/{oldest}/{newest}`), so a restart continues with the next range; another range count or scope restarts.
  When the last range is done the list is cached in R2 (`auctions/v2/_meta/scylla_partitions.msgpack.br`, reused for
  `PARTITION_LIST_MAX_AGE_DAYS`) and the Redis progress is deleted. Gauges `sky_auctions_archive_partition_scan_ranges_done`
  and `_partition_scan_found`, counters `_partition_scan_retries` and `_partition_scan_splits`; progress is logged every 256 ranges.
- **Per tag-month:** phase-1 parts are read uncached (never rewritten). The Scylla partitions overlapping the month are paged
  (`LocalOne`, `PAGE_SIZE`, up to `PAGE_RETRIES` retries per page with 2/4/8 s backoff resuming from the same paging state), rows
  outside the month are skipped (End before 2019-06 counts as garbage), rows are deduped per auction uid (sold, then higher bid,
  then later End). Auctions phase 1 has are only cross-checked (highest bid, sold flag, End, item bytes length, starting bid);
  phase-1 data always wins. A Scylla-only auction that already has an `auction_locations` row in another tag-month is a stale
  copy (an unsold snapshot of an auction bought earlier and archived in an earlier month) and is skipped (`rows_superseded`).
  Scylla-only rows are written as parts N, N+1, ... (source `scylla`) and verified by uncached read-back. Then, in this order:
  1. the R2 tag index entry (source `merged` when phase-1 parts exist, `scylla` otherwise),
  2. the month manifest `auctions/v2/_meta/scylla/{yyyy-MM}.msgpack.br` (`ScyllaMonthManifest`, one `ScyllaTagImport` per tag:
     first new part, new parts, rows, sold, part hashes),
  3. the Redis marker.
  Tag index and manifest writes are conditional PUTs (If-Match on the ETag read, If-None-Match: * when new; SkyIndexer writes
  the tag indexes the same way); on a conflict the object is re-read and the change re-applied, up to 5 times, else the
  tag-month fails without marker. A failure before the marker is redone next pass: parts N.. are rewritten and the manifest
  entry of the tag is replaced, never duplicated. A phase-1 month whose tag index entry is missing is rebuilt from its parts
  (recomputed hashes must match `archived_months.sha256`).
- **Tag `unknown` is remapped by item name:** `weekly_auctions_2` holds every auction from 2019-11 until about 2020-05 whose tag was
  not saved under the lowercase tag `unknown` (12-hour timekeys; 2.4M rows in 2019-11 alone). Those rows only have the display name,
  so `ItemNameTagResolver` (built once per pass from MariaDB `Items` and `AltItemNames`, read only) normalises it (colour codes,
  leading/trailing stars, `[Lvl N] ` pet prefix, double spaces) and resolves it when exactly one tag matches, retrying without a
  leading reforge word. Ambiguous (`Clay`, `Water Bottle`) and unmatched names stay under `unknown`. When a month has `unknown`
  partitions and no `unknown` marker, the sweep first pages them once and spills the resolved rows, grouped by resolved tag, into a
  disk-backed `RetagSpill`; each tag-month then merges its retagged rows into the normal dedupe/superseded/part flow (a tag with only
  retagged rows gets its own tag-month, marker and manifest entry). The `unknown` tag-month skips the rows that resolve and archives
  only the remainder. Parts of a tag-month are still written once. Per month it logs `unknown rows: N retagged to T tags, M kept;
  top unresolved names: ...`; counters `sky_auctions_archive_retagged_rows` and `_unresolved_rows`.
  **Restarts:** the pre-scan is redone on every pass that still has unmarked tags in the month (it is skipped only when the `unknown`
  tag-month already has its marker); retagged rows whose resolved tag is already marked are dropped,
  that tag-month received them earlier. **Caveat:** if `Items`/`AltItemNames` change between two passes of the same month, a row may
  resolve to a different tag in the second pass and be archived twice; the superseded check catches this once the earlier
  tag-month is imported by SkyIndexer.
- **Markers (Redis):** host `ARCHIVE:REDIS_HOST`, else `SETTINGS_REDIS_HOST` (sky-settings-redis, next to the bazaar archive
  markers; not the retired sky-auctions-redis). Keys: `auctions_archive:v2:{tag}:{yyyy-MM}` =
  `archived:{scyllaOnlyRows}:{newParts}:{iso}` | `empty:{iso}` | `overlap_only:{iso}`; `auctions_archive:v2:month:{yyyy-MM}` =
  iso when every tag of the month is done (the manifest gets `CompletedAt` first, also when no tag had new rows);
  `auctions_archive:v2:sweep_complete` = iso when every month in scope is done. Tag-months and months with a marker are
  skipped; lost markers are harmless (an imported tag/month is recognised by `scylla_at`, otherwise the work is redone).
- **Hand-off and pipeline gate:** SkyIndexer's `ScyllaArchiveImportWorker` (every `ARCHIVE:IMPORT_POLL_MINUTES`, default 5)
  reads each manifest whose sentinel has no `scylla_at`, verifies each new part's SHA-256 against the manifest and the tag
  index, writes `auction_locations` (never moving another tag-month's location) and `auction_bidders` (bidder uuid to
  `Players.Id`), upserts the tag's `archived_months` row (parts, rows, sold rows, sha256 over all parts, source, `index_rows`
  and `index_bidder_rows` grown by the same amount so the retention worker's `index_rows == rows` gate holds, `scylla_at`) and,
  once the manifest is completed and every entry imported, sets `scylla_at` on the sentinel. **The sweep starts month M+1 only
  after month M's sentinel has `scylla_at`** (it logs `waiting for SkyIndexer to import {M}` once and checks again every `IMPORT_WAIT_MINUTES`), so the
  superseded check always sees the locations of every earlier month.
- **Deploy order:** `dev` (core) → SkyIndexer (applies the `ArchiveScyllaAt` migration, runs the import worker) → SkyAuctions.
  The sweep reads `archived_months.scylla_at`; until SkyIndexer has migrated, its start check fails and retries every 10 minutes.
- **Known lossy fields** (Scylla does not store them, so Scylla-only rows lack them): `Claimed`, claimed bidders, MariaDB's
  short-keyed `NBTLookups` (Scylla's string-keyed `nbtlookup` is kept as the `NbtLookupStrings` column instead), `Reforge` and
  `AnvilUses` as columns (restored from `nbtlookup` "modifier"/"anvil_uses" when present), bid `BidderId` (resolved to
  `Players.Id` by SkyIndexer for `auction_bidders` only), bid bidder names. A seller profile that MariaDB had as null is stored by
  Scylla as the seller uuid, a null bid profile as the bidder uuid. The block's sold flag is `highest bid > 0`, not Scylla's
  `issold`. Scylla-only columns kept: highest bidder name, profile name, coop name, color, item uid, item id, nbtlookup strings
  and enchantments that are no `EnchantmentType` (`UnknownEnchantments`).
- **Metrics** (`/metrics`): `sky_auctions_archive_tag_months_total`, `_tag_months_empty`, `_parts_written`, `_parts_verified`,
  `_verify_failures`, `_rows_archived`, `_rows_already_archived`, `_rows_skipped_bogus_end`, `_rows_superseded`,
  `_retagged_rows`, `_unresolved_rows`, `_crosscheck_rows`, `_crosscheck_mismatches{field}`, `_pages`, `_partitions_read`, `_page_retries`, `_errors`, gauges
  `_current_month`, `_partitions_known`, `_last_month_completed_timestamp`, `_waiting_for_import_month`; store
  `sky_auctions_blob_*` (incl. `_blob_index_conflicts`). SkyIndexer: `sky_indexer_scylla_import_{tags, parts_verified,
  hash_mismatches, index_rows, bidder_rows, bids_without_bidder, months_completed, errors}`.
- **Config** (`ARCHIVE:*`, defaults in `appsettings.json`): `ENABLED` (false), `START_DELAY_SECONDS`, `OLDEST_MONTH`,
  `NEWEST_MONTH`, `PAGE_SIZE`, `PAGE_PAUSE_MS`, `PARTITION_PAUSE_MS`, `TAG_PAUSE_MS`, `MONTH_PAUSE_MS`, `IDLE_PAUSE_MINUTES`
  (pause between passes once the sweep is complete or while phase 1 is behind), `IMPORT_WAIT_MINUTES` (default 5, pause
  between passes while waiting for SkyIndexer to import the previous month), `MAX_ROWS_PER_PART`, `MAX_PART` (at most 254: `archived_months.parts` is
  a tinyint count), `PAGE_RETRIES`, `CIRCUIT_BREAKER_FAILURES`, `CIRCUIT_BREAKER_PAUSE_MINUTES`, `PARTITION_LIST_MAX_AGE_DAYS`, `PARTITION_SCAN_RANGES`,
  `CROSSCHECK_LOG_LIMIT`, `REDIS_HOST` (empty = `SETTINGS_REDIS_HOST`). Needs `S3:ENABLED=true`, `S3:BUCKET_NAME`,
  `DBConnection` (read-only is enough) and Redis.
- **Redo a month** (e.g. 2019-11 swept before the `unknown` remap existed). SkyAuctions' MariaDB user is read-only, so the operator
  deletes the MariaDB index rows and the sweep coordinates through a Redis key:
  1. `SET auctions_archive:v2:redo:2019-11 1` on sky-settings-redis.
  2. On its next pass the sweep reaches the month (oldest to newest) and first resets the R2 side: deletes the tag-month markers
     (manifest tags plus every tag with a partition in the month) and the month marker, and rewrites the manifest with no tags and
     `CompletedAt = null`. It then logs `redo of 2019-11 waits for the MariaDB reset: run ...` (warning, once per process and month),
     sets `sky_auctions_archive_redo_waiting_month` and ends the pass; nothing later is swept.
  3. Run the three statements from that log line: `DELETE FROM auction_locations WHERE month = {idx}; DELETE FROM archived_months
     WHERE month = {idx} AND tag_id <> 0; UPDATE archived_months SET scylla_at = NULL WHERE month = {idx} AND tag_id = 0;`.
     `auction_locations` has only its `uid` primary key, so the first DELETE scans the table; for millions of rows delete in
     batches (`DELETE FROM auction_locations WHERE month = {idx} LIMIT 200000;` until 0 rows affected) to keep lock time short.
  4. The next pass finds the MariaDB state clean (sentinel without `scylla_at`, no tag rows, no locations), deletes the redo key and
     sweeps the month from scratch (including the `unknown` pre-scan/remap); the completed manifest makes SkyIndexer re-import it.
  **Why the order matters:** if the SQL ran first, SkyIndexer (polling every 5 minutes) could see a sentinel without `scylla_at`
  and the still-completed stale manifest and re-import it. The sweep therefore empties and un-completes the manifest before it
  accepts the MariaDB state as reset. A redo key for a month outside `OLDEST_MONTH..NEWEST_MONTH` (checked +-24 months) is logged
  and deleted. A redo on a finished sweep clears `sweep_complete` and walks the months again. The old parts stay in R2
  unreferenced (the tag index and manifest point only at the new ones).
- **Deletion from Scylla is a later step** and not part of this phase. A Scylla partition (tag, week) may be deleted once every
  month it overlaps has `scylla_at` on its sentinel.

## Read side (v2 blobs)

`Controllers/ArchiveV2Controller.cs` (public like the other controllers, registered with `S3:ENABLED`, 503 otherwise) serves the
v2 archive through the core `Coflnet.Sky.Core.Archive.ArchivedAuctionReader` (R2 via `S3AuctionBlobStore`, MariaDB read only via
`ArchiveContext`/`DBConnection`). Read only; nothing is written anywhere.

- `GET /api/archive/v2/{tag}/months`: the tag index (`month`, `parts`, `rows`, `soldRows`, `source`, `verifiedAt`), 404 without index.
- `GET /api/archive/v2/{tag}/{yyyy-MM}?endFrom=&endTo=&sold=&bin=&minPrice=&maxPrice=&auctioneer=&itemName=&limit=&desc=&includeItemBytes=`:
  `ArchivedAuctionDto`s sorted by End (parts are merged, phase-2 parts overlap phase-1 ones); limit 200 by default, at most 5000.
  Price = highest bid when sold, starting bid otherwise.
- `GET /api/archive/v2/{tag}/{yyyy-MM}/export?format=csv|jsonl&...`: same filters, streamed, at most 100000 rows (`ArchiveExport`).
- `GET /api/archive/v2/auction/{uuid}`: one `auction_locations` read + one block read, with bids; 404 when not archived.
- `GET /api/archive/v2/player/{bidderId}/bids?fromMonth=&toMonth=&limit=`: via `auction_bidders`, each block read once.
- `GET /api/archive/v2/verify/{tag}/{yyyy-MM}`: uncached read of every part, recomputed SHA-256 against the tag index and
  `archived_months.sha256` (over the parts the row counts; phase-2 parts only after SkyIndexer imported them).
