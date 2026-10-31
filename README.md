# SkyAuctions

Microservice for storing and querying Hypixel SkyBlock auction data. Supports multi-tier storage with ScyllaDB for recent data and S3 (Cloudflare R2) for archived data.

## Architecture Overview

```
                    ┌─────────────┐
                    │   Queries   │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌───────────┐
        │ ScyllaDB │ │   S3/R2  │ │  MariaDB  │
        │ (Recent) │ │(Archived)│ │ (Legacy)  │
        └──────────┘ └──────────┘ └───────────┘
              │            │
              │      ┌─────┴─────┐
              │      │   Index   │
              │      ├───────────┤
              │      │Bloom Filter│
              │      │Player Index│
              │      └───────────┘
```

### Storage Tiers

| Tier | Storage | Time Range | Use Case |
|------|---------|------------|----------|
| Hot | ScyllaDB | Last 3 months | Active queries, real-time data |
| Cold | S3/R2 | 3+ months old | Historical queries, cost-effective |
| Legacy | MariaDB | Any | Migration source, fallback |

## Deploying

This project should be deployed within a container using .NET 10.

### Configuration

See `appsettings.json` for all configuration options.

#### S3/R2 Configuration

```json
{
  "S3": {
    "ENABLED": "true",
    "SERVICE_URL": "https://<account-id>.r2.cloudflarestorage.com",
    "ACCESS_KEY": "<your-access-key>",
    "SECRET_KEY": "<your-secret-key>",
    "BUCKET_NAME": "sky-auctions",
    "REGION": "auto"
  },
  "S3_MIGRATION": {
    "ENABLED": "true",
    "DRY_RUN": "true",
    "MONTHS_TO_KEEP_IN_SCYLLA": "3"
  }
}
```

---

## Player Auction Export

### Overview

The system maintains per-player participation indices in S3 that track every auction where a player was involved (as seller or bidder).

### S3 Player Index Structure

```
s3://bucket/players/{uuid-prefix}/{player-uuid}/{year}.json.gz
```

Example:
```
s3://sky-auctions/players/ab/abcdef12345678901234567890123456/2024.json.gz
```

Each file contains an array of `PlayerParticipationEntry`:
```json
[
  {
    "AuctionUid": "12345678-1234-1234-1234-123456789012",
    "End": "2024-03-15T10:30:00Z",
    "Tag": "HYPERION",
    "Type": 0  // 0=Seller, 1=Bidder
  }
]
```

### API Endpoints for Player Export

#### Request Export (CSV)

```http
POST /export/jobs
Content-Type: application/json

{
  "ByEmail": "user@example.com",
  "ItemTag": "HYPERION",
  "Start": "2024-01-01T00:00:00Z",
  "End": "2024-03-01T00:00:00Z",
  "Users": ["player-uuid-1", "player-uuid-2"],
  "Filters": {
    "Reforge": "Heroic"
  },
  "Columns": ["Uuid", "End", "Price", "Seller", "Buyer"]
}
```

Response:
```json
{
  "JobId": "abc123...",
  "Status": "Pending",
  "SignedUrl": null
}
```

#### Check Export Status

```http
GET /export/jobs/{email}
```

#### Download Export (when complete)

The `SignedUrl` field in the response provides a pre-signed URL valid for 24 hours.

### Programmatic Player Auction Retrieval

To retrieve all auctions for a specific player:

```csharp
// Inject S3PlayerIndexService
var participations = await s3PlayerIndex.GetParticipation(playerGuid, year);

// Filter by type
var sold = participations.Where(p => p.Type == ParticipationType.Seller);
var bought = participations.Where(p => p.Type == ParticipationType.Bidder);

// Fetch full auction data
foreach (var entry in participations)
{
    var auction = await s3Blobs.ReadAuctions(entry.Tag, new DateTime(entry.End.Year, entry.End.Month, 1));
    var match = auction.FirstOrDefault(a => Guid.Parse(a.Uuid) == entry.AuctionUid);
}
```

---

## Filter Queries Against Item Tags

### Overview

The `QueryService.GetFiltered()` method supports flexible filtering with automatic routing to ScyllaDB or S3 based on the date range.

### API Endpoint

```http
GET /api/auctions/{itemTag}?EndAfter={timestamp}&EndBefore={timestamp}&filter1=value1&filter2=value2
```

### Query Flow

```
1. Parse time range (EndAfter, EndBefore)
2. Calculate timeKey range (week-based partitioning)
3. For each timeKey:
   a. Check if date >= scyllaCutoff (3 months ago)
   b. If yes → Query ScyllaDB
   c. If no → Query S3 blob for that month
4. Apply filter expression to results
5. Stream results back to caller
```

### Example: Query HYPERION Sales

```http
GET /api/auctions/HYPERION?EndAfter=1704067200&EndBefore=1709251200&Reforge=Heroic&Stars=5
```

This query:
1. Translates timestamps to `2024-01-01` → `2024-03-01`
2. For timeKeys in ScyllaDB range → direct query
3. For older timeKeys → fetches from S3 blobs
4. Applies filter: `Reforge == "Heroic" && Stars == 5`

### S3 Query Path Details

When data is older than 3 months:

```csharp
// S3 blob key format
var key = $"auctions/{tag}/{year:D4}-{month:D2}.json.gz";

// Example: auctions/HYPERION/2023-06.json.gz
```

The system:
1. Downloads the compressed blob (~50-200 MB for popular items)
2. Decompresses using GZip
3. Deserializes JSON to `List<SaveAuction>`
4. Applies filter expression in-memory
5. Streams matching results

### Filter Expression Examples

| Filter | Description |
|--------|-------------|
| `Reforge=Heroic` | Match reforge name |
| `Stars=5` | Match star count |
| `Enchantment=ultimate_one_for_all` | Has specific enchantment |
| `Bin=true` | Buy-it-now auctions only |
| `Seller=uuid` | Specific seller |

---

## Memory (RAM) Sizing for S3 Archival

### Memory Components

| Component | Memory Usage | Notes |
|-----------|--------------|-------|
| **Bloom Filter** | ~960 MB | 800M entries @ 1% FPR |
| **Per-Blob Processing** | ~200 MB peak | Decompression + deserialization |
| **Player Index Buffer** | ~50-100 MB | Before flush |
| **Migration Batch** | ~100 MB | 2000 auctions at a time |
| **HTTP/gRPC overhead** | ~100-200 MB | Connections, buffers |

### Recommended Pod Sizing

#### Minimum (Query-only)
- **Requests**: 512 Mi
- **Limits**: 1.5 Gi
- Suitable for: Read-only queries, no migration

#### Standard (Query + Migration)
- **Requests**: 1 Gi
- **Limits**: 2.5 Gi
- Suitable for: Active migration + normal queries

#### Heavy Migration
- **Requests**: 2 Gi
- **Limits**: 4 Gi
- Suitable for: Initial backfill, bulk operations

### Memory Calculation Details

#### Bloom Filter (Global UUID Index)

```
Optimal bits = -n * ln(p) / (ln(2)^2)
For n=800M entries, p=0.01 (1% FPR):
  bits = 800,000,000 * 9.6 ≈ 7.68 billion bits ≈ 960 MB
```

The bloom filter is loaded once at startup and kept in memory.

#### Per-Blob Memory (During Query/Migration)

A typical month blob for a popular tag:
- Compressed: 10-50 MB
- Decompressed: 50-200 MB
- Deserialized objects: 100-400 MB peak

Memory is released after processing each blob.

#### Player Index Buffering

The `S3PlayerIndexService` buffers participation entries before flushing:
- Flush every 20 batches (40,000 auctions)
- Peak buffer: ~50-100 MB depending on bid counts

### Kubernetes Resource Example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sky-auctions
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: sky-auctions
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2.5Gi"
            cpu: "2000m"
        env:
        - name: S3__ENABLED
          value: "true"
        - name: S3_MIGRATION__ENABLED
          value: "true"
        - name: S3_MIGRATION__DRY_RUN
          value: "true"  # Set to false after verification
```

### Memory Optimization Tips

1. **Disable migration during peak hours**: Set `S3_MIGRATION:ENABLED=false`
2. **Stream large queries**: Use `IAsyncEnumerable` endpoints
3. **Reduce bloom filter size**: If fewer than 800M auctions, adjust expected items
4. **Horizontal scaling**: Deploy multiple replicas with query-only config

---

## Monitoring

### Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `sky_auctions_s3_queries_total` | Counter | S3 blob reads |
| `sky_auctions_s3_migrated_total` | Counter | Auctions migrated to S3 |
| `sky_auctions_s3_verification_failures_total` | Counter | Migration verification failures |
| `sky_auctions_bloom_filter_size_bytes` | Gauge | Bloom filter memory usage |

### Health Checks

```http
GET /health
GET /ready
```

---

## API Reference

### Player Archive Endpoints (`/api/player/archive`)

These endpoints provide access to archived player data from S3.

#### Get Player Participation for Year

Retrieves all auctions where a player was involved (as seller or bidder) in a specific year.

```http
GET /api/player/archive/{playerUuid}/participation/{year}
```

Response:
```json
[
  {
    "auctionUid": "12345678-1234-1234-1234-123456789012",
    "end": "2024-03-15T10:30:00Z",
    "tag": "HYPERION",
    "type": 0
  }
]
```

#### Get Full Auctions for Player

Retrieves complete auction data for a player's participations.

```http
GET /api/player/archive/{playerUuid}/auctions/{year}?type=0&tag=HYPERION&limit=50
```

Parameters:
- `type`: 0=Seller, 1=Bidder (optional)
- `tag`: Filter by item tag (optional)
- `limit`: Max results (default 100)

#### Get Combined History (ScyllaDB + S3)

Retrieves auction history from both recent (ScyllaDB) and archived (S3) storage.

```http
GET /api/player/archive/{playerUuid}/history?startYear=2023&endYear=2024&type=0&limit=100
```

Response:
```json
{
  "playerUuid": "abc123...",
  "queryStartYear": 2023,
  "queryEndYear": 2024,
  "scyllaDbCount": 15,
  "s3ArchiveCount": 234,
  "recentAuctions": [...],
  "archivedParticipations": [...]
}
```

#### Check If Auction Exists (Bloom Filter)

Fast O(1) check if an auction might exist in the archive.

```http
GET /api/player/archive/auction/{auctionUuid}/exists
```

Response:
```json
{
  "auctionUuid": "abc123...",
  "bloomFilterAvailable": true,
  "mayExistInArchive": true
}
```

**Note**: `mayExistInArchive=false` means definitely not present. `mayExistInArchive=true` means possibly present (1% false positive rate).

---

## Migration Workflow

### 1. Enable Dry-Run Mode (Default)

```json
{
  "S3_MIGRATION": {
    "ENABLED": "true",
    "DRY_RUN": "true"
  }
}
```

This:
- Copies data to S3
- Verifies integrity
- Does NOT delete from ScyllaDB

### 2. Monitor Metrics

Watch for:
- `sky_auctions_s3_verification_failures_total` should be 0
- `sky_auctions_s3_migrated_total` increasing

### 3. Enable Deletion

After confidence in S3 storage:

```json
{
  "S3_MIGRATION": {
    "DRY_RUN": "false"
  }
}
```

### 4. Backfill Sources

Two backfill services are available:

- **ScyllaToS3BackfillService**: Migrates from ScyllaDB (`S3:BackfillScylla=true`)
- **MariaDbToS3BackfillService**: Migrates from MariaDB (`S3:BackfillMariaDb=true`)

### External CQL PEM TLS

Set all of `CASSANDRA:TLS:CA_CERT_PATH`, `CERT_PATH`, and `KEY_PATH` to opt in.
The certificate PEM includes its intermediate chain. `SERVER_NAME` is optional;
otherwise each contact IP is verified normally. The current legacy listener needs
explicit `SERVER_NAME=sky-client` and its CA in the trust bundle until its separate
server-certificate migration. Password authentication remains enabled.

Core 0.7.5 reloads projected PEM files on new Cassandra connections and rejects
broken replacements. Existing connections remain active. Mount without `subPath`
and provide a private writable ephemeral `HOME` for UID 1000: Cassandra 3.23 needs
validated public intermediates in the CurrentUser CA store to send the full chain.
No root or machine trust is installed. The callback ordering is pinned to that
driver version. In-flight valid identities can finish during renewal.

Without PEM configuration the existing PFX settings remain available, with strict
configured-root validation or normal OS validation when no root is configured.
`/health/ready` performs only `SELECT key FROM system.local`, bounded to five
seconds; the existing `/metrics` readiness probe and worker timings are unchanged.
Kafka, Redis, internal HTTP, SQL and archive TLS are outside this opt-in.

## R2 archive v2 fallbacks and the Scylla audit

Before old rows are deleted from MariaDB and Scylla every reader has to survive on the R2 archive v2 (`auctions/v2/...`, located through `auction_locations`):
- `GET`/`POST /api/auction/{uuid}`: Scylla, then `UuidLookupService` (MariaDB, Scylla, legacy S3 v1) and, as the last tier, the archive reader (same `SaveAuction` JSON).
- `GET /api/player/{uuid}/auctions`: when Scylla returns fewer than `amount` auctions, the archived sells that ended before `end` (via `auction_sellers`) are appended.
- `GET /api/player/archive/{uuid}/history`: archived sells/bids of the years asked for are added to `ArchivedParticipations`.
- Privacy export (`GET /api/privacy/player/{uuid}`): `ArchiveV2` lists the player's seller/bidder index entries (uid, end, tag, month, part). The erase of this service does not change the archive; `privacy erase` of SkyIndexer (`ArchivePlayerErasure`) does.
- The sweep refuses to write any part, tag index, manifest or marker of a month whose `archived_months` rows have `deleted_at` / `deleted_rows > 0` (metric `sky_auctions_archive_refused_deleted_month`), redo included.

`ScyllaArchiveAudit` (`ARCHIVE:AUDIT_ENABLED`, default off, read only) classifies every Scylla row of the months `AUDIT_MONTHS` (default: the sweep's range) against R2 as `equal`, `r2_better_superset`, `needs_merge`, `missing` or `bogus_end` under the equivalence table in `Services/Archive/ScyllaAuditEquivalence.cs`, at `AUDIT_CONSISTENCY` (default `ALL`), throttled by `AUDIT_PARTITIONS_PER_MINUTE`. Results: `auctions/v2/_meta/audit/scylla/{yyyy-MM}.msgpack.br` (per class and column counts, samples, per partition row count, End range, max WRITETIME and a SHA-256 of the rows; a 200 uid read-back through the public reader) and the metrics `sky_auctions_archive_audit_rows{class}`, `_audit_partitions_done`, `_audit_column_diffs{column}`. Progress: Redis `auctions_archive:v2:audit:{yyyy-MM}`; delete the key and the result object to audit a month again.

**Deletion gate semantics** (what a delete step may rely on):
- A Scylla partition is *covered* only if it is not `Unreadable` and every one of its rows is `equal` or `r2_better_superset`. One `needs_merge`, `missing`, `bogus_end` or unreadable row (or a row whose `serialisedbids` blob is corrupt: column `serialisedbids_unreadable`, class `needs_merge`) makes the whole partition not covered.
- A partition that spans several months is covered only when every month it overlaps passes; a single failing month blocks it.
- `bogus_end` rows (End before 2019-06, the random-timekey garbage) are never covered, even when R2 has a matching row.
- A set `CompletedAt` does **not** mean the month is clean: check `ClassCounts["unreadable"]` (unreadable partitions are retried in the next pass) and the Redis marker (`done:{iso}` is clean, `unreadable:{n}:{iso}` is not, `progress:...` is still running).
- The proof is against the tag index entry's `PartSha256` (the audit verifies the located block against it), **not** against `archived_months.sha256` of MariaDB. Known gap: the two are not cross-checked by the audit.

`GET /api/player/{uuid}/auctions` and `GET /api/player/archive/{uuid}/history` bound their archive part by `ARCHIVE:READ_TIMEOUT_MS` (default 3000): on timeout or failure they answer with the non-archive result and the response header `X-Archive-Warning: timeout|failed`.
