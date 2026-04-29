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
