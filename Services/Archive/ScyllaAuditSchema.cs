using System;
using System.Collections.Generic;
using System.Linq;

namespace Coflnet.Sky.Auctions.Archive;

/// <summary>One row of <c>system_schema.columns</c>.</summary>
/// <param name="Name">Column name.</param>
/// <param name="Kind">partition_key, clustering or regular.</param>
/// <param name="Type">CQL type, e.g. <c>frozen&lt;map&lt;text, text&gt;&gt;</c>.</param>
public sealed record ScyllaColumnInfo(string Name, string Kind, string Type);

/// <summary>
/// The columns of <c>sky_auctions.weekly_auctions_2</c> the audit knows how to compare, checked against the live schema before any row is
/// read: a column the audit does not know could hold data that is compared nowhere, so the audit refuses to run (a "covered" verdict would be a lie).
/// Also builds the audit's SELECT statements, including the <c>WRITETIME</c> columns that make the partition hash notice later writes.
/// </summary>
public sealed class ScyllaAuditSchema
{
    /// <summary>Keyspace of the table.</summary>
    public const string Keyspace = "sky_auctions";
    /// <summary>The audited table.</summary>
    public const string Table = "weekly_auctions_2";

    /// <summary>CQL that reads the live column list.</summary>
    public const string ColumnsCql = "SELECT column_name, kind, type FROM system_schema.columns WHERE keyspace_name = '" + Keyspace + "' AND table_name = '" + Table + "'";

    /// <summary>Every column the audit selects and compares (<c>count</c> and <c>end</c> are CQL keywords and are quoted in the statements).</summary>
    public static readonly string[] KnownColumns =
    {
        "tag", "timekey", "issold", "end", "auctionuid", "uuid", "itemname", "itemlore", "extra", "category", "tier", "startingbid", "highestbidamount", "bin", "start", "itemcreatedat",
        "auctioneer", "profileid", "profilename", "coop", "coopname", "highestbidder", "highestbiddername", "extraattributesjson", "nbtlookup", "itembytes", "itemuid", "itemid", "enchantments",
        "serialisedbids", "count", "color",
    };

    /// <summary>Regular columns whose <c>WRITETIME</c> goes into the partition hash (the largest value of a row is hashed).</summary>
    public static readonly string[] WriteTimeColumns = { "serialisedbids", "itembytes", "auctioneer", "nbtlookup", "highestbidamount" };

    private static readonly string[] CqlKeywords = { "end", "count" };

    /// <summary>Live columns the audit does not know (the audit must not run).</summary>
    public IReadOnlyList<string> Unknown { get; private init; } = Array.Empty<string>();
    /// <summary>Known columns the live table lacks (selecting them would fail).</summary>
    public IReadOnlyList<string> Absent { get; private init; } = Array.Empty<string>();
    /// <summary>Columns whose <c>WRITETIME</c> is selected, in alias order <c>wt0, wt1, ...</c>.</summary>
    public IReadOnlyList<string> WriteTimeSelected { get; private init; } = Array.Empty<string>();
    /// <summary>Wanted write-time columns that can not be asked for (absent, not regular, or a non-frozen collection).</summary>
    public IReadOnlyList<string> WriteTimeSkipped { get; private init; } = Array.Empty<string>();
    /// <summary>The partition scan statement.</summary>
    public string PartitionCql { get; private init; }
    /// <summary>The by-uid statement of the sample read-back.</summary>
    public string UidCql { get; private init; }

    /// <summary>True when the audit may run.</summary>
    public bool IsUsable => Unknown.Count == 0 && Absent.Count == 0;

    /// <summary>Human readable reason the schema is not usable.</summary>
    public string Problem => $"unknown columns [{string.Join(", ", Unknown)}], missing columns [{string.Join(", ", Absent)}]";

    /// <summary>Plan the statements for the live column list.</summary>
    public static ScyllaAuditSchema Build(IEnumerable<ScyllaColumnInfo> columns)
    {
        var live = columns.ToDictionary(c => c.Name, StringComparer.Ordinal);
        var writeTimes = WriteTimeColumns.Where(name => live.TryGetValue(name, out var info) && CanAskWriteTime(info)).ToList();
        var select = string.Join(", ", KnownColumns.Select(Quote).Concat(writeTimes.Select((name, i) => $"WRITETIME({name}) AS wt{i}")));
        return new ScyllaAuditSchema
        {
            Unknown = live.Keys.Except(KnownColumns, StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal).ToList(),
            Absent = KnownColumns.Except(live.Keys, StringComparer.Ordinal).ToList(),
            WriteTimeSelected = writeTimes,
            WriteTimeSkipped = WriteTimeColumns.Except(writeTimes).ToList(),
            PartitionCql = $"SELECT {select} FROM {Table} WHERE tag = ? AND timekey = ? BYPASS CACHE",
            UidCql = $"SELECT {select} FROM {Table} WHERE auctionuid = ?",
        };
    }

    /// <summary>Quote a column name when it is a CQL keyword.</summary>
    public static string Quote(string column) => CqlKeywords.Contains(column) ? "\"" + column + "\"" : column;

    private static bool CanAskWriteTime(ScyllaColumnInfo info)
        => info.Kind == "regular" && !IsMultiCellCollection(info.Type);

    private static bool IsMultiCellCollection(string type)
        => type.StartsWith("map<", StringComparison.Ordinal) || type.StartsWith("set<", StringComparison.Ordinal) || type.StartsWith("list<", StringComparison.Ordinal);
}
