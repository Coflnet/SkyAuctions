using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using MessagePack;

namespace Coflnet.Sky.Auctions.Archive
{
    /// <summary>
    /// Disk-backed buffer of the rows of the Scylla tag <c>unknown</c> that were resolved to a real tag (see <see cref="ItemNameTagResolver"/>).
    /// Record layout, appended to one <see cref="DiskBackedStream"/>: five fingerprint fields (highest bid, sold, end ticks, item
    /// bytes length, starting bid), then an int length and a one-row <see cref="ArchivedAuctionBlock"/> (tag = resolved tag, month,
    /// part 0, source scylla) holding the already mapped row and its extras. An in-memory index keeps the record offsets per resolved tag,
    /// so a tag-month reads exactly its own rows.
    /// </summary>
    internal sealed class RetagSpill : IDisposable
    {
        private readonly int month;
        private readonly DiskBackedStream stream = new();
        private readonly Dictionary<string, List<long>> offsets = new(StringComparer.Ordinal);

        public RetagSpill(int month) => this.month = month;

        /// <summary>Records written.</summary>
        public int Count { get; private set; }

        /// <summary>Resolved tags that have rows, ordinally sorted.</summary>
        public IReadOnlyList<string> Tags => offsets.Keys.OrderBy(t => t, StringComparer.Ordinal).ToList();

        /// <summary>Append a mapped row (its <see cref="SaveAuction.Tag"/> is the resolved tag).</summary>
        public void Add(SaveAuction row, ScyllaAuctionExtras extras, Fingerprint fp)
        {
            var block = ArchivedAuctionBlock.FromAuctions(row.Tag, month, 0, ArchivedAuctionBlock.SourceScylla, new[] { (row, extras) });
            var bytes = MessagePackSerializer.Serialize(block);
            if (!offsets.TryGetValue(row.Tag, out var list))
                offsets[row.Tag] = list = new List<long>();
            stream.Position = stream.Length;
            list.Add(stream.Position);
            using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
            writer.Write(fp.HighestBidAmount);
            writer.Write(fp.IsSold);
            writer.Write(fp.EndTicks);
            writer.Write(fp.ItemBytesLength);
            writer.Write(fp.StartingBid);
            writer.Write(bytes.Length);
            writer.Write(bytes);
            Count++;
        }

        /// <summary>Rows of one resolved tag in write order (empty for other tags).</summary>
        public IEnumerable<(SaveAuction Row, ScyllaAuctionExtras Extras, Fingerprint Fp)> ReadTag(string tag)
        {
            if (!offsets.TryGetValue(tag, out var list))
                yield break;
            foreach (var offset in list)
            {
                stream.Position = offset;
                using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
                var fp = new Fingerprint(reader.ReadInt64(), reader.ReadBoolean(), reader.ReadInt64(), reader.ReadInt32(), reader.ReadInt64());
                var block = MessagePackSerializer.Deserialize<ArchivedAuctionBlock>(reader.ReadBytes(reader.ReadInt32()));
                yield return (block.ToAuctions().Single(), block.ToExtras()[0], fp);
            }
        }

        public void Dispose() => stream.Dispose();
    }
}
