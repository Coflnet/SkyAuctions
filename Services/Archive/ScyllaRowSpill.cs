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
    /// Append-only buffer of mapped Scylla rows of one tag-month, in memory up to <see cref="DiskBackedStream.DefaultThreshold"/>
    /// and in a temp file (deleted on dispose) beyond. Rows can only be cut into parts after the whole tag-month was read,
    /// because a later partition may hold a better copy of the same auction (dedupe); an ENCHANTED_BOOK month does not fit the pod's memory.
    /// Each record is a one-row <see cref="ArchivedAuctionBlock"/>, so the round trip is exactly the archive's own.
    /// </summary>
    internal sealed class ScyllaRowSpill : IDisposable
    {
        private readonly string tag;
        private readonly int month;
        private readonly DiskBackedStream stream = new();
        private readonly BinaryWriter writer;

        public ScyllaRowSpill(string tag, int month)
        {
            this.tag = tag;
            this.month = month;
            writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        }

        /// <summary>Records written.</summary>
        public int Count { get; private set; }

        /// <summary>Append a row; returns its sequence number.</summary>
        public int Add(SaveAuction row, ScyllaAuctionExtras extras)
        {
            var block = ArchivedAuctionBlock.FromAuctions(tag, month, 0, ArchivedAuctionBlock.SourceScylla, new[] { (row, extras) });
            var bytes = MessagePackSerializer.Serialize(block);
            writer.Write(bytes.Length);
            writer.Write(bytes);
            return Count++;
        }

        /// <summary>Every record in write order (single pass; ends the writing phase).</summary>
        public IEnumerable<(int Seq, SaveAuction Row, ScyllaAuctionExtras Extras)> ReadAll()
        {
            writer.Flush();
            stream.Position = 0;
            using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
            for (var seq = 0; seq < Count; seq++)
            {
                var length = reader.ReadInt32();
                var block = MessagePackSerializer.Deserialize<ArchivedAuctionBlock>(reader.ReadBytes(length));
                yield return (seq, block.ToAuctions().Single(), block.ToExtras()[0]);
            }
        }

        public void Dispose()
        {
            writer.Dispose();
            stream.Dispose();
        }
    }
}
