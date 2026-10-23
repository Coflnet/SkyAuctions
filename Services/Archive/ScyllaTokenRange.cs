using System;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;

namespace Coflnet.Sky.Auctions.Archive
{
    /// <summary>
    /// A slice of the Murmur3 token ring for <c>token(tag, timekey)</c> restrictions: <c>(Start, End]</c>, or <c>[Start, End]</c>
    /// for the first slice of the ring (<see cref="StartInclusive"/>). Bound values are <c>long</c> (CQL bigint), as the
    /// <c>token()</c> markers expect.
    /// </summary>
    public readonly record struct ScyllaTokenRange(long Start, bool StartInclusive, long End)
    {
        private static readonly BigInteger RingSize = BigInteger.Pow(2, 64);

        /// <summary>
        /// The whole ring [long.MinValue, long.MaxValue] cut into <paramref name="count"/> consecutive slices of (almost) equal
        /// width; every token lies in exactly one slice.
        /// </summary>
        public static IReadOnlyList<ScyllaTokenRange> Split(int count)
        {
            if (count < 1)
                throw new ArgumentOutOfRangeException(nameof(count));
            var bounds = new long[count + 1];
            bounds[0] = long.MinValue;
            bounds[count] = long.MaxValue;
            for (var i = 1; i < count; i++)
                bounds[i] = (long)(long.MinValue + RingSize * i / count);
            var result = new ScyllaTokenRange[count];
            for (var i = 0; i < count; i++)
                result[i] = new ScyllaTokenRange(bounds[i], i == 0, bounds[i + 1]);
            return result;
        }

        /// <summary>True if the range can be halved (it holds more than one token).</summary>
        public bool CanSplit => (BigInteger)End - Start > 1;

        /// <summary>The two halves; together they hold exactly the tokens of this range.</summary>
        public (ScyllaTokenRange Low, ScyllaTokenRange High) Halve()
        {
            if (!CanSplit)
                throw new InvalidOperationException("Range " + this + " can not be split");
            var mid = (long)((BigInteger)Start + ((BigInteger)End - Start) / 2);
            return (new ScyllaTokenRange(Start, StartInclusive, mid), new ScyllaTokenRange(mid, false, End));
        }

        /// <summary>True if <paramref name="token"/> lies in the range.</summary>
        public bool Contains(long token) => (StartInclusive ? token >= Start : token > Start) && token <= End;

        /// <inheritdoc/>
        public override string ToString()
            => (StartInclusive ? "[" : "(") + Start.ToString(CultureInfo.InvariantCulture) + ", " + End.ToString(CultureInfo.InvariantCulture) + "]";
    }
}
