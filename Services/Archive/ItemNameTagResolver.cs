using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;

namespace Coflnet.Sky.Auctions.Archive
{
    /// <summary>
    /// Maps the display name of an auction to its item tag. Used by the Scylla sweep to give the rows of the Scylla tag
    /// <c>unknown</c> (auctions of 2019-11 .. 2020-05 whose tag was never saved) their real tag. A name resolves only when every
    /// known (name, tag) pair of its normalised form agrees on a single tag; ambiguous names (e.g. "Clay", "Water Bottle") never resolve.
    /// </summary>
    public class ItemNameTagResolver
    {
        private static readonly Regex ColourCodes = new("§.", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private static readonly Regex LevelPrefix = new(@"^\[Lvl \d+\]\s*", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private static readonly Regex Spaces = new(@"\s{2,}", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private static readonly HashSet<string> ReforgeWords = new(
            Enum.GetNames<ItemReferences.Reforge>().Where(n => n != nameof(ItemReferences.Reforge.None)), StringComparer.OrdinalIgnoreCase);

        private readonly Dictionary<string, string> unique = new(StringComparer.Ordinal);
        private readonly HashSet<string> ambiguous = new(StringComparer.Ordinal);

        /// <summary>Resolver without any known name (nothing resolves).</summary>
        public ItemNameTagResolver() : this(Array.Empty<(string, string)>()) { }

        /// <summary>Build from (name, tag) pairs: Items.Name to Items.Tag and AltItemNames.Name to the Items.Tag of its item.</summary>
        public ItemNameTagResolver(IEnumerable<(string Name, string Tag)> pairs)
        {
            foreach (var (name, tag) in pairs)
            {
                if (string.IsNullOrEmpty(tag) || string.Equals(tag, "unknown", StringComparison.OrdinalIgnoreCase))
                    continue;
                var key = Normalise(name);
                if (key == null || key.Equals("null", StringComparison.OrdinalIgnoreCase))
                    continue;
                if (ambiguous.Contains(key))
                    continue;
                if (!unique.TryGetValue(key, out var existing))
                    unique[key] = tag;
                else if (existing != tag)
                {
                    unique.Remove(key);
                    ambiguous.Add(key);
                }
            }
        }

        /// <summary>Names that map to a single tag.</summary>
        public int UniqueNames => unique.Count;
        /// <summary>Names that map to several tags.</summary>
        public int AmbiguousNames => ambiguous.Count;

        /// <summary>
        /// Display name without colour codes (<c>§x</c>), leading/trailing stars, a leading <c>[Lvl N] </c> pet prefix and double spaces;
        /// null for null/empty.
        /// </summary>
        public static string Normalise(string itemName)
        {
            if (string.IsNullOrWhiteSpace(itemName))
                return null;
            var text = ColourCodes.Replace(itemName, "").Trim();
            text = text.Trim('✪', ' ', '\t').Trim();
            text = LevelPrefix.Replace(text, "");
            text = Spaces.Replace(text, " ").Trim();
            return text.Length == 0 ? null : text;
        }

        /// <summary>
        /// The tag of an item name: exact normalised lookup first, then, if the first word is a reforge, the rest of the name.
        /// False when unknown or ambiguous. Never yields <c>unknown</c>.
        /// </summary>
        public bool TryResolve(string itemName, out string tag)
        {
            tag = null;
            var name = Normalise(itemName);
            if (name == null)
                return false;
            if (unique.TryGetValue(name, out tag))
                return true;
            if (ambiguous.Contains(name))
                return false;
            var space = name.IndexOf(' ');
            if (space > 0 && ReforgeWords.Contains(name[..space]))
            {
                var rest = name[(space + 1)..].TrimStart();
                if (rest.Length > 0 && unique.TryGetValue(rest, out tag))
                    return true;
            }
            tag = null;
            return false;
        }

        /// <summary>Read the Items and AltItemNames tables (read only) into a resolver.</summary>
        public static async Task<ItemNameTagResolver> LoadAsync(HypixelContext db, CancellationToken cancellationToken)
        {
            var items = await db.Items.AsNoTracking().Select(i => new { i.Id, i.Tag, i.Name }).ToListAsync(cancellationToken).ConfigureAwait(false);
            var tagById = items.Where(i => i.Tag != null).ToDictionary(i => i.Id, i => i.Tag);
            var alt = await db.AltItemNames.AsNoTracking().Select(a => new { a.Name, a.DBItemId }).ToListAsync(cancellationToken).ConfigureAwait(false);
            var pairs = items.Select(i => (i.Name, i.Tag))
                .Concat(alt.Where(a => tagById.ContainsKey(a.DBItemId)).Select(a => (a.Name, tagById[a.DBItemId])));
            return new ItemNameTagResolver(pairs);
        }
    }
}
