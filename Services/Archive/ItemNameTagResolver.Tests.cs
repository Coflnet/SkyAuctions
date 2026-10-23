using System.Linq;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Archive;

public class ItemNameTagResolverTests
{
    private static ItemNameTagResolver Of(params (string, string)[] pairs) => new(pairs);

    [TestCase("§6Hyperion §d✪✪✪", "Hyperion")]
    [TestCase("  Hyperion ✪✪✪✪✪  ", "Hyperion")]
    [TestCase("✪ Hyperion", "Hyperion")]
    [TestCase("[Lvl 42] Wolf", "Wolf")]
    [TestCase("§7[Lvl 1] §fWolf", "Wolf")]
    [TestCase("Spicy   Leaping  Sword", "Spicy Leaping Sword")]
    public void Normalise_StripsDecoration(string input, string expected)
        => Assert.That(ItemNameTagResolver.Normalise(input), Is.EqualTo(expected));

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("§6✪")]
    public void Normalise_ReturnsNullForEmpty(string input)
        => Assert.That(ItemNameTagResolver.Normalise(input), Is.Null);

    [Test]
    public void ResolvesUniqueNames_ViaItemsAndAlternativeNames()
    {
        var resolver = Of(("Hyperion", "HYPERION"), ("Old Hyperion", "HYPERION"), ("Wolf", "WOLF"));
        Assert.That(resolver.TryResolve("§6Hyperion ✪", out var tag), Is.True);
        Assert.That(tag, Is.EqualTo("HYPERION"));
        Assert.That(resolver.TryResolve("Old Hyperion", out tag), Is.True);
        Assert.That(tag, Is.EqualTo("HYPERION"));
        Assert.That(resolver.TryResolve("[Lvl 42] Wolf", out tag), Is.True);
        Assert.That(tag, Is.EqualTo("WOLF"));
        Assert.That(resolver.TryResolve("Nothing", out tag), Is.False);
        Assert.That(tag, Is.Null);
    }

    [Test]
    public void AmbiguousNames_NeverResolve_EvenViaReforgeFallback()
    {
        var resolver = Of(("Clay", "CLAY"), ("Clay", "CLAY_BALL"), ("Clay", "CLAY"), ("Sword", "SWORD"));
        Assert.That(resolver.AmbiguousNames, Is.EqualTo(1));
        Assert.That(resolver.TryResolve("Clay", out _), Is.False);
        Assert.That(resolver.TryResolve("Spicy Clay", out _), Is.False);
    }

    [Test]
    public void SamePairRepeated_StaysUnique()
    {
        var resolver = Of(("Wolf", "WOLF"), ("Wolf", "WOLF"));
        Assert.That(resolver.TryResolve("Wolf", out var tag) && tag == "WOLF", Is.True);
    }

    [Test]
    public void ReforgePrefix_IsStrippedOnlyForReforgeWords()
    {
        var resolver = Of(("Leaping Sword", "LEAPING_SWORD"));
        Assert.That(resolver.TryResolve("Spicy Leaping Sword", out var tag), Is.True);
        Assert.That(tag, Is.EqualTo("LEAPING_SWORD"));
        Assert.That(resolver.TryResolve("spicy Leaping Sword", out _), Is.True, "case-insensitive");
        Assert.That(resolver.TryResolve("Banana Leaping Sword", out _), Is.False, "not a reforge");
    }

    [Test]
    public void NeverResolvesToUnknown_AndIgnoresNullNames()
    {
        var resolver = Of(("Mystery", "unknown"), ("Other", "UNKNOWN"), ("Blank", null), ("Empty", ""), ("null", "NULL_ITEM"), (null, "X"));
        Assert.That(resolver.UniqueNames, Is.Zero);
        Assert.That(resolver.TryResolve("Mystery", out _), Is.False);
        Assert.That(resolver.TryResolve("null", out _), Is.False);
    }
}
