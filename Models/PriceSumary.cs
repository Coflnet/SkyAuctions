using System.Runtime.Serialization;

namespace Coflnet.Sky.Auctions.Models;

[DataContract]
public class PriceSumary
{
    [DataMember(Name = "max")]
    public long Max { get; set; }
    [DataMember(Name = "min")]
    public long Min;
    [DataMember(Name = "median")]
    public long Med;
    [DataMember(Name = "mean")]
    public double Mean;
    [DataMember(Name = "mode")]
    public long Mode;
    [DataMember(Name = "volume")]
    public double Volume;
}