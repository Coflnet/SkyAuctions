using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Coflnet.Sky.Auctions.Models;

[DataContract]
public class QueryArchive : PriceSumary
{
    /// <summary>
    /// The tag of the item
    /// </summary>
    public string Tag { get; set; }
    /// <summary>
    /// The filters used for this query
    /// </summary>
    public Dictionary<string, string> Filters { get; set; }
    /// <summary>
    /// Hashkey of the filters
    /// </summary>
    public string FilterKey { get; set; }
    /// <summary>
    /// Start of the period
    /// </summary>
    [DataMember(Name = "start")]
    public DateTime Start { get; set; }
    /// <summary>
    /// End of the period
    /// </summary>
    [DataMember(Name = "end")]
    public DateTime End { get; set; }
}