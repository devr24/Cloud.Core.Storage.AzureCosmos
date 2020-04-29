using Newtonsoft.Json;

namespace Cloud.Core.Storage.AzureCosmos
{
    /// <summary>
    /// Class CountItem.
    /// Implements the <see cref="Cloud.Core.ITableItem" />
    /// </summary>
    /// <seealso cref="Cloud.Core.ITableItem" />
    internal class CountItem : ITableItem
    {
        /// <summary>
        /// Gets or sets the identifier key.
        /// </summary>
        /// <value>The key.</value>
        public string Key { get; set; }

        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty("id")]
        public string Id { get; set; }
    }
}
