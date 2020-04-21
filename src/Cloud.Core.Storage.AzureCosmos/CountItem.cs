using Newtonsoft.Json;

namespace Cloud.Core.Storage.AzureCosmos
{
    public class CountItem : ITableItem
    {
        public string Key { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }
    }
}
