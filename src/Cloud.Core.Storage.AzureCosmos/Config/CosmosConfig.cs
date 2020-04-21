namespace Cloud.Core.Storage.AzureCosmos.Config
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using System.Linq;

    /// <summary>
    /// Msi Configuration for Azure KeyVault.
    /// </summary>
    public class MsiConfig : Validation.AttributeValidator
    {
        /// <summary>
        /// Gets or sets the name of the cosmos instance.
        /// </summary>
        /// <value>
        /// The name of the key vault instance.
        /// </value>
        [Required]
        public string InstanceName { get; set; }

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>
        /// The tenant identifier.
        /// </value>        
        [Required]
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>
        /// The subscription identifier.
        /// </value>
        [Required]
        public string SubscriptionId { get; set; }

        /// <summary>
        /// Gets or sets the database identifier.
        /// </summary>
        /// <value>
        /// The database identifier.
        /// </value>
        [Required]
        public string DatabaseName { get; set; }
    }

    /// <summary>Connection string config.</summary>
    public class ConnectionConfig : Validation.AttributeValidator
    {
        /// <summary>
        /// Gets or sets the connection string for connecting to storage.
        /// </summary>
        /// <value>
        /// Storage connection string.
        /// </value>
        [Required]
        public string ConnectionString { get; set; }

        public string InstanceName
        {
            get
            {             
                if (ConnectionString.IsNullOrEmpty())
                    return null;

                const string replaceStart = "AccountEndpoint=https://";
                const string replaceEnd = ".documents.azure.com:443/";

                var parts = ConnectionString.Split(';');

                if (parts.Length <= 1)
                    return null;

                // Account name is used as the indentifier.
                return parts.Where(p => p.StartsWith(replaceStart))
                    .FirstOrDefault()?.Replace(replaceStart, string.Empty).Replace(replaceEnd, string.Empty);
            }
        }

        [Required]
        public string DatabaseName { get; set; }
    }

    /// <summary>
    /// Service Principle Configuration for Azure KeyVault.
    /// </summary>
    public class ServicePrincipleConfig : Validation.AttributeValidator
    {
        /// <summary>
        /// Gets or sets the application identifier.
        /// </summary>
        /// <value>
        /// The application identifier.
        /// </value>
        [Required]
        public string AppId { get; set; }

        /// <summary>
        /// Gets or sets the application secret.
        /// </summary>
        /// <value>
        /// The application secret string.
        /// </value>
        [Required]
        public string AppSecret { get; set; }

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>
        /// The tenant identifier.
        /// </value>
        [Required]
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>
        /// The subscription identifier.
        /// </value>
        [Required]
        public string SubscriptionId { get; set; }

        /// <summary>
        /// Gets or sets the name of the storage instance.
        /// </summary>
        /// <value>
        /// The name of the storage instance.
        /// </value>
        [Required]
        public string InstanceName { get; set; }

        /// <summary>
        /// Gets or sets the database identifier.
        /// </summary>
        /// <value>
        /// The database identifier.
        /// </value>
        [Required]
        public string DatabaseName { get; set; }
        
        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return $"AppId: {AppId}, TenantId: {TenantId}, Cosmos InstanceName: {InstanceName}";
        }
    }
}
