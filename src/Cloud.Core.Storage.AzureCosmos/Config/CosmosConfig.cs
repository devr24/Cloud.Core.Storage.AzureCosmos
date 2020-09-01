namespace Cloud.Core.Storage.AzureCosmos.Config
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using System.Linq;

    /// <summary>
    /// Class Configuration Base.
    /// Implements the <see cref="Validation.AttributeValidator" />
    /// </summary>
    /// <seealso cref="Validation.AttributeValidator" />
    public abstract class ConfigBase : Validation.AttributeValidator
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
        /// Gets or sets the database identifier.
        /// </summary>
        /// <value>
        /// The database identifier.
        /// </value>
        [Required]
        public string DatabaseName { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether [create if not exists].
        /// </summary>
        /// <value><c>true</c> if [create if not exists]; otherwise, <c>false</c>.</value>
        public bool CreateDatabaseIfNotExists { get; set; }

        /// <summary>
        /// Gets or sets the list of tables to be created during initialisation.
        /// </summary>
        /// <value>The list of tables to create.</value>
        public string[] CreateTables { get; set; }
    }

    /// <summary>
    /// Msi Configuration for Azure KeyVault.
    /// </summary>
    public class MsiConfig : ConfigBase
    {
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
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return $"TenantId: {TenantId}, SubscriptionId: {SubscriptionId}, Cosmos InstanceName: {InstanceName}, Database: {DatabaseName}";
        }
    }

    /// <summary>Connection string config.</summary>
    public class ConnectionConfig : ConfigBase
    {
        /// <summary>
        /// Gets or sets the connection string for connecting to storage.
        /// </summary>
        /// <value>
        /// Storage connection string.
        /// </value>
        [Required]
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets the name of the instance.
        /// </summary>
        /// <value>The name of the instance.</value>
        public new string InstanceName
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

                // Account name is used as the identifier.
                return parts
                    .FirstOrDefault(p => p.StartsWith(replaceStart))?.Replace(replaceStart, string.Empty).Replace(replaceEnd, string.Empty);
            }
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return $"Cosmos InstanceName: {InstanceName}, Database: {DatabaseName}";
        }
    }

    /// <summary>
    /// Service Principle Configuration for Azure KeyVault.
    /// </summary>
    public class ServicePrincipleConfig : ConfigBase
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
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return $"AppId: {AppId}, TenantId: {TenantId}, SubscriptionId: {SubscriptionId}, Cosmos InstanceName: {InstanceName}, Database: {DatabaseName}";
        }
    }
}
