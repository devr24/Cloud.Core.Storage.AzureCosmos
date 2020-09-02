namespace Microsoft.Extensions.DependencyInjection
{
    using System;
    using Cloud.Core;
    using Cloud.Core.Storage.AzureCosmos;
    using Cloud.Core.Storage.AzureCosmos.Config;

    /// <summary>
    /// Class Service Collection extensions.
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Adds an instance of Azure cosmos storage as a singleton with a specific instance name, using managed user config to setup.  Requires the instance 
        /// name, TenantId and SubscriptionId to be supplied.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="key">The key to use when looking up the instance from the factory.</param>
        /// <param name="instanceName">Name of the table storage instance to connect to.</param>
        /// <param name="tenantId">Tenant Id the instance lives in.</param>
        /// <param name="subscriptionId">Subscription Id for the tenant.</param>
        /// <param name="databaseName">The name of the database</param>
        /// <param name="createDbIfNotExists">Create the database and tables if they don't already exists.</param>
        /// <param name="createTables">Set a list of table names that are to be created on initialisation of the Cosmos client.</param> 
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddCosmosStorageSingletonNamed(this IServiceCollection services, string key, string instanceName, string tenantId, string subscriptionId, string databaseName, bool createDbIfNotExists = true, string[] createTables = null)
        {
            var instance = new CosmosStorage(new MsiConfig
            {
                InstanceName = instanceName,
                TenantId = tenantId,
                SubscriptionId = subscriptionId,
                DatabaseName = databaseName,
                CreateDatabaseIfNotExists = createDbIfNotExists,
                CreateTables = createTables
            });

            if (!key.IsNullOrEmpty())
                instance.Name = key;

            services.AddSingleton<ITableStorage>(instance);
            services.AddFactoryIfNotAdded<ITableStorage>();
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure cosmos storage as a singleton, using managed user config to setup.  Requires the instance 
        /// name, TenantId and SubscriptionId to be supplied.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="instanceName">Name of the table storage instance to connect to.</param>
        /// <param name="tenantId">Tenant Id the instance lives in.</param>
        /// <param name="subscriptionId">Subscription Id for the tenant.</param>
        /// <param name="databaseName">The name of the database</param>
        /// <param name="createDbIfNotExists">Create the database and tables if they don't already exists.</param>
        /// <param name="createTables">Set a list of table names that are to be created on initialisation of the Cosmos client.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddCosmosStorageSingleton(this IServiceCollection services, string instanceName, string tenantId, string subscriptionId, string databaseName, bool createDbIfNotExists = true, string[] createTables = null)
        {
            services.AddCosmosStorageSingleton(new MsiConfig
            {
                InstanceName = instanceName,
                TenantId = tenantId,
                SubscriptionId = subscriptionId,
                DatabaseName = databaseName,
                CreateDatabaseIfNotExists = createDbIfNotExists,
                CreateTables = createTables
            });
            services.AddFactoryIfNotAdded<ITableStorage>();
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure cosmos storage as a singleton, using managed user config to setup.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="config">The configuration to initialise with.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddCosmosStorageSingleton(this IServiceCollection services, MsiConfig config)
        {
            services.AddSingleton<ITableStorage>(new CosmosStorage(config));
            services.AddFactoryIfNotAdded<ITableStorage>();
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure cosmos storage as a singleton, using service principle config to setup.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="config">The configuration to initialise with.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddCosmosStorageSingleton(this IServiceCollection services, ServicePrincipleConfig config)
        {
            services.AddSingleton<ITableStorage>(new CosmosStorage(config));
            services.AddFactoryIfNotAdded<ITableStorage>();
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure cosmos storage as a singleton, using connection string config to setup.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="config">The configuration to initialise with.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddCosmosStorageSingleton(this IServiceCollection services, ConnectionConfig config)
        {
            services.AddSingleton<ITableStorage>(new CosmosStorage(config));
            services.AddFactoryIfNotAdded<ITableStorage>();
            return services;
        }
    }
}
