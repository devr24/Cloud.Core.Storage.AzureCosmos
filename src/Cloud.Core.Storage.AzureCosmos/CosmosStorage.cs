namespace Cloud.Core.Storage.AzureCosmos
{
    using System.Reactive.Linq;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Diagnostics.CodeAnalysis;
    using Microsoft.Azure.Management.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Microsoft.Rest;
    using Microsoft.Rest.TransientFaultHandling;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.Extensions.Logging;
    using Config;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.Fluent;
    using System.Threading;
    using System.Collections.Concurrent;

    /// <summary>
    /// Azure specific implementation of cloud cosmos storage.
    /// </summary>
    public class CosmosStorage : CosmosStorageBase, ITableStorage
    {
        private const string DefaultPartitionKeyPath = "/_partitionKey";

        /// <summary>
        /// Initializes a new instance of <see cref="CosmosStorage" /> with Service Principle authentication.
        /// </summary>
        /// <param name="config">The Service Principle configuration settings for connecting to storage.</param>
        /// <param name="logger">The logger to log information to.</param>
        /// <inheritdoc />
        public CosmosStorage([NotNull]ServicePrincipleConfig config, [MaybeNull] ILogger logger = null)
            : base(config, logger) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosStorage" /> class with a Connection String.
        /// </summary>
        /// <param name="config">The Connection String information for connecting to Storage.</param>
        /// <param name="logger">The Logger?.</param>
        public CosmosStorage([NotNull]ConnectionConfig config, [MaybeNull] ILogger logger = null)
            : base(config, logger) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosStorage" /> class with Managed Service Identity (MSI) authentication.
        /// </summary>
        /// <param name="config">The Managed Service Identity (MSI) configuration for connecting to storage.</param>
        /// <param name="logger">The Logger?.</param>
        public CosmosStorage([NotNull]MsiConfig config, [MaybeNull]ILogger logger = null)
            : base(config, logger) { }

        /// <summary>
        /// Gets the entity from the requested table, using the key identifier.
        /// </summary>
        /// <typeparam name="T">Type of object returned.</typeparam>
        /// <param name="tableName">Name of the table to search.</param>
        /// <param name="key">The key, used to find the entity.</param>
        /// <returns>Task of type T.</returns>
        /// <inheritdoc cref="ITableStorage.GetEntity{T}"/>
        public async Task<T> GetEntity<T>(string tableName, string key) where T : class, ITableItem, new()
        {
            var cosmosContainer = CosmosClient.GetContainer(DatabaseName, tableName);
            var partitionKey = ExtractPartitionKey(key, out string modifiedKey);
            key = modifiedKey;
            try
            {
                return await cosmosContainer.ReadItemAsync<T>(key, partitionKey);
            }
            catch (CosmosException)
            {
                return null;
            }
        }

        /// <summary>
        /// Checks if an entity exists within the table name, using the key given.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="key">The key to search for.</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public async Task<bool> Exists(string tableName, string key)
        {
            var cosmosContainer = CosmosClient.GetContainer(DatabaseName, tableName);
            var partitionKey = ExtractPartitionKey(key, out string modifiedKey);
            key = modifiedKey;

            try
            {
                var item = await cosmosContainer.ReadItemAsync<object>(key, partitionKey); 

                if (!item.IsNullOrDefault())
                {
                    return true;
                }
            }
            catch (Exception ex)
                when (ex.Message.ToLowerInvariant().Contains("resource not found"))
            {
                return false;
            }

            return false;
        }

        /// <summary>
        /// Deletes the entity with the given key from the requested table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="key">The key to search for.</param>
        /// <returns>Task.</returns>
        public async Task DeleteEntity(string tableName, string key)
        {
            var cosmosContainer = CosmosClient.GetContainer(DatabaseName, tableName);
            var partitionKey = ExtractPartitionKey(key, out string modifiedKey);

            await cosmosContainer.DeleteItemAsync<object>(modifiedKey, partitionKey);
        }

        /// <summary>
        /// Deletes multiple entities with the list of keys (done in batches), from the supplied table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="keys">The keys for deletion.</param>
        /// <param name="batchSize">Size of the batch to delete at any one time (defaults to 10).</param>
        /// <returns>Task.</returns>
        public async Task DeleteEntities(string tableName, List<string> keys, int batchSize = 10)
        {
            var cosmosContainer = CosmosClient.GetContainer(DatabaseName, tableName);
            var exceptions = new ConcurrentQueue<Exception>();

            // Delete each item
            var task = Task.Run(() => { 

                Parallel.ForEach(keys, (k) => {
                    try
                    {
                         var partitionKey = ExtractPartitionKey(k, out string modifiedKey);
                         cosmosContainer.DeleteItemAsync<object>(modifiedKey, partitionKey).GetAwaiter().GetResult();
                    }            
                    catch (Exception e)
                    {
                        // Store the exception and continue with the loop.        
                        exceptions.Enqueue(e);
                    }
                });

            });

            await task;

            // Throw the exceptions here after the loop completes.
            if (exceptions.Count > 0) throw new AggregateException(exceptions);
        }

        /// <summary>
        /// Inserts or updates (upserts) the passed entity into the given table.
        /// Key should be "PARTITIONVALUE/ID" or just "ID"
        /// If the partition key was declared as Property X, then make sure the Key has a matching
        /// partition, otherwise an error will occur.  For example, if partition key was declared as the "Name" property
        /// of an object, then when the type T comes through, T.Name must be "PartitionKey".
        /// </summary>
        /// <typeparam name="T">Type of object upserted.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="data">The data to insert.</param>
        /// <returns>Task.</returns>
        public async Task UpsertEntity<T>(string tableName, T data) where T : class, ITableItem
        {            
            var cosmosContainer = CosmosClient.GetContainer(DatabaseName, tableName);

            // Extract partition key will split the passed in partition key ("partition/id") and return the modified 
            // version of the key ("id" only) as well as the partition key ("partition").
            var partitionKey = ExtractPartitionKey(data.Key, out string modifiedKey);
            data.Key = modifiedKey;

            await cosmosContainer.CreateItemAsync(data, partitionKey);
        }

        /// <summary>
        /// Upserts multiple entities into the given table.
        /// Key should be "PARTITIONVALUE/ID" or just "ID"
        /// If the partition key was declared as Property X, then make sure the Key has a matching
        /// partition, otherwise an error will occur.  For example, if partition key was declared as the "Name" property
        /// of an object, then when the type T comes through, T.Name must be "PartitionKey".
        /// </summary>
        /// <typeparam name="T">Type of object to upsert.</typeparam>
        /// <param name="tableName">Name of the table the items will be added to.</param>
        /// <param name="data">The data.</param>
        /// <param name="batchSize">Size of the batch to update at any one time (defaults to 10).</param>
        /// <returns>Task.</returns>
        public async Task UpsertEntities<T>(string tableName, List<T> data, int batchSize = 10) where T : class, ITableItem
        {
            var cosmosContainer = CosmosClient.GetContainer(DatabaseName, tableName);
            var exceptions = new ConcurrentQueue<Exception>();

            // Delete each item
            var task = Task.Run(() => {

                Parallel.ForEach(data, d => {
                    try
                    {
                        var partitionKey = ExtractPartitionKey(d.Key, out string modifiedKey);
                        d.Key = modifiedKey;
                        
                        cosmosContainer.CreateItemAsync(d, partitionKey).GetAwaiter().GetResult();
                    }
                    catch (Exception e)
                    {
                        // Store the exception and continue with the loop.        
                        exceptions.Enqueue(e);
                    }
                });

            });

            await task;

            // Throw the exceptions here after the loop completes.
            if (exceptions.Count > 0) throw new AggregateException(exceptions);
        }

        /// <summary>
        /// List the entities of a given table name, with a supplied query.  Results returned as an Enumerable.
        /// </summary>
        /// <typeparam name="T">Type of object returned in the Enumerable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="selectColumns">The columns to select (if required).</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns enumerable list.</returns>
        public IEnumerable<T> ListEntities<T>(string tableName, List<string> selectColumns, CancellationTokenSource token = default)
            where T : class, ITableItem, new()
        {
            return ListEntitiesObservable<T>(tableName, selectColumns, token).ToEnumerable();
        }

        /// <summary>
        /// List the entities of a given table name, with a supplied query.  Results returned as an Enumerable.
        /// </summary>
        /// <typeparam name="T">Type of object returned in the Enumerable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="filterQuery">The query to execute.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns enumerable list.</returns>
        public IEnumerable<T> ListEntities<T>(string tableName, string filterQuery, CancellationTokenSource token = default)
            where T : class, ITableItem, new()
        {
            return ListEntitiesObservable<T>(tableName, filterQuery, token).ToEnumerable();
        }

        /// <summary>
        /// List the entities of a given table name (only the columns specified), with the supplied query.  Results returned as an Enumerable.
        /// </summary>
        /// <typeparam name="T">Type of object returned in the Enumerable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="selectColumns"></param>
        /// <param name="filterQuery">The query to execute.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns enumerable list of items.</returns>
        public IEnumerable<T> ListEntities<T>(string tableName, List<string> selectColumns, string filterQuery, CancellationTokenSource token) where T : class, ITableItem, new()
        {
            return ListEntitiesObservable<T>(tableName, selectColumns, filterQuery, token).ToEnumerable();
        }

        /// <summary>
        /// List the entities of a given table name.  Results returned as an Enumerable.
        /// </summary>
        /// <typeparam name="T">Type of object returned in the Enumerable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns enumerable list of items.</returns>
        public IEnumerable<T> ListEntities<T>(string tableName, CancellationTokenSource token) where T : class, ITableItem, new()
        {
            return ListEntitiesObservable<T>(tableName, "SELECT * FROM c", new CancellationTokenSource()).ToEnumerable();
        }

        /// <summary>
        /// List the entities of a given table name.  Results returned as an Observable.
        /// </summary>
        /// <typeparam name="T">Type of object returned in the observable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns observable.</returns>
        public IObservable<T> ListEntitiesObservable<T>(string tableName, CancellationTokenSource token) where T : class, ITableItem, new()
        {
            return ListEntitiesObservable<T>(tableName, "SELECT * FROM c", new CancellationTokenSource());
        }

        /// <summary>
        /// List the entities of a given table name.  Results returned as an Observable.
        /// </summary> 
        /// <typeparam name="T">Type of object returned in the observable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="filterQuery">Narrow down search with a query filter.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns observable.</returns>
        public IObservable<T> ListEntitiesObservable<T>(string tableName, string filterQuery, CancellationTokenSource token = default)
            where T : class, ITableItem, new()
        {
            try
            {
                return Observable.Create<T>(async obs =>
                {
                    var cancelToken = token ?? new CancellationTokenSource();

                    var cosmosContainer = CosmosClient.GetContainer(DatabaseName, tableName);

                    var cosmosQuery = new QueryDefinition(filterQuery);

                    FeedIterator<T> queryIterator;

                    do
                    {
                        // Stop when cancellation requested.
                        if (token != null && token.IsCancellationRequested)
                        {
                            queryIterator = null;
                        }
                        else
                        {
                            queryIterator = cosmosContainer.GetItemQueryIterator<T>(cosmosQuery);

                            // Raise the observable OnNext for each result to be processed.
                            foreach (var item in await queryIterator.ReadNextAsync(cancelToken.Token))
                            {
                                obs.OnNext(item);
                            }
                        }

                    } while (queryIterator != null && queryIterator.HasMoreResults);
                    obs.OnCompleted();
                });
            }
            catch (Exception e)
            {
                Logger?.LogError(e, $"Error {e.Message} occurred listing entities, table name: {tableName}");
                throw;
            }
        }

        /// <summary>
        /// List the entities of a given table name.  Results returned as an Observable.
        /// </summary> 
        /// <typeparam name="T">Type of object returned in the observable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="selectColumns">Columns to return for the object.</param>
        /// <param name="filterQuery">Narrow down search with a query filter.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns observable.</returns>
        public IObservable<T> ListEntitiesObservable<T>(string tableName, List<string> selectColumns, string filterQuery, CancellationTokenSource token)
            where T : class, ITableItem, new()
        {
            string cols = "*";

            if (selectColumns.Count != 0)
            {
                var formattedColumns = new List<string>();

                foreach (var column in selectColumns)
                {
                    formattedColumns.Add($"c['{column}']");
                }

                cols = string.Join(", ", formattedColumns);                
            }

            // Clean the where clause.
            if (filterQuery.IsNullOrEmpty())
            {
                filterQuery = string.Empty;
            }
            else
            {
                filterQuery = filterQuery.ToLowerInvariant().Contains("where ") ? filterQuery : $"WHERE ";
            }
                
            return ListEntitiesObservable<T>(tableName, $"SELECT {cols} FROM c {filterQuery}", new CancellationTokenSource());
        }

        /// <summary>
        /// List the entities of a given table name.  Results returned as an Observable.
        /// </summary> 
        /// <typeparam name="T">Type of object returned in the observable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="selectColumns">Columns to return for the object.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns observable.</returns>
        public IObservable<T> ListEntitiesObservable<T>(string tableName, List<string> selectColumns, CancellationTokenSource token = default)
            where T : class, ITableItem, new()
        {
            return ListEntitiesObservable<T>(tableName, selectColumns, null, token);
        }

        /// <summary>
        /// List the name of all containers within a given database.
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<string>> ListTableNames()
        {
            var database = CosmosClient.GetDatabase(DatabaseName);
            var iterator = database.GetContainerQueryIterator<ContainerProperties>();
            var containers = await iterator.ReadNextAsync().ConfigureAwait(false);
            var names = new List<string>();

            foreach (var c in containers)
            {
                names.Add(c.Id);
            }

            return names;
        }

        /// <summary>
        /// Count items in a particular table.
        /// </summary> 
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="query">Narrow down search with a query filter.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Count of items.</returns>
        public async Task<long> CountItemsQuery(string tableName, string query, CancellationTokenSource token = null)
        {
            return await Counter(tableName, query, null, token);
        }

        /// <summary>
        /// Count items in a particular table using a search key.
        /// </summary> 
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="key">Which match the passed in key</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Count of items.</returns>
        public async Task<long> CountItems(string tableName, string key, CancellationTokenSource token = null)
        {           
            return await Counter(tableName, $"SELECT c.id FROM c WHERE CONTAINS(ToString(c), '{key}')", null, token);
        }

        /// <summary>
        /// Count items in a particular table.
        /// </summary>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Count of items</returns>
        public async Task<long> CountItems(string tableName, CancellationTokenSource token = null)
        {
            return await Counter(tableName, "SELECT c.id FROM c", null, token);
        }

        /// <summary>
        /// Count items in a particular table.
        /// </summary>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="countIncrement">Action method executed when the count happens.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Count of items</returns>
        public async Task<long> CountItems(string tableName, Action<long> countIncrement, CancellationTokenSource token = default)
        {
            return await Counter(tableName, "SELECT c.id FROM c", countIncrement, token);
        }

        /// <summary>
        /// Delete the table passed in.
        /// </summary>
        /// <param name="tableName">Name of the table to delete.</param>
        /// <returns>Async Task.</returns>
        public async Task DeleteTable(string tableName)
        {
            var cosmosContainer = CosmosClient.GetContainer(DatabaseName, tableName);
            await cosmosContainer.DeleteContainerAsync();
        }

        /// <summary>
        /// Create the table name passed in.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public async Task CreateTable(string tableName)
        {
            //Default if none is given
            string partitionKeyPath = DefaultPartitionKeyPath;

            //Different to the extract partition key method
            if (tableName.Contains("/"))
            {
                var keyParts = tableName.Split("/");

                if (keyParts.Length > 2)
                {
                    throw new InvalidOperationException("Key and partition key cannot be parsed because there is more than one /");
                }

                partitionKeyPath = "/" + keyParts[1];
                tableName = keyParts[0];
            }

            var cosmosDatabase = CosmosClient.GetDatabase(DatabaseName);
            await cosmosDatabase.CreateContainerIfNotExistsAsync(tableName, partitionKeyPath);
        }

        /// <summary>
        /// Check to see if the table exists in the database.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns><c>true</c> if exists, <c>false</c> otherwise.</returns>
        public async Task<bool> TableExists(string tableName)
        {
            return (await ListTableNames()).Any(s => s == tableName);
        }

        /// <summary>
        /// Common count mechanism used on the public facing count methods.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="tblQuery">Query for looking up table.</param>
        /// <param name="countIncrement">Action, called every time an increment happens.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Task&lt;System.Int64&gt; count of items.</returns>
        private async Task<long> Counter(string tableName, string tblQuery, Action<long> countIncrement, CancellationTokenSource token)
        {
            try
            {
                var cancelToken = token ?? new CancellationTokenSource();
                var cosmosContainer = CosmosClient.GetContainer(DatabaseName, tableName);
                var cosmosQuery = new QueryDefinition(tblQuery);

                FeedIterator<CountItem> queryIterator;

                long count = 0;

                do
                {
                    // Stop when cancellation requested.
                    if (token != null && token.IsCancellationRequested)
                    {
                        queryIterator = null;
                    }
                    else
                    {
                        queryIterator = cosmosContainer.GetItemQueryIterator<CountItem>(cosmosQuery);

                        var results = await queryIterator.ReadNextAsync(cancelToken.Token);

                        count += results.LongCount();
                        countIncrement?.Invoke(count);
                    }

                } while (queryIterator != null && queryIterator.HasMoreResults);

                return count;
            }
            catch (Exception e)
            {
                Logger?.LogError(e, $"Error {e.Message} occurred listing entities, table name: {tableName}");
                throw;
            }
        }

        /// <summary>
        /// Transforms a single key object into key value and partition key based on a /
        /// </summary>
        /// <param name="key">The object containing key value and if there is one a partition key</param>
        /// <param name="modifiedKey">The key value with partition key removed</param>
        /// <returns></returns>
        private PartitionKey ExtractPartitionKey(string key, out string modifiedKey)
        {
            var partitionKey = PartitionKey.None;

            if (key.Contains("/"))
            {
                var keyParts = key.Split("/");

                if (keyParts.Length > 2)
                {
                    throw new InvalidOperationException("Key and partition key cannot be parsed because there is more than one /");
                }

                partitionKey = new PartitionKey(keyParts[0]);
                key = keyParts[1];
            }

            modifiedKey = key;

            return partitionKey;
        }
    }

    /// <summary>
    /// Base class for Azure specific implementation of cloud table storage.
    /// </summary>
    public abstract class CosmosStorageBase
    {
        internal static readonly ConcurrentDictionary<string, string> ConnectionStrings = new ConcurrentDictionary<string, string>();
        internal readonly ILogger Logger;
        internal readonly ServicePrincipleConfig ServicePrincipleConfig;
        internal readonly MsiConfig MsiConfig;
        internal string ConnectionString;

        private CosmosClient _cloudClient;
        private DateTimeOffset? _expiryTime;
        private readonly string _instanceName;
        private readonly string _subscriptionId;
        private readonly bool _createIfNotExists;

        internal CosmosClient CosmosClient
        {
            get
            {
                if (_cloudClient == null || _expiryTime <= DateTime.UtcNow)
                {
                    InitializeClient();
                }

                return _cloudClient;
            }
        }

        /// <summary>
        /// Gets or sets the name of the instance.
        /// </summary>
        /// <value>The name.</value>
        public string Name { get; set; }

        /// <summary>
        /// The name of the database
        /// </summary>
        public string DatabaseName;

        private void InitializeClient()
        {
            if (ConnectionString.IsNullOrEmpty())
            {
                ConnectionString = BuildStorageConnection().GetAwaiter().GetResult();
            }

            var clientBuilder = new CosmosClientBuilder(ConnectionString);
            clientBuilder.WithThrottlingRetryOptions(TimeSpan.FromMilliseconds(500), 3);
            var client = clientBuilder
                                .WithConnectionModeDirect()
                                .Build();

            if (client == null)
            {
                throw new InvalidOperationException("Cannot build Cosmos Client using connection string");
            }

            _cloudClient = client;

            // Create the database if it does not exist and been instructed to.
            if (_createIfNotExists)
            {
                _cloudClient.CreateDatabaseIfNotExistsAsync(DatabaseName).GetAwaiter().GetResult();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosStorageBase"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="logger">The logger.</param>
        protected CosmosStorageBase(ConnectionConfig config, ILogger logger = null)
        {
            // Validate the config.
            config.ThrowIfInvalid();

            Logger = logger;
            ConnectionString = config.ConnectionString;
            Name = config.InstanceName;
            DatabaseName = config.DatabaseName;

            _createIfNotExists = config.CreateDatabaseIfNotExists;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosStorageBase"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="logger">The logger.</param>
        protected CosmosStorageBase(MsiConfig config, ILogger logger = null)
        {
            // Validate the config.
            config.ThrowIfInvalid();

            Logger = logger;
            MsiConfig = config;
            Name = config.InstanceName;
            DatabaseName = config.DatabaseName;

            _instanceName = config.InstanceName;
            _subscriptionId = config.SubscriptionId;
            _createIfNotExists = config.CreateDatabaseIfNotExists;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosStorageBase"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="logger">The logger.</param>
        protected CosmosStorageBase(ServicePrincipleConfig config, ILogger logger = null)
        {
            // Validate the config.
            config.ThrowIfInvalid();

            Logger = logger;
            ServicePrincipleConfig = config;
            Name = config.InstanceName;
            DatabaseName = config.DatabaseName;

            _instanceName = config.InstanceName;
            _subscriptionId = config.SubscriptionId;
            _createIfNotExists = config.CreateDatabaseIfNotExists;
        }

        /// <summary>
        /// Builds a connection string for the storage account when none was specified during initialisation.
        /// </summary>
        /// <returns>Connection <see cref="string"/></returns>
        /// <exception cref="InvalidOperationException">
        /// If the Storage Namespace can not be resolved or access keys are not configured.
        /// </exception>
        [ExcludeFromCodeCoverage]
        internal async Task<string> BuildStorageConnection()
        {
            try
            {
                // If we already have the connection string for this instance - don't go get it again.
                if (ConnectionStrings.TryGetValue(_instanceName, out var connStr))
                    return connStr;

                const string azureManagementAuthority = "https://management.azure.com/";
                const string windowsLoginAuthority = "https://login.windows.net/";
                string token;

                // Use Msi Config if it's been specified, otherwise, use Service principle.
                if (MsiConfig != null)
                {
                    // Managed Service Identity (MSI) authentication.
                    var provider = new AzureServiceTokenProvider();
                    token = provider.GetAccessTokenAsync(azureManagementAuthority, MsiConfig.TenantId).GetAwaiter().GetResult();

                    if (string.IsNullOrEmpty(token)) {
                        throw new InvalidOperationException("Could not authenticate using Managed Service Identity, ensure the application is running in a secure context");
                    }

                    _expiryTime = DateTime.Now.AddDays(1);
                }
                else
                {
                    // Service Principle authentication
                    // Grab an authentication token from Azure.
                    var context = new AuthenticationContext($"{windowsLoginAuthority}{ServicePrincipleConfig.TenantId}");

                    var credential = new ClientCredential(ServicePrincipleConfig.AppId, ServicePrincipleConfig.AppSecret);
                    var tokenResult = context.AcquireTokenAsync(azureManagementAuthority, credential).GetAwaiter().GetResult();

                    if (tokenResult == null || tokenResult.AccessToken == null)
                    {
                        throw new InvalidOperationException($"Could not authenticate to {windowsLoginAuthority}{ServicePrincipleConfig.TenantId} using supplied AppId: {ServicePrincipleConfig.AppId}");
                    }

                    _expiryTime = tokenResult.ExpiresOn;
                    token = tokenResult.AccessToken;
                }

                // Set credentials and grab the authenticated REST client.
                var tokenCredentials = new TokenCredentials(token);

                var client = RestClient.Configure()
                    .WithEnvironment(AzureEnvironment.AzureGlobalCloud)
                    .WithLogLevel(HttpLoggingDelegatingHandler.Level.BodyAndHeaders)
                    .WithCredentials(new AzureCredentials(tokenCredentials, tokenCredentials, string.Empty, AzureEnvironment.AzureGlobalCloud))
                    .WithRetryPolicy(new RetryPolicy(new HttpStatusCodeErrorDetectionStrategy(), new FixedIntervalRetryStrategy(3, TimeSpan.FromMilliseconds(500))))
                    .Build();

                // Authenticate against the management layer.
                var azureManagement = Azure.Authenticate(client, string.Empty).WithSubscription(_subscriptionId);

                // Get the storage namespace for the passed in instance name.
                var storageNamespace = azureManagement.CosmosDBAccounts.List().FirstOrDefault(n => n.Name == _instanceName);

                // If we cant find that name, throw an exception.
                if (storageNamespace == null)
                {
                    throw new InvalidOperationException($"Could not find the storage instance {_instanceName} in the subscription Id specified");
                }

                // Storage accounts use access keys - this will be used to build a connection string.
                var accessKeys = await storageNamespace.ListKeysAsync();

                // If the access keys are not found (not configured for some reason), throw an exception.
                if (accessKeys == null)
                {
                    throw new InvalidOperationException($"Could not find access keys for the storage instance {_instanceName}");
                }

                // We just default to the primary key.
                var key = accessKeys.PrimaryMasterKey;

                // Build the connection string.
                var connectionString = $"AccountEndpoint={storageNamespace.DocumentEndpoint};AccountKey={key}";

                // Cache the connection string off so we don't have to re-authenticate.
                if (!ConnectionStrings.ContainsKey(_instanceName))
                {
                    ConnectionStrings.TryAdd(_instanceName, connectionString);
                }

                // Return the connection string.
                return connectionString;
            }
            catch (Exception e)
            {
                _expiryTime = null;
                Logger?.LogError(e, "An exception occured during connection to Table storage");
                throw new InvalidOperationException("An exception occurred during service connection, see inner exception for more detail", e);
            }
        }
    }
}
