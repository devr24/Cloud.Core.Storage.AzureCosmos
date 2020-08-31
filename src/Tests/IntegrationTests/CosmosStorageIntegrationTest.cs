using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Cloud.Core.Storage.AzureCosmos.Config;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Xunit;

namespace Cloud.Core.Storage.AzureCosmos.Tests.IntegrationTests
{
    [IsIntegration]
    public class CosmosStorageIntegrationTest
    {
        private readonly ITableStorage _cosmosClient;

        public CosmosStorageIntegrationTest()
        {
            var readConfig = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();

            var config = new Config.ServicePrincipleConfig
            {
                InstanceName = readConfig.GetValue<string>("InstanceName"),
                TenantId = readConfig.GetValue<string>("TenantId"),
                SubscriptionId = readConfig.GetValue<string>("SubscriptionId"),
                DatabaseName = "Test",
                AppId = readConfig.GetValue<string>("AppId"),
                AppSecret = readConfig.GetValue<string>("AppSecret"),
                CreateDatabaseIfNotExists = true
            };

            _cosmosClient = new CosmosStorage(config);
        }

        /// <summary>Verify entities can be created and deleted as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_DeleteEntitites()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var key = Guid.NewGuid().ToString();
                var key2 = Guid.NewGuid().ToString();
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = key, Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = key2, Name = nameKey, OtherField = "other1" };

                // Act/Assert - setup and delete entities confirming created and deleted.
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
                var entityOneExists = await _cosmosClient.Exists(containerName, key);
                var entityTwoExists = await _cosmosClient.Exists(containerName, key2);

                entityOneExists.Should().Be(true);
                entityTwoExists.Should().Be(true);

                var listOfKeys = new List<string>() { key, key2 };

                await _cosmosClient.DeleteEntities(containerName, listOfKeys);

                entityOneExists = await _cosmosClient.Exists(containerName, key);
                entityTwoExists = await _cosmosClient.Exists(containerName, key2);

                entityOneExists.Should().Be(false);
                entityTwoExists.Should().Be(false);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure upserting entities works as expecting.</summary>
        [Fact]
        public async Task Test_CosmosStorage_UpsertEntities()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var key = Guid.NewGuid().ToString();
                var key2 = Guid.NewGuid().ToString();
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = key, Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = key2, Name = nameKey, OtherField = "other1" };
                var entities = new List<SampleEntity> { entity, entity2 };

                // Act
                await _cosmosClient.UpsertEntities(containerName, entities);
                var entityOneExists = await _cosmosClient.Exists(containerName, key);
                var entityTwoExists = await _cosmosClient.Exists(containerName, key2);

                // Assert
                entityOneExists.Should().Be(true);
                entityTwoExists.Should().Be(true);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure a list of table names are gathered as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListTableNames()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange/Act
                var containerNames = await _cosmosClient.ListTableNames();

                // Assert
                containerNames.Should().Contain(containerName);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure count items returns the expected result.</summary>
        [Fact]
        public async Task Test_CosmosStorage_CountItems()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var key = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = key, Name = "Name", OtherField = "other1", OtherField2 = 1 };
                var actionHit = false;

                // Act
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.CountItems(containerName, (count) => { actionHit = true; });
                await Task.Delay(1000);

                // Assert
                actionHit.Should().BeTrue();
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure upserting a single entity works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_UpsertSingle()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var key = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = key, Name = "Name", OtherField = "other1", OtherField2 = 1 };

                // Act - ensure there's an object to check for.         
                await _cosmosClient.UpsertEntity(containerName, entity);              
                var exists = await _cosmosClient.Exists(containerName, key);

                // Assert
                exists.Should().Be(true);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure listing entities as enumerable with filter works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesEnumerableNoToken()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };

                // Act - ensure there's an object to check for.
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
                
                var entities = _cosmosClient.ListEntities<SampleEntity>(containerName, $"SELECT * FROM c WHERE c.Name = '{nameKey}'");

                // Assert
                entities.Count().Should().Be(2);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure listing entities first items, with filter, works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesRetrieveFirstValid()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };

                // Act - ensure there's an object to check for.
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
                
                var entities = _cosmosClient.ListEntities<SampleEntity>(containerName, $"SELECT * FROM c WHERE c.Name = '{nameKey}'");
                var firstEntity = entities.First();

                // Assert
                firstEntity.Should().NotBeNull();
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure listing entities as enumerable with filter works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesRetrieveFirstFailWhenIsNoEntity()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();

                // Act - ensure there's an object to check for.           
                var entities = _cosmosClient.ListEntities<SampleEntity>(containerName, $"SELECT * FROM c WHERE c.Name = '{nameKey}'");

                // Assert
                Assert.Throws<InvalidOperationException>(() => entities.First());
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure listing entities as enumerable, with cancellation token, works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesEnumerableWithToken()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var token = new CancellationTokenSource();

                //Act - ensure there's an object to check for.    
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
                       
                var entities = _cosmosClient.ListEntities<SampleEntity>(containerName, $"SELECT * FROM c WHERE c.Name = '{nameKey}'", token);

                // Assert
                entities.Count().Should().Be(2);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure listing entities as enumerable, with cancellation token and no filter, works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesEnumerableWithTokenAndNoQuery()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var key = Guid.NewGuid().ToString();
                var key2 = Guid.NewGuid().ToString();
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = key, Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = key2, Name = nameKey, OtherField = "other1" };
                var token = new CancellationTokenSource();

                // Act - ensure there's an object to check for.   
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
        
                var entities = _cosmosClient.ListEntities<SampleEntity>(containerName, token);

                // Assert
                entities.Count().Should().BeGreaterThan(0);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure listing entities as enumerable, with columns, works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesEnumerableWithColumnsToken()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var columns = new List<string>() { "Name", "Key" };
                var token = new CancellationTokenSource();

                // Act - ensure there's an object to check for.  
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
         
                var entities = _cosmosClient.ListEntities<SampleEntity>(containerName, columns, $"WHERE c.Name = '{nameKey}'", token).ToList();

                // Assert - Confirm entity is retrieved with only the selected fields
                entities.Count().Should().Be(2);

                entities[0].Key.Should().NotBeNullOrEmpty();
                entities[0].Name.Should().NotBeNullOrEmpty();
                entities[0].OtherField.Should().BeNull();

                entities[1].Key.Should().NotBeNullOrEmpty();
                entities[1].Name.Should().NotBeNullOrEmpty();
                entities[1].OtherField.Should().BeNull();
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure listing entities as enumerable, with columns and filter, works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesEnumerableWithColumnsAndNoToken()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var columns = new List<string>() { "Name", "Key" };

                // Act - ensure there's an object to check for.     
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
      
                var entities = _cosmosClient.ListEntities<SampleEntity>(containerName, columns, $"WHERE c.Name = '{nameKey}'").ToList();

                // Assert - Confirm entity is retrieved with only the selected fields
                entities.Count().Should().Be(2);

                entities[0].Key.Should().NotBeNullOrEmpty();
                entities[0].Name.Should().NotBeNullOrEmpty();
                entities[0].OtherField.Should().BeNull();

                entities[1].Key.Should().NotBeNullOrEmpty();
                entities[1].Name.Should().NotBeNullOrEmpty();
                entities[1].OtherField.Should().BeNull();
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure listing entities as onservable, with filter, works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesObservableNoToken()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                int count = 0;
                int loops = 0;

                // Act - ensure there's an object to check for.       
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
    
                var entities = _cosmosClient.ListEntitiesObservable<SampleEntity>(containerName, $"SELECT * FROM c WHERE c.Name = '{nameKey}'").Subscribe(e =>
                {
                    count++;
                });

                // Wait for subscription.
                do
                {
                    await Task.Delay(500);
                    loops++;
                } while (loops < 5 || count == 0);

                // Assert
                count.Should().Be(2);
                entities.Should().NotBeNull();
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure listing entities as onservable, with filter and cancellation token, works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesObservableWithToken()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();

                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };

                int count = 0;
                int loops = 0;

                var token = new CancellationTokenSource();

                // Act - ensure there's an object to check for.   
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);    
                
                var entities = _cosmosClient.ListEntitiesObservable<SampleEntity>(containerName, $"SELECT * FROM c WHERE c.Name = '{nameKey}'", token).Subscribe(e =>
                {
                    count++;
                });

                // Wait for subscription.
                do
                {
                    await Task.Delay(500);
                    loops++;
                } while (loops < 5 || count == 0);

                // Assert
                count.Should().Be(2);
                entities.Should().NotBeNull();
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure listing entities as onservable, no filter, works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesObservableWithTokenAndNoQuery()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };

                int count = 0;
                int loops = 0;
                var token = new CancellationTokenSource();

                // Act - ensure there's an object to check for.     
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
      
                var entities = _cosmosClient.ListEntitiesObservable<SampleEntity>(containerName, token).Subscribe(e =>
                {
                    count++;
                });

                // Wait for subscription.
                do
                {
                    await Task.Delay(500);
                    loops++;
                } while (loops < 5 || count == 0);

                // Assert
                count.Should().BeGreaterThan(0);
                entities.Should().NotBeNull();
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure list entities observable by specifying columns returns as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesObservableWithColumnsTokenAndQuery()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };

                int count = 0;
                int loops = 0;
                var token = new CancellationTokenSource();
                var columns = new List<string>() { "Name", "Key" };
                var retrievedEntities = new List<SampleEntity>();

                // Act - ensure there's an object to check for.    
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
       
                var entities = _cosmosClient.ListEntitiesObservable<SampleEntity>(containerName, columns, $"WHERE c.Name = '{nameKey}'", token).Subscribe(e =>
                {
                    retrievedEntities.Add(e);
                    count++;
                });

                // Wait for subscription.
                do
                {
                    await Task.Delay(500);
                    loops++;
                } while (loops < 5 || count == 0);

                // Assert - Confirm entity is retrieved with only the selected fields.
                count.Should().Be(2);

                retrievedEntities[0].Key.Should().NotBeNullOrEmpty();
                retrievedEntities[0].Name.Should().NotBeNullOrEmpty();
                retrievedEntities[0].OtherField.Should().BeNull();

                retrievedEntities[1].Key.Should().NotBeNullOrEmpty();
                retrievedEntities[1].Name.Should().NotBeNullOrEmpty();
                retrievedEntities[1].OtherField.Should().BeNull();
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure list entities by specifying columns returns as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_ListEntitiesObservableWithColumnsNoTokenAndNoQuery()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();

                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };

                int count = 0;
                int loops = 0;

                var token = new CancellationTokenSource();
                var columns = new List<string>() { "Name", "Key" };
                var retrievedEntities = new List<SampleEntity>();

                // Act - ensure there's an object to check for.     
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
      
                var entities = _cosmosClient.ListEntitiesObservable<SampleEntity>(containerName, columns, token).Subscribe(e =>
                {
                    retrievedEntities.Add(e);
                    count++;
                });

                // Wait for subscription.
                do
                {
                    await Task.Delay(500);
                    loops++;
                } while (loops < 5 || count == 0);

                // Assert - Confirm entites are retrieved with only the selected fields
                count.Should().Be(2);

                retrievedEntities[0].Key.Should().NotBeNullOrEmpty();
                retrievedEntities[0].Name.Should().NotBeNullOrEmpty();
                retrievedEntities[0].OtherField.Should().BeNull();

                retrievedEntities[1].Key.Should().NotBeNullOrEmpty();
                retrievedEntities[1].Name.Should().NotBeNullOrEmpty();
                retrievedEntities[1].OtherField.Should().BeNull();
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure count entities with query filter and cancellation token returns as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_CountEntitiesWithQueryAndToken()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var token = new CancellationTokenSource();

                // Act
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);

                var entities = await _cosmosClient.CountItemsQuery(containerName, "SELECT * FROM c", token);

                // Assert
                entities.Should().BeGreaterThan(1);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure count entities with query filter returns as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_CountEntitiesWithQueryNoToken()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };

                // Act
                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);
                
                var entities = await _cosmosClient.CountItemsQuery(containerName, "SELECT * FROM c");

                // Assert
                entities.Should().BeGreaterThan(1);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure count entities (no filter) returns as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_CountEntitiesWithNoQueryAndToken()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();

                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };

                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);

                var token = new CancellationTokenSource();

                //Act - ensure there's an object to check for.           
                var entities = await _cosmosClient.CountItems(containerName, token);

                // Assert
                entities.Should().BeGreaterThan(1);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure count entities using a key returns as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_CountEntitiesWithKey()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange
                var nameKey = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };
                var entity2 = new SampleEntity() { Key = Guid.NewGuid().ToString(), Name = nameKey, OtherField = "other1" };

                await _cosmosClient.UpsertEntity(containerName, entity);
                await _cosmosClient.UpsertEntity(containerName, entity2);

                //Act - ensure there's an object to check for.           
                var entities = await _cosmosClient.CountItems(containerName, nameKey);

                // Assert
                entities.Should().Be(2);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure get entity returns the item correctly as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_GetEntity()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange - create test entity.
                var key = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = key, Name = "Name", OtherField = "other1" };
                await _cosmosClient.UpsertEntity(containerName, entity);

                // Act - ensure there's an object to check for.
                var result = await _cosmosClient.GetEntity<SampleEntity>(containerName, key);
                await _cosmosClient.DeleteEntity(containerName, key);

                // Assert
                result.Key.Should().Be(entity.Key);
                result.Name.Should().Be(entity.Name);
                result.OtherField.Should().Be(entity.OtherField);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure searching an non-existent item returns null.</summary>
        [Fact]
        public async Task Test_CosmosStorage_GetEntityThatDoesntExistReturnsNull()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange - create test entity.
                var key = Guid.NewGuid().ToString();

                // Act - ensure there's an object to check for.
                var result = await _cosmosClient.GetEntity<SampleEntity>(containerName, key);

                // Assert
                result.Should().Be(null);

            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure deleting a single item works as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_DeleteSingle()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create container
                await _cosmosClient.CreateTable(containerName);

                // Arrange - create test entity.
                var key = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = key, Name = "name1", OtherField = "other1" };
                await _cosmosClient.UpsertEntity(containerName, entity);

                // Act - ensure there's an object to check for.
                var exists = await _cosmosClient.Exists(containerName, key);
                await _cosmosClient.DeleteEntity(containerName, key);
                var existsAfterDeletion = await _cosmosClient.Exists(containerName, key);

                // Assert
                exists.Should().Be(true);
                existsAfterDeletion.Should().Be(false);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Verify check exists returns expected results.</summary>
        [Fact]
        public async Task Test_CosmosStorage_CheckExists()
        {
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Create test container.
                await _cosmosClient.CreateTable(containerName);

                // Arrange - create test entity.
                var key = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = key, Name = "name1", OtherField = "other1" };
                await _cosmosClient.UpsertEntity(containerName, entity);

                // Act - ensure there's an object to check for.
                var exists = await _cosmosClient.Exists(containerName, key);
                await _cosmosClient.DeleteEntity(containerName, key);
                var existsAfterDeletion = await _cosmosClient.Exists(containerName, key);

                // Assert
                exists.Should().Be(true);
                existsAfterDeletion.Should().Be(false);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure items are created and partition key is set as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_PartitionTests()
        {
            // Arrange
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Act - Create container with name as partition key
                await _cosmosClient.CreateTable(containerName + "/Name");
                
                // Add an object.
                var id1 = Guid.NewGuid().ToString();
                var key = "name1/" + id1;
                var entity = new SampleEntity() { Key = key, Name = "name1", OtherField = "other1" };
                await _cosmosClient.UpsertEntity(containerName, entity);

                // Add a second object.
                var id2 = Guid.NewGuid().ToString();
                var secondKey = "name2/" + id2;
                var secondEntity = new SampleEntity() { Key = secondKey, Name = "name2", OtherField = "other1" };
                await _cosmosClient.UpsertEntity(containerName, secondEntity);

                // Check first object exists.
                var exists = await _cosmosClient.Exists(containerName, key);

                // Retrieve the second object.
                var retrievedEntity = await _cosmosClient.GetEntity<SampleEntity>(containerName, secondKey);
                var expectedId = secondKey.Replace("name2/", "");
                await _cosmosClient.DeleteEntity(containerName, key);
                var existsAfterDeletion = await _cosmosClient.Exists(containerName, key);
                
                // Assert
                exists.Should().Be(true);
                retrievedEntity.Key.Should().Be(expectedId);
                retrievedEntity.Id.Should().Be(expectedId);
                retrievedEntity.Name.Should().Be("name2");
                retrievedEntity.OtherField.Should().Be("other1");
                retrievedEntity.OtherField2.Should().BeNull();
                existsAfterDeletion.Should().Be(false);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure table is created without a partition key set.</summary>
        [Fact]
        public async Task Test_CosmosStorage_CreateContainerAndDeleteContainerWithNoPartition()
        {
            // Arrange
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Act - create container.
                await _cosmosClient.CreateTable(containerName);
                var tableExists = await (_cosmosClient as CosmosStorage).TableExists(containerName);

                // Assert
                tableExists.Should().BeTrue();
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure table is created with a partition key set.</summary>
        [Fact]
        public async Task Test_CosmosStorage_CreateContainerAndDeleteContainerWithPartition()
        {
            // Arrange
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Act - Create container with partition.
                await _cosmosClient.CreateTable(containerName + "/ParitionKey");
                var tableExists = await (_cosmosClient as CosmosStorage).TableExists(containerName);

                // Assert
                tableExists.Should().BeTrue();
            }
            finally
            {
                // Remove test container. afterwards.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure the table is created without passing a partition key.  Partition will default to "_PartitionKey".</summary>
        [Fact]
        public async Task Test_CosmosStorage_AddToCreatedTableNoPartition()
        {
            // Arrange
            var containerName = Guid.NewGuid().ToString().Replace("-", string.Empty);

            try
            {
                // Act - create container.
                await _cosmosClient.CreateTable(containerName);

                // Add an object to verify table exists.
                var key = Guid.NewGuid().ToString();
                var entity = new SampleEntity() { Key = key, Name = "Name", OtherField = "other1" };
                await _cosmosClient.UpsertEntity(containerName, entity);

                // Check first object exists.
                var exists = await _cosmosClient.Exists(containerName, key);

                // Assert - item should exist in the table.
                exists.Should().Be(true);
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName);
            }
        }

        /// <summary>Ensure the creation of a table using a partition key and non partition key is setup as expected.</summary>
        [Fact]
        public async Task Test_CosmosStorage_WithAndWithoutPartitionKey()
        {
            var containerName1 = Guid.NewGuid().ToString();
            var containerName2 = Guid.NewGuid().ToString();

            try
            {
                // Arrange - Setup test enitity.
                var entityWithPartition = new SampleEntity()
                {
                    Name = "Name123",
                    OtherField = "Other1",
                    Key = "Name123/12345"
                };
                var entityNoPartition = new SampleEntity()
                {
                    Name = "Name123",
                    OtherField = "Other1",
                    Key = "12345"
                };

                // Act - setup containers.
                await _cosmosClient.CreateTable($"{containerName1}/Name");
                await _cosmosClient.CreateTable($"{containerName2}");

                // Upsert entity.
                await _cosmosClient.UpsertEntity(containerName1, entityWithPartition);
                await _cosmosClient.UpsertEntity(containerName2, entityNoPartition);

                // Retrieve entities back.
                var s1 = await _cosmosClient.GetEntity<SampleEntity>(containerName1, entityWithPartition.Key);
                var s2 = await _cosmosClient.GetEntity<SampleEntity>(containerName2, entityNoPartition.Key);

                // Assert - the values are correct.
                s1.Should().NotBeNull();
                s2.Should().NotBeNull();
            }
            finally
            {
                // Remove test container.
                await _cosmosClient.DeleteTable(containerName1);
                await _cosmosClient.DeleteTable(containerName2);
            }
        }

        private class SampleEntity : ITableItem
        {
            public string Key { get; set; }
            public string Name { get; set; }
            public string OtherField { get; set; }
            public int? OtherField2 { get; set; }
            public bool OtherField3 { get; set; }

            [JsonProperty("id")] public string Id => Key.Split('/').LastOrDefault();
        }
    }
}
