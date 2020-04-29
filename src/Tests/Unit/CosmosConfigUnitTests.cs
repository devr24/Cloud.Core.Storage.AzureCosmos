using System.Linq;
using Cloud.Core.Storage.AzureCosmos.Config;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Cloud.Core.Storage.AzureCosmos.Tests.Unit
{
    [IsUnit]
    public class CosmosConfigUnitTests
    {
        /// <summary>Verify validation works as expected for msi config.</summary>
        [Fact]
        public void Test_Configuration_MsiValidation()
        {
            // Arrange
            var msiConfig = new MsiConfig();

            // Act/Assert - Check the msi config validation.
            var validationRes = msiConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(4);

            msiConfig.InstanceName = "test";
            validationRes = msiConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(3);

            msiConfig.TenantId = "test";
            validationRes = msiConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(2);

            msiConfig.DatabaseName = "test";
            validationRes = msiConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(1);

            msiConfig.SubscriptionId = "test";
            validationRes = msiConfig.Validate();
            validationRes.IsValid.Should().BeTrue();
            validationRes.Errors.ToList().Count.Should().Be(0);
            msiConfig.ToString().Length.Should().BeGreaterThan(0);
        }

        /// <summary>Verify validation works as expected for connection config.</summary>
        [Fact]
        public void Test_Configuration_ConnectionConfigValidation()
        {
            // Arrange
            var connectionConfig = new ConnectionConfig();

            // Act/Assert
            var validationRes = connectionConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(2);

            connectionConfig.ConnectionString = "test";
            validationRes = connectionConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(1);

            connectionConfig.DatabaseName = "test";
            validationRes = connectionConfig.Validate();
            validationRes.IsValid.Should().BeTrue();
            validationRes.Errors.ToList().Count.Should().Be(0);
            connectionConfig.ToString().Length.Should().BeGreaterThan(0);
        }

        /// <summary>Verify validation works as expected for service principle.</summary>
        [Fact]
        public void Test_Configuration_ServicePrincipleValidation()
        {
            // Arrange
            var spConfig = new ServicePrincipleConfig();

            // Act/Assert
            var validationRes = spConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(6);

            spConfig.InstanceName = "test";
            validationRes = spConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(5);

            spConfig.AppId = "test";
            validationRes = spConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(4);

            spConfig.AppSecret = "test";
            validationRes = spConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(3); 
            
            spConfig.TenantId = "test";
            validationRes = spConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(2); 
            
            spConfig.DatabaseName = "test";
            validationRes = spConfig.Validate();
            validationRes.IsValid.Should().BeFalse();
            validationRes.Errors.ToList().Count.Should().Be(1); 

            spConfig.SubscriptionId = "test";
            validationRes = spConfig.Validate();
            validationRes.IsValid.Should().BeTrue();
            validationRes.Errors.ToList().Count.Should().Be(0);
            spConfig.ToString().Length.Should().BeGreaterThan(0);
        }

        /// <summary>Add multiple instances and ensure table storage named instance factory resolves as expected.</summary>
        [Fact]
        public void Test_ServiceCollection_NamedInstances()
        {
            // Arrange
            IServiceCollection serviceCollection = new ServiceCollection();

            // Act/Assert
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeFalse();
            serviceCollection.ContainsService(typeof(IStateStorage)).Should().BeFalse();
            serviceCollection.ContainsService(typeof(IAuditLogger)).Should().BeFalse();

            serviceCollection.AddCosmosStorageSingletonNamed("TS1", "tableStorageInstance1", "test", "test", "test");
            serviceCollection.AddCosmosStorageSingletonNamed("TS2", "tableStorageInstance2", "test", "test", "test");
            serviceCollection.AddCosmosStorageSingleton("tableStorageInstance3", "test", "test", "test");
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();

            var provider = serviceCollection.BuildServiceProvider();
            var namedInstanceProv = provider.GetService<NamedInstanceFactory<ITableStorage>>();
            namedInstanceProv.Should().NotBeNull();

            namedInstanceProv["TS1"].Should().NotBeNull();
            namedInstanceProv["TS2"].Should().NotBeNull();
            namedInstanceProv["tableStorageInstance3"].Should().NotBeNull();
        }

        /// <summary>Verify instance name is built as expected for connection config.</summary>
        [Fact]
        public void Test_ConnectionConfig_InstanceName()
        {
            // Arrange
            var config1 = new ConnectionConfig();
            var config2 = new ConnectionConfig();
            var config3 = new ConnectionConfig();
            var config4 = new ConnectionConfig();

            // Act
            config2.ConnectionString = "AB";
            config3.ConnectionString = "A;B";
            config4.ConnectionString = "A;AccountEndpoint=https://B;C";

            // Assert
            config1.InstanceName.Should().BeNull();
            config2.InstanceName.Should().Be(null);
            config3.InstanceName.Should().Be(null);
            config4.InstanceName.Should().Be("B");
        }
        
        /// <summary>Check the ITableStorage is added to the service collection when using the new extension methods.</summary>
        [Fact]
        public void Test_ServiceCollection_AddCosmosStorageSingleton()
        {
            // Arrange
            IServiceCollection serviceCollection = new ServiceCollection();

            serviceCollection.AddCosmosStorageSingleton("test", "test", "test", "test");
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<ITableStorage>)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(object)).Should().BeFalse();
            serviceCollection.Clear();

            serviceCollection.AddCosmosStorageSingletonNamed("key1", "test", "test", "test", "test");
            serviceCollection.AddCosmosStorageSingletonNamed("key2", "test", "test", "test", "test");
            serviceCollection.AddCosmosStorageSingleton("test1", "test", "test", "test");
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<ITableStorage>)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();

            // Act/Assert
            var prov = serviceCollection.BuildServiceProvider();
            var resolvedFactory = prov.GetService<NamedInstanceFactory<ITableStorage>>();

            resolvedFactory["key1"].Should().NotBeNull();
            resolvedFactory["key2"].Should().NotBeNull();
            resolvedFactory["test1"].Should().NotBeNull();
            serviceCollection.Clear();

            serviceCollection.AddCosmosStorageSingleton(new ServicePrincipleConfig { InstanceName = "test", AppId = "test", AppSecret = "test", TenantId = "test", SubscriptionId = "test", DatabaseName = "test"});
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddCosmosStorageSingleton(new ConnectionConfig { ConnectionString = "test", DatabaseName = "test"});
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddCosmosStorageSingleton("test", "test", "test", "test");
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();
        }

        /// <summary>Verify properties can be got and set as expected.</summary>
        [Fact]
        public void Test_CountItem_Properties()
        {
            // Arrange
            var count = new CountItem();

            // Act
            count.Id = "a";
            count.Key = "b";

            // Assert
            count.Id.Should().Be("a");
            count.Key.Should().Be("b");
        }
    }
}
