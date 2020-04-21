using System;
using System.Linq;
using Cloud.Core.Storage.AzureCosmos.Config;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Cloud.Core.Storage.AzureCosmos.Tests.Unit
{
    [IsUnit]
    public class CosmosConfigTests
    {
        [Fact]
        public void Test_Configuration_MsiValidation()
        {
            var msiConfig = new MsiConfig();

            // Check the msi config validation.
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
        }

        [Fact]
        public void Test_Configuration_ConnectionConfigValidation()
        {
            var connectionConfig = new ConnectionConfig();

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
        }

        [Fact]
        public void Test_Configuration_ServicePrincipleValidation()
        {
            var spConfig = new ServicePrincipleConfig();

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
        }

        /// <summary>Add multiple instances and ensure table storage named instance factory resolves as expected.</summary>
        [Fact]
        public void Test_ServiceCollection_NamedInstances()
        {
            IServiceCollection serviceCollection = new ServiceCollection();

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

        [Fact]
        public void Test_ConnectionConfig_InstanceName()
        {
            var config = new ConnectionConfig();
            config.InstanceName.Should().BeNull();

            config.ConnectionString = "AB";
            config.InstanceName.Should().Be(null);

            config.ConnectionString = "A;B";
            config.InstanceName.Should().Be(null);

            config.ConnectionString = "A;AccountEndpoint=https://B;C";
            config.InstanceName.Should().Be("B");
        }
        
        /// <summary>Check the ITableStorage is added to the service collection when using the new extension methods.</summary>
        [Fact]
        public void Test_ServiceCollection_AddCosmosStorageSingleton()
        {
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

            var prov = serviceCollection.BuildServiceProvider();

            var resolvedFactory = prov.GetService<NamedInstanceFactory<ITableStorage>>();

            resolvedFactory["key1"].Should().NotBeNull();
            resolvedFactory["key2"].Should().NotBeNull();
            resolvedFactory["test1"].Should().NotBeNull();
            serviceCollection.Clear();

            serviceCollection.AddCosmosStorageSingleton(new ServicePrincipleConfig { InstanceName = "test", AppId = "test", AppSecret = "test", TenantId = "test", SubscriptionId = "test" });
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddCosmosStorageSingleton(new ConnectionConfig { ConnectionString = "test" });
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddCosmosStorageSingleton("test", "test", "test", "test");
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();
        }

        // extension for named instance singleton - msi auth, service principle auth
        // extension to add cosmos storage singleton - msi, sp, conn string auth
    }
}
