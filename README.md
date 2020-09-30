# **Cloud.Core.Storage.AzureCosmos** 
[![Build status](https://dev.azure.com/cloudcoreproject/CloudCore/_apis/build/status/Cloud.Core%20Packages/Cloud.Core.Storage.AzureCosmos_Package)](https://dev.azure.com/cloudcoreproject/CloudCore/_build/latest?definitionId=19)
![Code Coverage](https://cloud1core.blob.core.windows.net/codecoveragebadges/Cloud.Core.Storage.AzureCosmos-LineCoverage.png) 
[![Cloud.Core.Storage.AzureCosmos package in Cloud.Core feed in Azure Artifacts](https://feeds.dev.azure.com/cloudcoreproject/dfc5e3d0-a562-46fe-8070-7901ac8e64a0/_apis/public/Packaging/Feeds/8949198b-5c74-42af-9d30-e8c462acada6/Packages/b7d921b0-864c-48d8-851b-c08cf98fd8e3/Badge)](https://dev.azure.com/cloudcoreproject/CloudCore/_packaging?_a=package&feed=8949198b-5c74-42af-9d30-e8c462acada6&package=b7d921b0-864c-48d8-851b-c08cf98fd8e3&preferRelease=true)


<div id="description">

Azure specific implementation of table storage interface for Cosmos.  Uses the ITableStorage interface from _Cloud.Core_.

</div>

## **Usage**

### Initialisation and Authentication

There are three ways you can instantiate the Blob Storage Client.  Each way dictates the security mechanism the client uses to connect.  The three mechanisms are:

1. Connection String
2. Service Principle
3. Managed Service Identity

Below are examples of instantiating each type.

#### 1. Connection String
Create an instance of the Cosmos Storage client with ConnectionConfig for connection string as follows:

```csharp
var tableConfig = new ConnectionConfig
    {
        ConnectionString = "<connectionstring>"
    };

// Table client.
var tablestorage = new CosmosStorage(blobConfig);	
```
Note: Instance name not required to be specified anywhere in configuration here as it is taken from the connection string itself.

#### 2. Service Principle
Create an instance of the Table Storage client with CosmosStorageConfig for Service Principle as follows:

```csharp
var tableConfig = new ServicePrincipleConfig
    {
        AppId = "<appid>",
        AppSecret = "<appsecret>",
        TenantId = "<tenantid>",
        StorageInstanceName = "<storageinstancename>",
        SubscriptionId = subscriptionId
    };

// Table client.
var tablestorage = new CosmosStorage(blobConfig);	
```

Usually the AppId, AppSecret (both of which are setup when creating a new service principle within Azure) and TenantId are specified in 
Configuration (environment variables/AppSetting.json file/key value pair files [for Kubernetes secret store] or command line arguments).

SubscriptionId can be accessed through the secret store (this should not be stored in config for security reasons).

#### 3. Management Service Idenity (MSI) 
This authentication also works for Managed User Identity.  Create an instance of the Table Storage client with MSI authentication as follows:

```csharp
var tableConfig = new MsiConfig
    {
        TenantId = "<tenantid>",
        StorageInstanceName = "<storageinstancename>",
        SubscriptionId = subscriptionId
    };

// Table client.
var tablestorage = new CosmosStorage(blobConfig);	
```

All that's required is the instance name to connect to.  Authentication runs under the context the application is running.

### Insert or Update (Upsert)
The following code shows how to insert a new record, or replace an existing record:

```csharp
// Insert a single item.
await tablestorage.UpsertEntity("tableName1", new SampleEntity() { Key ="partitionKey1/rowKey1", Name = "TEST1" });

// Insert mutliple items.
await tablestorage.UpsertEntites("tableName1", new List<Test>()
{
    new SampleEntity() { Key ="partitionKey1/rowKey2", Name = "TEST2"},
    new SampleEntity() { Key ="partitionKey1/rowKey3",  Name = "TEST3"},
    new SampleEntity() { Key ="partitionKey1/rowKey4",  Name = "TEST4"},
});
```

You can plass any generic type into the UpsertEntity call, as long as it implemented ITableItem interface, ensuring it has a Key.  Sample class (used in this example) is defined as:

```csharp
public class SampleClass: ITableItem 
{
    public string Key { get; set }
    public string Name { get; set; }
    // You can add as many properties as you need to the class, as long as it has ITableItem, that's all that is needed.
}
```

### Retrieve
The following code shows how to retrieve an entity:

```csharp
// Retrieve a single entity.
var entity = await tablestorage.GetEntity<SampleEntity>("tableName1", "partitionKey1/rowKey1");

// You retrieve multiple within a given table or partition as follows...
// Observable:
var items = tablestorage.ListEntitiesObservable<SampleEntity>("tableName1", "PartitionKey eq 'partitionKey1'").Subscribe(e =>
{
    // Do some processing here.
    Console.WriteLine(e.Key);
});

// Enumerable:
var items = tablestorage.ListEntities<SampleEntity>("tableName1", "PartitionKey eq 'partitionKey1'");

foreach(var item in items)
{
    // Do some processing here.
    Console.WriteLine(e.Key);
}
```
Read more on the types of query you can use [here](https://docs.microsoft.com/en-us/dotnet/api/microsoft.windowsazure.storage.table.tablequery?view=azure-dotnet);

### Delete
You can delete a single or multiple entities as follows:

```csharp
// Delete single.
await tablestorage.DeleteEntity("tableName1", "partitionKey1/rowKey1");

// Delete multiple.
await tablestorage.DeleteEntites("tableName1", new List<string>() { "partitionKey1/rowKey1", "partitionKey1/rowKey2", "partitionKey1/rowKey3" })
```


### Exists
Check to see if an entity exists with the supplied table and key as follows:

```csharp
var exists = await tablestorage.Exists("tableName1", "partitionKey1/rowKey1");
```

If you need anything more specific you can use the ListEntities method with a query containing exactly what you need.




**Note** - Do not update the Microsoft.IdentityModel.Clients.ActiveDirectory package.  It should be set to version 3.19.8.  This is the only package which overlaps between other Cloud.Core packages and must be kept inline (either update all or leave all as is currently).


## Test Coverage
A threshold will be added to this package to ensure the test coverage is above 80% for branches, functions and lines.  If it's not above the required threshold 
(threshold that will be implemented on ALL of the core repositories to gurantee a satisfactory level of testing), then the build will fail.

## Compatibility
This package has has been written in .net Standard and can be therefore be referenced from a .net Core or .net Framework application. The advantage of utilising from a .net Core application, 
is that it can be deployed and run on a number of host operating systems, such as Windows, Linux or OSX.  Unlike referencing from the a .net Framework application, which can only run on 
Windows (or Linux using Mono).
 
## Setup
This package is built using .net Standard 2.1 and requires the .net Core 3.1 SDK, it can be downloaded here: 
https://www.microsoft.com/net/download/dotnet-core/

IDE of Visual Studio or Visual Studio Code, can be downloaded here:
https://visualstudio.microsoft.com/downloads/

## How to access this package
All of the Cloud.Core.* packages are published to a public NuGet feed.  To consume this on your local development machine, please add the following feed to your feed sources in Visual Studio:
https://dev.azure.com/cloudcoreproject/CloudCore/_packaging?_a=feed&feed=Cloud.Core
 
For help setting up, follow this article: https://docs.microsoft.com/en-us/vsts/package/nuget/consume?view=vsts


<a href="https://dev.azure.com/cloudcoreproject/CloudCore" target="_blank">
<img src="https://cloud1core.blob.core.windows.net/icons/cloud_core_small.PNG" />
</a>
