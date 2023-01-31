using Azure.Core;

namespace Papst.EventStore.AzureTableStorage;
public class TableEventStoreOptions
{
  public string StorageAccountName { get; set; } = string.Empty;
  public string TableName { get; set; } = string.Empty;
  public TokenCredential? Credential { get; set; }
}
