using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Papst.EventStore.Abstractions;
using Papst.EventStore.Abstractions.EventAggregation;
using Papst.EventStore.Abstractions.EventRegistration;
using Papst.EventStore.Abstractions.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Papst.EventStore.AzureTableStorage;

internal class TableEventStore : IEventStore
{
  private const string SnapshotRowKey = "Snapshot";
  private readonly ILogger<TableEventStore> _logger;
  private readonly IOptions<TableEventStoreOptions> _options;
  private readonly IEventTypeProvider _typeProvider;
  private readonly TableClient _tableClient;

  private static class FieldNames
  {
    internal const string DocumentId = "DocumentId";
    internal const string Version = "Version";
    internal const string DocumentType = "DocumentType";
    internal const string Name = "Name";
    internal const string Data = "Data";
    internal const string DataType = "DataType";
    internal const string TargetType = "TargetType";
    internal const string MetaDataUserId = "MetaData_UserId";
    internal const string MetaDataUserName = "MetaData_UserName";
    internal const string MetaDataTenantId = "MetaData_TenantId";
    internal const string MetaDataComment = "MetaData_Comment";
    internal const string MetaDataAdditional = "MetaData_Additional";
  }

  public TableEventStore(ILogger<TableEventStore> logger, IOptions<TableEventStoreOptions> options, IEventTypeProvider typeProvider)
  {
    _logger = logger;
    _options = options;
    _typeProvider = typeProvider;

    if (_options.Value.Credential == null)
    {
      throw new NotSupportedException("Credential for Table access is not set");
    }

    _tableClient = new TableClient(
      new Uri($"https://{_options.Value.StorageAccountName}.table.core.windows.net"),
      _options.Value.TableName,
      _options.Value.Credential);
  }

  public async Task<EventStoreResult> AppendAsync(Guid streamId, ulong expectedVersion, EventStreamDocument doc, CancellationToken token = default)
  {
    _logger.LogDebug("Start appending single event to {StreamId} with Expected Version {Version}", streamId, expectedVersion);
    ulong version = expectedVersion + 1;
    TableEntity document = Map(streamId, version, doc);

    Response result = await _tableClient.AddEntityAsync(document, token).ConfigureAwait(false);

    if (result.Status == 201) // Created
    {
      _logger.LogInformation("Inserted with {Version} to {Stream}", document.RowKey, document.PartitionKey);
      return new EventStoreResult
      {
        DocumentId = doc.Id,
        StreamId = streamId,
        Version = version,
        IsSuccess = true,
      };
    }
    else if (result.Status == 409) // Conflict
    {
      _logger.LogWarning(
          "Inserting {Document} with {Version} to {Stream} failed because of Version Conflict",
          doc.Id,
          version,
          streamId
      );
      throw new EventStreamVersionMismatchException(streamId, expectedVersion, version, "A Document with that Version already exists");
    }
    else
    {
      _logger.LogError(
          "Inserted {Document} with {Version} to {Stream} failed with {Reason}",
          doc.Id,
          version,
          streamId,
          result.ReasonPhrase);

      return new EventStoreResult
      {
        DocumentId = null,
        StreamId = streamId,
        Version = expectedVersion,
        IsSuccess = false
      };
    }
  }

  private static TableEntity Map(Guid streamId, ulong version, EventStreamDocument doc)
    => Map(streamId, version.ToString(), version, doc);
  private static TableEntity Map(Guid streamId, string rowKey, ulong version, EventStreamDocument doc)
  {
    return new TableEntity(streamId.ToString(), rowKey)
    {
      [FieldNames.DocumentId] = doc.Id,
      [FieldNames.Version] = (long)version,
      [FieldNames.DocumentType] = doc.DocumentType.ToString(),
      [FieldNames.Name] = doc.Name,
      [FieldNames.Data] = doc.Data.ToString(),
      [FieldNames.DataType] = doc.DataType,
      [FieldNames.TargetType] = doc.TargetType,
      [FieldNames.MetaDataUserId] = doc.MetaData.UserId,
      [FieldNames.MetaDataUserName] = doc.MetaData.UserName,
      [FieldNames.MetaDataTenantId] = doc.MetaData.TenantId,
      [FieldNames.MetaDataComment] = doc.MetaData.Comment,
      [FieldNames.MetaDataAdditional] = doc.MetaData.Additional != null ? JsonConvert.SerializeObject(doc.MetaData.Additional) : "{}"
    };
  }

  private static EventStreamDocument Map(TableEntity entity)
  {
    return new()
    {
      Id = entity.ContainsKey(FieldNames.DocumentId) ? entity.GetGuid(FieldNames.DocumentId)!.Value : throw new EventStreamException("Document does not contain Id"),
      Version = entity.ContainsKey(FieldNames.Version) ? (ulong)(entity.GetInt64(FieldNames.Version)!.Value) : throw new EventStreamException("Document does not contain Version"),
      StreamId = Guid.Parse(entity.PartitionKey),
      DocumentType = entity.ContainsKey(FieldNames.DocumentType) ? Enum.Parse<EventStreamDocumentType>(entity.GetString(FieldNames.DocumentType)) : throw new EventStreamException("Document does not contain DocumentType"),
      Name = entity.ContainsKey(FieldNames.Name) ? entity.GetString(FieldNames.Name) : throw new EventStreamException("Document does not contain Name"),
      Data = entity.ContainsKey(FieldNames.Data) ? JObject.Parse(entity.GetString(FieldNames.Data)) : JObject.Parse("{}"),
      DataType = entity.ContainsKey(FieldNames.DataType) ? entity.GetString(FieldNames.DataType) : throw new EventStreamException("Document does not contain DataType"),
      TargetType = entity.ContainsKey(FieldNames.TargetType) ? entity.GetString(FieldNames.TargetType) : throw new EventStreamException("Document does not contain DataType"),
      MetaData = new()
      {
        UserId = entity.ContainsKey(FieldNames.MetaDataUserId) ? entity.GetGuid(FieldNames.MetaDataUserId) : null,
        UserName = entity.ContainsKey(FieldNames.MetaDataUserName) ? entity.GetString(FieldNames.MetaDataUserName) : null,
        TenantId = entity.ContainsKey(FieldNames.MetaDataTenantId) ? entity.GetGuid(FieldNames.MetaDataTenantId) : null,
        Comment = entity.ContainsKey(FieldNames.MetaDataComment) ? entity.GetString(FieldNames.MetaDataComment) : null,
        Additional = entity.ContainsKey(FieldNames.MetaDataAdditional) ? JsonConvert.DeserializeObject<Dictionary<string, string>>(entity.GetString(FieldNames.MetaDataAdditional)) : null
      }
    };
  }

  public async Task<EventStoreResult> AppendAsync(Guid streamId, ulong expectedVersion, IEnumerable<EventStreamDocument> documents, CancellationToken token = default)
  {
    _logger.LogDebug("Start appending multiple events to {StreamId} with Expected Version {Version}", streamId, expectedVersion);

    if (!documents.Any())
    {
      throw new NotSupportedException("Document Collection must not be empty");
    }

    ulong version = expectedVersion + 1;
    List<TableEntity> tableDocuments = new(documents.Select(doc => Map(streamId, version++, doc)));

    Response<IReadOnlyList<Response>> result = await _tableClient.SubmitTransactionAsync(
      tableDocuments.Select(doc => new TableTransactionAction(TableTransactionActionType.Add, doc)),
      token).ConfigureAwait(false);

    if (!result.Value.Any(res => res.Status != 201))
    {
      _logger.LogError(
          "Inserting Multiple Documents to {Stream} failed with {Reason}",
          streamId,
          result.Value.First(res => res.Status != 201).ReasonPhrase);
      return new EventStoreResult
      {
        DocumentId = null,
        StreamId = streamId,
        IsSuccess = false
      };
    }
    else
    {
      _logger.LogInformation(
        "Inserted Documents to {Stream} with new {Version}",
        streamId,
        version
      );

      return new EventStoreResult
      {
        DocumentId = null,
        StreamId = streamId,
        IsSuccess = true,
        Version = version
      };
    }
  }

  public async Task<EventStoreResult> AppendEventAsync<TDocument, TTargetType>(Guid streamId, string name, ulong expectedVersion, TDocument document, Guid? userId = null, string? username = null, Guid? tenantId = null, string? comment = null, Dictionary<string, string>? additional = null, CancellationToken token = default) where TDocument : class
  => await AppendAsync(
    streamId,
    expectedVersion,
    new EventStreamDocument
    {
      Id = Guid.NewGuid(),
      StreamId = streamId,
      DocumentType = EventStreamDocumentType.Event,
      Data = JObject.FromObject(document),
      DataType = _typeProvider.ResolveType(typeof(TDocument)),
      TargetType = TypeUtils.NameOfType(typeof(TTargetType)),
      Name = name,
      Time = DateTimeOffset.Now,
      Version = 0,
      MetaData = new EventStreamMetaData
      {
        UserId = userId,
        UserName = username,
        Comment = comment,
        TenantId = tenantId,
        Additional = additional
      }
    },
    token
  );

  public async Task<EventStoreResult> AppendSnapshotAsync(Guid streamId, ulong expectedVersion, EventStreamDocument snapshot, bool deleteOlderSnapshots = true, CancellationToken token = default)
  {
    // Set Latest snapshot
    TableEntity snapshopEntity = Map(streamId, SnapshotRowKey, expectedVersion + 1, snapshot);
    _logger.LogInformation("Upserting Snapshot for {StreamId} with version {Version}", streamId, expectedVersion + 1);
    await _tableClient.UpsertEntityAsync(snapshopEntity, TableUpdateMode.Replace, token).ConfigureAwait(false);

    return await AppendAsync(streamId, expectedVersion, snapshot, token).ConfigureAwait(false);
  }

  public async Task<IEventStream> CreateAsync(Guid streamId, EventStreamDocument doc, CancellationToken token = default)
  {
    _logger.LogInformation("Creating {Stream}", streamId);
    TableEntity document = Map(streamId, 0, doc);

    Response result = await _tableClient.AddEntityAsync(document, token).ConfigureAwait(false);

    if (result.Status == 201) // Created
    {
      return await ReadAsync(streamId, token).ConfigureAwait(false);
    }
    else
    {
      _logger.LogWarning(
          "Inserting {Document} with {Version} to {Stream} failed because of Version Conflict",
          doc.Id,
          0,
          streamId
      );
      throw new EventStreamVersionMismatchException(streamId, 0, 0, "A Document with that Version already exists");
    }
  }

  public async Task<IEventStream> CreateEventStreamAsync<TDocument, TTargetType>(Guid streamId, string name, TDocument document, Guid? userId = null, string? username = null, Guid? tenantId = null, string? comment = null, Dictionary<string, string>? additional = null, CancellationToken token = default) where TDocument : class
  {
    return await CreateAsync(
    streamId,
    new EventStreamDocument
    {
      Id = Guid.NewGuid(),
      StreamId = streamId,
      DocumentType = EventStreamDocumentType.Event,
      Data = JObject.FromObject(document),
      DataType = _typeProvider.ResolveType(typeof(TDocument)),
      TargetType = TypeUtils.NameOfType(typeof(TTargetType)),
      Name = name,
      Time = DateTimeOffset.Now,
      Version = 0,
      MetaData = new EventStreamMetaData
      {
        UserId = userId,
        UserName = username,
        Comment = comment,
        TenantId = tenantId,
        Additional = additional
      }
    },
    token
    );
  }

  public async Task<IEventStream> ReadAsync(Guid streamId, ulong fromVersion, CancellationToken token = default)
  {
    return await ReadInternalAsync(streamId, fromVersion, token).ConfigureAwait(false);
  }

  private async Task<TableEventStream> ReadInternalAsync(Guid streamId, ulong fromVersion, CancellationToken token)
  {
    var call = _tableClient.QueryAsync<TableEntity>($"PartitionKey eq '{streamId}' AND RowKey gte '{fromVersion}'", 100, cancellationToken: token).ConfigureAwait(false);

    List<EventStreamDocument> documents = new();
    await foreach (TableEntity entity in call)
    {
      documents.Add(Map(entity));
    }

    return new TableEventStream(streamId, documents);
  }

  public Task<IEventStream> ReadAsync(Guid streamId, CancellationToken token = default)
    => ReadAsync(streamId, 0, token);

  public async Task<IEventStream> ReadFromSnapshotAsync(Guid streamId, CancellationToken token = default)
  {
    // read snapshot
    NullableResponse<TableEntity> snapshot = await _tableClient.GetEntityIfExistsAsync<TableEntity>(streamId.ToString(), SnapshotRowKey, cancellationToken: token).ConfigureAwait(false);

    if (!snapshot.HasValue)
    {
      throw new EventStreamException($"Snapshot for Stream {streamId} does not exist!");
    }

    var snapshotDocument = Map(snapshot.Value);

    return await ReadAsync(streamId, snapshotDocument.Version, token).ConfigureAwait(false);
  }
}
