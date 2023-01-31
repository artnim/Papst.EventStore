using Papst.EventStore.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Papst.EventStore.AzureTableStorage;
internal class TableEventStream : IEventStream
{
  public Guid StreamId { get; }

  private readonly List<EventStreamDocument> _documents;

  public EventStreamDocument? LatestSnapShot => _documents.LastOrDefault(doc => doc.DocumentType == EventStreamDocumentType.Snapshot);

  public IReadOnlyList<EventStreamDocument> Stream => _documents;

  internal TableEventStream(Guid streamId, IEnumerable<EventStreamDocument> documents)
  {
    StreamId = streamId;
    _documents = documents as List<EventStreamDocument> ?? documents.ToList();
  }

  internal void InsertSnapshot(EventStreamDocument snapshot)
  {
    int index = 0;
    foreach (var document in _documents)
    {
      if (document.Version > snapshot.Version)
      {
        break;
      }
      index++;
    }

    _documents.Insert(index, snapshot);
  }
}
