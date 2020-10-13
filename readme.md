# Blob Query

## Introduction

It is a common pattern in big data environments that historic data is appended to a file in chronologic order and each row is serialized in JSON, CSV or TSV. In Azure, we use the [Append blob](https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs#about-append-blobs) for that use case.

An example looks like this:

```json
{"timestamp":"2020-04-01T17:42:54+00:00","event":"processed"}
{"timestamp":"2020-04-01T17:42:57+00:00","event":"delivered","response":"250 2.0.0 OK  1585762986 d13si179870eju.314 - gsmtp"}
{"timestamp":"2020-04-01T18:27:06+00:00","event":"open"}
```

Also, these files are usually not global, because that would introduce a continuous challenge for acquiring a writer lock among the clients, which would decrease the efficiency of the system by a lot. Instead, they are partitioned by an aggregate root (in this example, these are mail delivery logs, so by e-mail address, for example), and to avoid getting the files too big, by time as well. So, their naming would look like this:

```
/delivery-logs/recipient@example.org/2020/09/logs.jsonl
```

Note: in some cases, since data written in this format is usually historic, time may be more important (for example, when we usually care about the last month, last year or last few years; or we need to delete outdated data in an easier way) than the related entity it belongs to, so the partitioning can be in another order like `/delivery-logs/2020/09/recipient@example.org/logs.jsonl`.

## Query

These files are usually consumed by big data analytics systems, that are designed for offline data processing.

But in some cases, where the data is partitioned the right way, it is not frequently access and a little longer latency is acceptable, it can be used as an online data storage as well. It can be also be used as a substitute for Table Storage, for storing historic data and substituting the partition key as the blob name.

Since the concept of [`IAsyncEnumerable`](https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.iasyncenumerable-1) is available, we could stream the contents of the blob, parse on the fly and let the client decide the query. Usage would look like this:

```cs
await foreach (var row in blob.ReadLinesAsJson<DeliveryLogEntry>()
	.Where(e => e.Event == DeliveryEvent.Open)
	.Take(10)
)
{
	// ...
}
```

### Implementation #1

- Usage: **easy**
- Implementation: **easy**

The easy way to implement this is to use a `TextReader` to read the lines. Its construction is straightforward from `BaseBlobClient` > `Stream` > `StreamReader` (`TextReader`).

```cs
public static async IAsyncEnumerable<string> ReadLinesAsync(
    this TextReader reader,
    CancellationToken cancellationToken = default
)
{
    string line = await reader.ReadLineAsync();
    while (line != null)
    {
        yield return line;
        cancellationToken.ThrowIfCancellationRequested();
        line = await reader.ReadLineAsync();
    }
}
```

And then parse them as JSON:

```cs
public static async IAsyncEnumerable<T> ReadLinesAsJson<T>(
    this TextReader reader,
    JsonSerializerOptions jsonSerializerOptions,
    CancellationToken cancellationToken = default
)
{
    await foreach (var line in reader.ReadLinesAsync(cancellationToken))
    {
        if (String.IsNullOrWhiteSpace(line))
        {
            continue;
        }

        yield return JsonSerializer.Deserialize<T>(line, jsonSerializerOptions);
    }
}
```

While this implementation is easy, it materializes each line as a new [String](https://docs.microsoft.com/en-us/dotnet/api/system.string), while they are used only once and discarded right after use. The Strings get allocated to the [heap](https://docs.microsoft.com/en-us/dotnet/standard/garbage-collection/fundamentals#the-managed-heap) and thus put pressure on the [Garbage Collector](https://docs.microsoft.com/en-us/dotnet/standard/garbage-collection/).

### Implementation #2

- Usage: **easy**
- Implementation: **advanced**

Instead of materializing each line, let's harness the power of a new concept called [Pipelines](https://docs.microsoft.com/en-us/dotnet/standard/io/pipelines), that can help with buffer/memory management.

```cs
public static async IAsyncEnumerable<T> ReadLinesAsJson<T>(
    this Stream stream,
    JsonSerializerOptions jsonSerializerOptions,
    CancellationToken cancellationToken = default
)
{
    PipeReader reader = PipeReader.Create(stream);

    while (true)
    {
        // try read
        var result = await reader.ReadAsync(cancellationToken);
        if (result.IsCanceled)
        {
            break;
        }

        // try find individual lines
        ReadOnlySequence<byte> buffer = result.Buffer;

        while (TryReadLine(ref buffer, out ReadOnlySequence<byte> line))
        {
            yield return Deserialize<T>(line, jsonSerializerOptions);
        }

        // advance reader
        reader.AdvanceTo(buffer.Start, buffer.End);

        // check whether we are at the end of the stream
        if (result.IsCompleted)
        {
            break;
        }
    }
}

private static bool TryReadLine(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> line)
{
    // try to find next line separator
    SequencePosition? position = buffer.PositionOf((byte)'\n');

    // not found
    if (position == null)
    {
        line = default;
        return false;
    }

    // create a window in the byte sequence
    line = buffer.Slice(0, position.Value);
    buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
    return true;
}
```

Instead of [TextReader](https://docs.microsoft.com/en-us/dotnet/api/system.io.textreader), we create a [PipeReader](https://docs.microsoft.com/en-us/dotnet/api/system.io.pipelines.pipereader) to read data from the stream. While TextReader is a high level concept, PipeReader provides low level data access to a Stream, while also solves buffer management and makes the read data available for processing synchronously.

An intermediate solution could have been using the raw Stream and preallocating a buffer `byte[4096]` big enough to read data into, and then pass a window of this buffer as `ReadOnlySpan<byte>` to the JSON serializer. But this approach would introduce two hard problems:
  - what if a line doesn't fit into this buffer? we need to create another one, and then connect the data somehow. what if still doesn't fit? copy them into a single buffer finally to make processing possible?
  - what if buffer length is not an exact multiple of line lengths? then we end up having the first half of a line already read into the end of our buffer. we need to rearrange the data in the buffer, to be able to read the next batch of bytes and keep the data in order for processing.

So, buffer management while keeping data in context is not easy at all, in fact, it is very hard to do efficiently. This is where PipeReader comes into play. What it does is, it allocates buffers on demand, builds a graph of them and maintains which part of which buffer is free, and which part contains data, and in which order they have to be connected. When we read from the Pipeline and then we request data for processing, we don't get a simple array of bytes `byte[]`, or a span of bytes `ReadOnlySpan<byte>` inside a larger buffer, but we get a sequence of bytes `ReadOnlySequence<byte>` that spans across buffers and connects the relevant segments in them in the right order.

Fortunately, the new JSON serializer supports reading from a `ReadOnlySpan<byte>`, we just have to create a `Utf8JsonReader` for that (which is a value type):

```cs
private static T Deserialize<T>(ReadOnlySequence<byte> line, JsonSerializerOptions jsonSerializerOptions)
{
    var reader = new Utf8JsonReader(line);
    return JsonSerializer.Deserialize<T>(ref reader, jsonSerializerOptions);
}
```

This implementation could fit most real world scenarios, as the implementation is efficient (complex, but hidden), while the usage doesn't suffer complexity, it is as simple and general as in the first case.

### Appendix #1

- Usage: **advanced**
- Implementation: **advanced**

While the previous implementation saves a lot of memory pressure, it still produces a lot of garbage for rows that are eventually filtered out. In a query like this:

```cs
blob.ReadLinesAsJson<DeliveryLogEntry>().Skip(10)
```

or:

```cs
blob.ReadLinesAsJson<DeliveryLogEntry>().Where(e => e.Event == DeliveryEvent.Open)
```

records are still fully parsed and materialized from JSON to objects with all their properties and values, which can be reference types themselves too and discarded right after parsing.

So, instead of materialized/deserialized objects, we could defer serialization by returning only the `Utf8JsonReader` instances to let the user parse only the neccessary parts to determine whether that row is needed or not; and also skip parsing when the content of the row is not related to the filter (e.g.: `Skip` operator). But since `Utf8JsonReader` is a `ref struct`, we can't use it as a generic type argument unfortunately that prevents us to use it in `IAsyncEnumerable`.

To achieve zero allocations for filtered entities, we have to build specialized readers, for example:

```cs
public static async IAsyncEnumerable<T> ReadLinesAsJson<T>(
    this Stream stream,
    Func<Utf8JsonReader, bool> predicate,
    JsonSerializerOptions jsonSerializerOptions,
    CancellationToken cancellationToken = default
)
```

This specialized method would know about filtering, like the `Where` operator, and it would incorporate that logic into itself. It would call `if (predicate(Reader))` before actually deserializes the object.

This method highly limits use cases by having to predefine supported operators, and also limits composability, but specialization is the price we have to pay for performance.
