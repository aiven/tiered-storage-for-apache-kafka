# Tiered Storage for Apache Kafka®

This project is an implementation of `RemoteStorageManager` for Apache Kafka tiered storage.

The implementation will have multiple configurable storage backends.
Currently, AWS S3, Google Cloud Storage, and Azure Blob Storage are supported.

The project follows the API specifications according to the latest version of [KIP-405: Kafka Tiered Storage](https://cwiki.apache.org/confluence/x/KJDQBQ).

## References

- [Configuration](./docs/configs.rst)
- [Metrics](./docs/metrics.rst)

## Getting started

You can see the [demo/](demo/README.md) directory for some configured setups.

Here are some steps you need to make to your brokers to get started with the plugin.

**Step 1. Prepare the plugin**:

The plugin consists of the core library (`core-<version>.tgz`) 
and object storage-specific libraries (`azure-<version>.tgz`, `gcs-<version>.tgz`, `s3-<version>.tgz`).
Extract the core library and selected storage library to the same or different directories.

**Step 2. Configure the brokers**:

```properties
# ----- Enable tiered storage at the broker level -----

remote.log.storage.system.enable=true

# ----- Configure the remote log metadata manager -----

# This is the default, but adding it for explicitness:
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
# Put the real listener name you'd like to use here to create the metadata topic:
remote.log.metadata.manager.listener.name=PLAINTEXT
# You may need to set this if you're experimenting with a single broker setup:
# rlmm.config.remote.log.metadata.topic.replication.factor=1
# Configure client connection details to access metadata topic
# rlmm.config.remote.log.metadata.common.client.*

# ----- Configure the remote storage manager -----

# Here you need either one or two directories depending on what you did in Step 1:
remote.log.storage.manager.class.path=/path/to/core/*:/path/to/storage/*
remote.log.storage.manager.class.name=io.aiven.kafka.tieredstorage.RemoteStorageManager

# 4 MiB is the current recommended chunk size:
rsm.config.chunk.size=4194304

# ----- Configure the storage backend -----

# Using GCS as an example:
rsm.config.storage.backend.class=io.aiven.kafka.tieredstorage.storage.gcs.GcsStorage
rsm.config.storage.gcs.bucket.name=my-bucket
rsm.config.storage.gcs.credentials.default=true
# The prefix can be skipped:
#rsm.config.storage.key.prefix: "some/prefix/"

# ----- Configure the fetch chunk cache -----

rsm.config.fetch.chunk.cache.class=io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache
rsm.config.fetch.chunk.cache.path=/cache/root/directory
# Pick some cache size, 16 GiB here:
rsm.config.fetch.chunk.cache.size=17179869184
# Prefetching size, 16 MiB here:
rsm.config.fetch.chunk.cache.prefetch.max.size=16777216
# Cache retention time ms, where -1 represents infinite retention
rsm.config.fetch.chunk.cache.retention.ms=600000

# ----- Configure the upload rate limit -----
# 100-200 MiB is the current recommended range for rate limiting
rsm.config.upload.rate.limit.bytes.per.second=104857600
```

You may want to tweak `remote.log.manager.task.interval.ms` 
and `log.retention.check.interval.ms` to see the tiered storage effects faster. 
However, you probably don't need to change this in production setups.

**Step 3. Start the broker**.

**Step 4. Create and fill the topic**.

```shell
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic topic1 \
    --config remote.storage.enable=true \
    --config segment.bytes=512000 \
    --config local.retention.bytes=1 \
    --config retention.bytes=10000000000000
   
bin/kafka-producer-perf-test.sh \
    --topic topic1 --num-records=10000 --throughput -1 --record-size 1000 \
    --producer-props acks=1 batch.size=16384 bootstrap.servers=localhost:9092
```

**Step 5**. After some time, you will start seeing segments on the remote storage, and later they will be deleted locally.

## Design

### Requirements

Conceptually, `RemoteStorageManager` is very simple and exposes a CRUD interface for 
uploading, (ranged) fetching, and deleting Kafka log segment files.

This implementation was done with few additional requirements:

1. Support for AWS S3, Google Cloud Storage, and Azure Blob Storage.
2. Compression. It must provide optional (configurable) compression. The compression must be conditional and double compression of Kafka-compressed log segments should be avoided.
3. Encryption. It must provide optional (configurable) encryption with key rotation support.
4. Small download overhead. It must avoid wasted download from remote storage.
5. Low impact upload. It must not interfere with normal broker operations

### Storage backends

There's little difference in how data is written to and read from different cloud object storages, mostly in using different SDKs.
This implementation abstracts away the concept of storage backend. This allows to select the storage backend with configuration.

### Chunking

Compression and encryption make it difficult to do ranged queries.
To read an encrypted file from the middle, one needs to decompress / decrypt--and so download--the whole prefix.
As files can be large, this may lead to huge download overhead.

One way to combat this is chunking.
With chunking, the original file is split into chunks, which are transformed (compressed, encrypted) independently.

```
Original file:               Transformed file:
+-----------+ -------\
|           |         \
|  Chunk 1  |          \----- +-----------+
|           |                 |  Chunk 1  |
+-----------+ --------------- +-----------+
|           |                 |  Chunk 2  |
|  Chunk 2  |        /------- +-----------+
|           |       /         |  Chunk 3  |
+-----------+ -----/     /--- +-----------+
|  Chunk 3  |           /
+-----------+ ---------/
```

Now, knowing the sizes of original and transformed chunks, it's possible to map from a position in the original file `Po` to a position in the transformed file `Pt` within the accuracy of a chunk size.
Thus, some overhead remains, but it is reduced as much as chunks are smaller than the whole file.

#### Concatenation and indexing

Splitting a file into multiple chunks may significantly increase the overhead of remote storage operations--most notably writes and lists--which are usually billed for.
To avoid this, transformed chunks could be concatenated back into a single blob and uploaded as one.

To maintain the ability to map offsets/chunks between original and transformed files, an index is needed.

The index must be stored on the remote storage along with other files related to a log segment to keep these uploads self-sufficient.

It's important to keep the size of these indices small.
One technique is to keep the size of original chunks fixed and explicitly list only the transformed side of chunks.
Furthermore, if compression is not used, the size of transformed chunks also remains fixed.

##### Index encoding

Binary encoding could reduce the index size further.
Values are encoded as a difference from a common base and the encoding uses the minimum number of bytes per value.
See the [javadoc](core/src/main/java/io/aiven/kafka/tieredstorage/manifest/index/serde/ChunkSizesBinaryCodec.java) for `ChunkSizesBinaryCodec` for details. We could expect 1-2 bytes per value on average.

A 2 GB file chunked into 1 MB chunks produce about 2000 index entries, which is 2-4 KB.

The index could be compressed with Zstd after that and have to be Base64-encoded (about 30% overhead). This results in 2.7-3.7 KB.

This encoding scheme allows indices to be stored (cached) in memory and accessed with little overhead.

**Rejected alternative**: An alternative approach may be to just write values as integers (4 byte per value) and rely on Zstd to compress it significantly.
Despite this produces similar results size-wise (3.6-4.2 KB, i.e. a little worse), it's difficult to store indices in memory in the encoded form, because this would require decompression on each access.

### Compression

Optional data compression with Zstandard is supported.
It can be enabled always or conditionally based on a heuristic.

#### Compression heuristic

For each segment that is to be uploaded, the heuristic checks if the first batch in the segment is compressed.
If it is not compressed, the whole segment is considered not compressed and compression is performed.

#### On compression dictionaries

It's possible to further reduce the size of compressed chunks if, instead of compressing them independently, first pretrain a shared compression dictionary.
This is not implemented now and will potentially be addressed in the future: preliminary research didn't demonstrate significant advantage.

### Encryption

Optional client-side encryption is supported.
Encryption is implemented with the envelope scheme with the key ring.

```
+----------------+                 +----------------+                 +-----------+
|      key       |                 |      data      |                 |  segment  |
| encryption key | ---encrypts---> | encryption key | ---encrypts---> |   data    |
+----------------+                 +----------------+                 +-----------+ 
```

Each segment has its unique AES-256 data encryption key (DEK).
The segment data and indices are encrypted with this key.
The DEK itself is encrypted with the public RSA key encryption key (KEK).
The KEK ID is stored together with the encrypted DEK so that the corresponding KEK (the private one) can be found on the key ring for decryption.

The key ring may contain several KEKs. One of them is active (used for encrypting DEKs during upload).

With this approach, it's possible to start using a new KEK for new data: keep the old KEKs on the key ring (for decrypting old data) and add the new KEK for new data.
It's also possible to rotate the old KEKs.
For this, the new KEK needs to be added to the key ring and then old DEKs must be re-encrypted with the new KEK and segment manifests updated.
This can be done gradually and transparently.

### Uploads

Uploads happen as scheduled background tasks. 
A broker schedules tasks per hosted replica leaders, sequentially uploading segments from oldest to newest, without including the active segment.

By default, the thread pool size is 10, so at most 10 concurrent uploads can happen at the same time.
This could be a resource consuming activity if without the proper constraints as uploading will take network bandwidth and CPU time.

#### Rate Limit

To avoid impacting broker operations, the plugin has implemented a rate limiting feature to define an upper-bound of bytes to be uploaded per broker.
This is implemented as a bucket of a defined size, shared per broker.
Once a bucket is filled (let's say 100MiB are being uploaded), then operations will wait until there are resources available.

Why not relying [Tiered Storage Quotas](https://cwiki.apache.org/confluence/display/KAFKA/KIP-956+Tiered+Storage+Quotas)?

Even though Quotas are useful, their granularity is measured per segment upload operation. 
This means that there is not control on how fast a single segment would be uploaded.

The plugin rate-limit is complementary to Tiered Storage Quotas.

#### S3 Multipart Upload

When uploading processed segments and indexes, multipart upload is used to put files on S3 back-end.

Even though, multipart transactions are aborted when an exception happens while processing, 
there's a chance that initiated transactions are not completed or aborted (e.g. broker process is killed) and incomplete part uploads hang without completing a transaction.
For these scenarios, is recommended to set a bucket lifecycle policy to periodically abort incomplete multipart uploads: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html>

### Ranged queries

The plugin uses ranged queries to access records on remote log segment.
Based on the [chunking](#chunking), the plugin will use the chunk position to get a range from the uploaded log segment.
Once downloaded, the record batch is served to the broker as an input stream.

The plugin will cache the chunks for potential reprocessing, and it does pre-fetching of next chunks when configured.

#### Local cache

Even in case of sequential reads, chunks may be required multiple times in a short period of time.
Fetching them each time from the remote storage would negatively affect the latency.
To mitigate this, the chunk caching is supported.
There are two built-in implementations: the in-memory chunk cache and disk-based chunk cache. In the most cases, the former cannot be used in production.

#### Prefetching

The cache is able to asynchronously prefetch next chunks, up to the specified number of bytes.
This positively affects sequential read performance.
At the moment, prefetching is limited with segment borders, i.e. it cannot prefetch from the following segment.

### SOCKS5 proxy

⚠️ This is an experimental feature subject for future changes.

| Object storage       | Supported | Host name resolution |
|----------------------|:---------:|:--------------------:|
|        AWS S3        |     ✅     |     Client-side      |
| Azure Blob Storage   |     ✅     |      Proxy-side      |
| Google Cloud Storage |     ✅     |      Proxy-side      |

## License

The project is licensed under the Apache license, version 2.0. Full license test is available in the [LICENSE](LICENSE) file.

## Contributing

For a detailed guide on how to contribute to the project, please check [CONTRIBUTING.md](CONTRIBUTING.md).

## Code of conduct

This project uses the [Contributor Covenant](https://www.contributor-covenant.org/) code of conduct. Please check [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for further details.

## Trademark notices

Apache Kafka is either a registered trademark or trademark of the Apache Software Foundation in the United States and/or other countries.
All product and service names used in this page are for identification purposes only and do not imply endorsement.

## Credits

This project is maintained by, [Aiven](https://aiven.io/) open source developers.

Recent contributors are listed on the GitHub project page, <https://github.com/aiven/tiered-storage-for-apache-kafka/graphs/contributors>.

Copyright (c) 2022 Aiven Oy and project contributors.
