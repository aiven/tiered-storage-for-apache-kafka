/*
 * Copyright 2023 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.fetch.ChunkManager;
import io.aiven.kafka.tieredstorage.fetch.ChunkManagerFactory;
import io.aiven.kafka.tieredstorage.fetch.FetchChunkEnumeration;
import io.aiven.kafka.tieredstorage.fetch.KeyNotFoundRuntimeException;
import io.aiven.kafka.tieredstorage.fetch.index.MemorySegmentIndexesCache;
import io.aiven.kafka.tieredstorage.fetch.index.SegmentIndexesCache;
import io.aiven.kafka.tieredstorage.fetch.manifest.MemorySegmentManifestCache;
import io.aiven.kafka.tieredstorage.fetch.manifest.SegmentManifestCache;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadata;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadataV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndex;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1Builder;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.serde.EncryptionSerdeModule;
import io.aiven.kafka.tieredstorage.manifest.serde.KafkaTypeSerdeModule;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataBuilder;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataSerde;
import io.aiven.kafka.tieredstorage.metrics.Metrics;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.RsaKeyReader;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectDeleter;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.ObjectUploader;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.transform.BaseDetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.BaseTransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.CompressionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DecryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DetransformFinisher;
import io.aiven.kafka.tieredstorage.transform.EncryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.RateLimitedInputStream;
import io.aiven.kafka.tieredstorage.transform.TransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.TransformFinisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.bucket4j.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_NUM_SAMPLES_CONFIG;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG;

public class RemoteStorageManager implements org.apache.kafka.server.log.remote.storage.RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(RemoteStorageManager.class);

    private final Time time;

    private Metrics metrics;

    private ObjectFetcher fetcher;
    private ObjectUploader uploader;
    private ObjectDeleter deleter;
    private boolean compressionEnabled;
    private boolean compressionHeuristic;
    private boolean encryptionEnabled;
    private int chunkSize;
    private RsaEncryptionProvider rsaEncryptionProvider;
    private AesEncryptionProvider aesEncryptionProvider;
    private ObjectMapper mapper;
    private ChunkManager chunkManager;
    private ObjectKeyFactory objectKeyFactory;
    private SegmentCustomMetadataSerde customMetadataSerde;
    private Set<SegmentCustomMetadataField> customMetadataFields;

    private SegmentManifestCache segmentManifestCache;
    private SegmentIndexesCache segmentIndexesCache;

    private Bucket rateLimitingBucket;

    public RemoteStorageManager() {
        this(Time.SYSTEM);
    }

    // for testing
    RemoteStorageManager(final Time time) {
        this.time = time;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs must not be null");
        final RemoteStorageManagerConfig config = new RemoteStorageManagerConfig(configs);
        final MetricConfig metricConfig = new MetricConfig().samples(config.getInt(METRICS_NUM_SAMPLES_CONFIG))
            .timeWindow(config.getLong(METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
            .recordLevel(Sensor.RecordingLevel.forName(config.getString(METRICS_RECORDING_LEVEL_CONFIG)));
        metrics = new Metrics(time, metricConfig);
        setStorage(config.storage());
        objectKeyFactory = new ObjectKeyFactory(config.keyPrefix(), config.keyPrefixMask());
        encryptionEnabled = config.encryptionEnabled();
        if (encryptionEnabled) {
            final Map<String, KeyPair> keyRing = new HashMap<>();
            config.encryptionKeyRing().forEach(
                (keyId, keyPaths) -> keyRing.put(keyId, RsaKeyReader.read(keyPaths.publicKey, keyPaths.privateKey)));
            rsaEncryptionProvider = new RsaEncryptionProvider(config.encryptionKeyPairId(), keyRing);
            aesEncryptionProvider = new AesEncryptionProvider();
        }
        final ChunkManagerFactory chunkManagerFactory = new ChunkManagerFactory();
        chunkManagerFactory.configure(configs);
        chunkManager = chunkManagerFactory.initChunkManager(fetcher, aesEncryptionProvider);
        chunkSize = config.chunkSize();
        compressionEnabled = config.compressionEnabled();
        compressionHeuristic = config.compressionHeuristicEnabled();

        mapper = getObjectMapper();

        segmentManifestCache = new MemorySegmentManifestCache(fetcher, mapper);
        segmentManifestCache.configure(config.segmentManifestCacheConfigs());

        segmentIndexesCache = new MemorySegmentIndexesCache();
        segmentIndexesCache.configure(config.fetchIndexesCacheConfigs());

        customMetadataSerde = new SegmentCustomMetadataSerde();
        customMetadataFields = config.customMetadataKeysIncluded();
        config.globalRateLimit().ifPresent(value -> rateLimitingBucket = RateLimitedInputStream.rateLimitBucket(value));
        if (rateLimitingBucket == null) {
            config.uploadRateLimit()
                .ifPresent(value -> rateLimitingBucket = RateLimitedInputStream.rateLimitBucket(value));
        }
    }

    // for testing
    void setStorage(final StorageBackend storage) {
        fetcher = storage;
        uploader = storage;
        deleter = storage;
    }

    // for testing
    void setSegmentManifestCache(final MemorySegmentManifestCache segmentManifestCache) {
        this.segmentManifestCache = segmentManifestCache;
    }

    // for testing
    void setChunkManager(final ChunkManager chunkManager) {
        this.chunkManager = chunkManager;
    }

    private ObjectMapper getObjectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(KafkaTypeSerdeModule.create());
        if (encryptionEnabled) {
            objectMapper.registerModule(EncryptionSerdeModule.create(rsaEncryptionProvider));
        }
        return objectMapper;
    }

    @Override
    public Optional<CustomMetadata> copyLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                       final LogSegmentData logSegmentData)
        throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentId must not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData must not be null");

        log.info("Copying log segment data, metadata: {}", remoteLogSegmentMetadata);

        final var customMetadataBuilder =
            new SegmentCustomMetadataBuilder(customMetadataFields, objectKeyFactory, remoteLogSegmentMetadata);

        final long startedMs = time.milliseconds();

        try {
            final boolean requiresCompression = requiresCompression(logSegmentData);

            final DataKeyAndAAD maybeEncryptionKey;
            if (encryptionEnabled) {
                maybeEncryptionKey = aesEncryptionProvider.createDataKeyAndAAD();
            } else {
                maybeEncryptionKey = null;
            }

            final ChunkIndex chunkIndex = uploadSegmentLog(
                remoteLogSegmentMetadata,
                logSegmentData,
                requiresCompression,
                maybeEncryptionKey,
                customMetadataBuilder
            );

            final SegmentIndexesV1 segmentIndexes = uploadIndexes(
                remoteLogSegmentMetadata,
                logSegmentData,
                maybeEncryptionKey,
                customMetadataBuilder
            );

            uploadManifest(
                remoteLogSegmentMetadata,
                chunkIndex,
                segmentIndexes,
                requiresCompression,
                maybeEncryptionKey,
                customMetadataBuilder
            );
        } catch (final Exception e) {
            try {
                // best effort on removing orphan files
                deleteSegmentObjects(remoteLogSegmentMetadata);
            } catch (final Exception ex) {
                // ignore all exceptions
                log.warn("Removing orphan files failed", ex);
            }
            throw new RemoteStorageException(e);
        }

        metrics.recordSegmentCopyTime(
            remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            startedMs, time.milliseconds());

        final var customMetadata = buildCustomMetadata(customMetadataBuilder);

        log.info("Copying log segment data completed successfully, metadata: {}", remoteLogSegmentMetadata);

        return customMetadata;
    }

    private void deleteSegmentObjects(final RemoteLogSegmentMetadata metadata) throws StorageBackendException {
        final Set<ObjectKey> keys = Arrays.stream(ObjectKeyFactory.Suffix.values())
            .map(s -> objectKeyFactory.key(metadata, s))
            .collect(Collectors.toSet());
        deleter.delete(keys);
    }

    SegmentIndexesV1 uploadIndexes(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final LogSegmentData segmentData,
        final DataKeyAndAAD maybeEncryptionKey,
        final SegmentCustomMetadataBuilder customMetadataBuilder
    ) throws IOException, RemoteStorageException, StorageBackendException {
        final List<InputStream> indexes = new ArrayList<>(IndexType.values().length);
        final SegmentIndexesV1Builder segmentIndexBuilder = new SegmentIndexesV1Builder();

        try (final ClosableInputStreamHolder closableInputStreamHolder = new ClosableInputStreamHolder()) {
            final var offsetIndex = transformIndex(
                IndexType.OFFSET,
                closableInputStreamHolder.add(Files.newInputStream(segmentData.offsetIndex())),
                indexSize(segmentData.offsetIndex()),
                maybeEncryptionKey,
                segmentIndexBuilder
            );
            indexes.add(offsetIndex);
            final var timeIndex = transformIndex(
                IndexType.TIMESTAMP,
                closableInputStreamHolder.add(Files.newInputStream(segmentData.timeIndex())),
                indexSize(segmentData.timeIndex()),
                maybeEncryptionKey,
                segmentIndexBuilder
            );
            indexes.add(timeIndex);
            final var producerSnapshotIndex = transformIndex(
                IndexType.PRODUCER_SNAPSHOT,
                closableInputStreamHolder.add(Files.newInputStream(segmentData.producerSnapshotIndex())),
                indexSize(segmentData.producerSnapshotIndex()),
                maybeEncryptionKey,
                segmentIndexBuilder
            );
            indexes.add(producerSnapshotIndex);
            final var leaderEpoch = transformIndex(
                IndexType.LEADER_EPOCH,
                closableInputStreamHolder.add(new ByteBufferInputStream(segmentData.leaderEpochIndex())),
                segmentData.leaderEpochIndex().remaining(),
                maybeEncryptionKey,
                segmentIndexBuilder
            );
            indexes.add(leaderEpoch);
            if (segmentData.transactionIndex().isPresent()) {
                final var transactionIndex = transformIndex(
                    IndexType.TRANSACTION,
                    closableInputStreamHolder.add(Files.newInputStream(segmentData.transactionIndex().get())),
                    indexSize(segmentData.transactionIndex().get()),
                    maybeEncryptionKey,
                    segmentIndexBuilder
                );
                indexes.add(transactionIndex);
            }
            final var suffix = ObjectKeyFactory.Suffix.INDEXES;
            final ObjectKey key = objectKeyFactory.key(remoteLogSegmentMetadata, suffix);
            try (final var in = new SequenceInputStream(Collections.enumeration(indexes))) {
                final var bytes = uploader.upload(in, key);
                metrics.recordObjectUpload(
                    remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                    suffix,
                    bytes
                );
                customMetadataBuilder.addUploadResult(suffix, bytes);

                log.debug("Uploaded indexes file for {}, size: {}", remoteLogSegmentMetadata, bytes);
            }
        }
        return segmentIndexBuilder.build();
    }

    static int indexSize(final Path indexPath) throws RemoteStorageException {
        try {
            final var size = Files.size(indexPath);
            if (size > Integer.MAX_VALUE) {
                throw new IllegalStateException(
                    "Index at path "
                        + indexPath
                        + " has size larger than Integer.MAX_VALUE");
            }
            return (int) size;
        } catch (final IOException e) {
            throw new RemoteStorageException("Error while getting index path size", e);
        }
    }

    private Optional<CustomMetadata> buildCustomMetadata(final SegmentCustomMetadataBuilder customMetadataBuilder) {
        final var customFields = customMetadataBuilder.build();
        if (!customFields.isEmpty()) {
            final var customMetadataBytes = customMetadataSerde.serialize(customFields);
            return Optional.of(new CustomMetadata(customMetadataBytes));
        } else {
            return Optional.empty();
        }
    }

    boolean requiresCompression(final LogSegmentData logSegmentData) {
        boolean requiresCompression = false;
        if (compressionEnabled) {
            if (compressionHeuristic) {
                try {
                    final File segmentFile = logSegmentData.logSegment().toFile();
                    final boolean alreadyCompressed = SegmentCompressionChecker.check(segmentFile);
                    requiresCompression = !alreadyCompressed;
                } catch (final InvalidRecordBatchException e) {
                    // Log and leave value as false to upload uncompressed.
                    log.warn("Failed to check compression on log segment: {}", logSegmentData.logSegment(), e);
                }
            } else {
                requiresCompression = true;
            }
        }
        return requiresCompression;
    }

    ChunkIndex uploadSegmentLog(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final LogSegmentData logSegmentData,
        final boolean requiresCompression,
        final DataKeyAndAAD maybeEncryptionKey,
        final SegmentCustomMetadataBuilder customMetadataBuilder
    ) throws IOException, StorageBackendException {
        final var objectKey = objectKeyFactory.key(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.LOG);

        try (final var logSegmentInputStream = Files.newInputStream(logSegmentData.logSegment())) {
            final var transformEnum = transformation(logSegmentInputStream, requiresCompression, maybeEncryptionKey);
            final TransformFinisher transformFinisher = TransformFinisher.newBuilder(
                    transformEnum,
                    remoteLogSegmentMetadata.segmentSizeInBytes()
                )
                .withRateLimitingBucket(rateLimitingBucket)
                .withOriginalFilePath(logSegmentData.logSegment())
                .build();

            try (final var sis = transformFinisher.toInputStream()) {
                final var bytes = uploader.upload(sis, objectKey);
                metrics.recordObjectUpload(
                    remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                    ObjectKeyFactory.Suffix.LOG,
                    bytes
                );
                customMetadataBuilder.addUploadResult(ObjectKeyFactory.Suffix.LOG, bytes);

                log.debug("Uploaded segment log for {}, size: {}", remoteLogSegmentMetadata, bytes);
            }
            return transformFinisher.chunkIndex();
        }
    }

    private TransformChunkEnumeration transformation(
        final InputStream logSegmentInputStream,
        final boolean requiresCompression,
        final DataKeyAndAAD maybeEncryptionKey
    ) {
        TransformChunkEnumeration transformEnum = new BaseTransformChunkEnumeration(
            logSegmentInputStream,
            chunkSize
        );
        if (requiresCompression) {
            transformEnum = new CompressionChunkEnumeration(transformEnum);
        }
        if (encryptionEnabled) {
            transformEnum = new EncryptionChunkEnumeration(
                transformEnum,
                () -> aesEncryptionProvider.encryptionCipher(maybeEncryptionKey)
            );
        }
        return transformEnum;
    }

    InputStream transformIndex(final IndexType indexType,
                               final InputStream index,
                               final int size,
                               final DataKeyAndAAD maybeEncryptionKey,
                               final SegmentIndexesV1Builder segmentIndexBuilder) {
        log.debug("Transforming index {} with size {}", indexType, size);
        if (size > 0) {
            TransformChunkEnumeration transformEnum = new BaseTransformChunkEnumeration(index, size);
            if (encryptionEnabled) {
                transformEnum = new EncryptionChunkEnumeration(
                    transformEnum,
                    () -> aesEncryptionProvider.encryptionCipher(maybeEncryptionKey));
            }
            final var transformFinisher = TransformFinisher.newBuilder(transformEnum, size)
                .withChunkingDisabled()
                .withRateLimitingBucket(rateLimitingBucket)
                .build();
            // Getting next element and expecting that it is the only one.
            // No need to get a sequenced input stream
            final var inputStream = transformFinisher.nextElement();
            final var chunkIndex = transformFinisher.chunkIndex();
            // by getting a chunk index, means that the transformation is completed.
            if (chunkIndex == null) {
                throw new IllegalStateException("Chunking disabled when single chunk is expected");
            }
            if (chunkIndex.chunks().size() != 1) {
                // not expected, as next element run once. But for safety
                throw new IllegalStateException("Number of chunks different than 1, single chunk is expected");
            }
            segmentIndexBuilder.add(indexType, chunkIndex.chunks().get(0).range().size());
            return inputStream;
        } else {
            segmentIndexBuilder.add(indexType, 0);
            return InputStream.nullInputStream();
        }
    }

    void uploadManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                        final ChunkIndex chunkIndex,
                        final SegmentIndexesV1 segmentIndexes,
                        final boolean requiresCompression,
                        final DataKeyAndAAD maybeEncryptionKey,
                        final SegmentCustomMetadataBuilder customMetadataBuilder
    ) throws StorageBackendException, IOException {
        final SegmentEncryptionMetadataV1 maybeEncryptionMetadata;
        if (maybeEncryptionKey != null) {
            maybeEncryptionMetadata = new SegmentEncryptionMetadataV1(maybeEncryptionKey);
        } else {
            maybeEncryptionMetadata = null;
        }
        final SegmentManifest segmentManifest = new SegmentManifestV1(
            chunkIndex,
            segmentIndexes,
            requiresCompression,
            maybeEncryptionMetadata,
            remoteLogSegmentMetadata
        );
        final String manifest = mapper.writeValueAsString(segmentManifest);
        final ObjectKey manifestObjectKey =
            objectKeyFactory.key(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.MANIFEST);

        try (final ByteArrayInputStream manifestContent = new ByteArrayInputStream(manifest.getBytes())) {
            final var bytes = uploader.upload(manifestContent, manifestObjectKey);
            metrics.recordObjectUpload(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                ObjectKeyFactory.Suffix.MANIFEST,
                bytes
            );
            customMetadataBuilder.addUploadResult(ObjectKeyFactory.Suffix.MANIFEST, bytes);

            log.debug("Uploaded segment manifest for {}, size: {}", remoteLogSegmentMetadata, bytes);
        }
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition) throws RemoteStorageException {
        return this.fetchLogSegment(
            remoteLogSegmentMetadata,
            startPosition,
            remoteLogSegmentMetadata.segmentSizeInBytes() - 1
        );
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition,
                                       final int endPosition) throws RemoteStorageException {
        final BytesRange range = BytesRange.of(
            startPosition,
            Math.min(endPosition, remoteLogSegmentMetadata.segmentSizeInBytes() - 1)
        );

        try {
            log.trace("Fetching log segment {} with range: {}", remoteLogSegmentMetadata, range);

            metrics.recordSegmentFetch(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                range.size());

            final var segmentManifest = fetchSegmentManifest(remoteLogSegmentMetadata);

            final var suffix = ObjectKeyFactory.Suffix.LOG;
            final var segmentKey = objectKey(remoteLogSegmentMetadata, suffix);
            return new FetchChunkEnumeration(chunkManager, segmentKey, segmentManifest, range, rateLimitingBucket)
                .toInputStream();
        } catch (final KeyNotFoundException | KeyNotFoundRuntimeException e) {
            throw new RemoteResourceNotFoundException(e);
        } catch (final ClosedByInterruptException e) {
            log.debug("Fetching log segment {} with range {} was interrupted", remoteLogSegmentMetadata, range);
            return InputStream.nullInputStream();
        } catch (final RuntimeException | StorageBackendException e) {
            final InputStream is = maybeReturnNullInputStreamIfInterrupted(e, remoteLogSegmentMetadata, range);
            if (is != null) {
                return is;
            } else {
                throw new RemoteStorageException(e);
            }
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    private static InputStream maybeReturnNullInputStreamIfInterrupted(
        final Throwable exception,
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final BytesRange range
    ) {
        if (exception.getCause() instanceof InterruptedException
            || exception.getCause() instanceof ClosedByInterruptException) {
            log.debug("Fetching log segment {} with range {} was interrupted", remoteLogSegmentMetadata, range);
            return InputStream.nullInputStream();
        } else if (exception.getCause() instanceof StorageBackendException) {
            return maybeReturnNullInputStreamIfInterrupted(exception.getCause(), remoteLogSegmentMetadata, range);
        } else {
            return null;
        }
    }

    @Override
    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final IndexType indexType) throws RemoteStorageException {
        try {
            log.trace("Fetching index {} for {}", indexType, remoteLogSegmentMetadata);

            final var segmentManifest = fetchSegmentManifest(remoteLogSegmentMetadata);

            final var key = objectKey(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.INDEXES);
            final var segmentIndex = segmentManifest.segmentIndexes().segmentIndex(indexType);
            if (segmentIndex == null) {
                throw new RemoteResourceNotFoundException("Index " + indexType + " not found on " + key);
            }
            if (segmentIndex.range().isEmpty()) {
                return InputStream.nullInputStream();
            }
            return segmentIndexesCache.get(
                key,
                indexType,
                () -> fetchIndexBytes(key, segmentIndex, segmentManifest)
            );
        } catch (final RemoteResourceNotFoundException e) {
            throw e;
        } catch (final KeyNotFoundException e) {
            throw new RemoteResourceNotFoundException(e);
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    private byte[] fetchIndexBytes(
        final ObjectKey key,
        final SegmentIndex segmentIndex,
        final SegmentManifest segmentManifest
    ) {
        final InputStream in;
        try {
            in = fetcher.fetch(key, segmentIndex.range());
        } catch (final StorageBackendException e) {
            throw new RuntimeException("Error fetching index from remote storage", e);
        }

        DetransformChunkEnumeration detransformEnum = new BaseDetransformChunkEnumeration(in);
        final Optional<SegmentEncryptionMetadata> encryptionMetadata = segmentManifest.encryption();
        if (encryptionMetadata.isPresent()) {
            detransformEnum = new DecryptionChunkEnumeration(
                detransformEnum,
                encryptionMetadata.get().ivSize(),
                encryptedChunk -> aesEncryptionProvider.decryptionCipher(encryptedChunk,
                    encryptionMetadata.get())
            );
        }
        final var detransformFinisher = new DetransformFinisher(detransformEnum);
        try (final var is = detransformFinisher.toInputStream()) {
            return is.readAllBytes();
        } catch (final IOException e) {
            throw new RuntimeException("Error reading de-transformed index bytes", e);
        }
    }

    private ObjectKey objectKey(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                final ObjectKeyFactory.Suffix suffix) {
        final ObjectKey segmentKey;
        if (remoteLogSegmentMetadata.customMetadata().isPresent()) {
            final var customMetadataBytes = remoteLogSegmentMetadata.customMetadata().get();
            final var fields = customMetadataSerde.deserialize(customMetadataBytes.value());
            segmentKey = objectKeyFactory.key(fields, remoteLogSegmentMetadata, suffix);
        } else {
            segmentKey = objectKeyFactory.key(remoteLogSegmentMetadata, suffix);
        }
        return segmentKey;
    }

    private SegmentManifest fetchSegmentManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws StorageBackendException, IOException {
        final ObjectKey manifestKey = objectKeyFactory.key(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.MANIFEST);
        return segmentManifestCache.get(manifestKey);
    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws RemoteStorageException {

        log.info("Deleting log segment data for {}", remoteLogSegmentMetadata);

        metrics.recordSegmentDelete(remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            remoteLogSegmentMetadata.segmentSizeInBytes());

        final long startedMs = time.milliseconds();

        try {
            deleteSegmentObjects(remoteLogSegmentMetadata);
        } catch (final Exception e) {
            metrics.recordSegmentDeleteError(remoteLogSegmentMetadata.remoteLogSegmentId()
                .topicIdPartition().topicPartition());
            throw new RemoteStorageException(e);
        }

        metrics.recordSegmentDeleteTime(
            remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            startedMs, time.milliseconds());

        log.info("Deleting log segment data for completed successfully {}", remoteLogSegmentMetadata);
    }

    @Override
    public void close() {
        metrics.close();
    }
}
