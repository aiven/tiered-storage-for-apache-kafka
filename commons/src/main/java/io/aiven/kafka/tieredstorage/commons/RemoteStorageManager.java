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

package io.aiven.kafka.tieredstorage.commons;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.aiven.kafka.tieredstorage.commons.manifest.SegmentEncryptionMetadataV1;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.commons.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.commons.manifest.serde.DataKeyDeserializer;
import io.aiven.kafka.tieredstorage.commons.manifest.serde.DataKeySerializer;
import io.aiven.kafka.tieredstorage.commons.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.commons.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.commons.security.RsaEncryptionProvider;
import io.aiven.kafka.tieredstorage.commons.storage.BytesRange;
import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;
import io.aiven.kafka.tieredstorage.commons.storage.StorageBackEndException;
import io.aiven.kafka.tieredstorage.commons.transform.BaseTransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.CompressionChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.EncryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.FetchChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.TransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.TransformFinisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.LEADER_EPOCH;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.OFFSET;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TIMESTAMP;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TRANSACTION;

public class RemoteStorageManager implements org.apache.kafka.server.log.remote.storage.RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(RemoteStorageManager.class);

    private final Metrics metrics;
    private final Sensor segmentCopyPerSec;

    private final Executor executor = new ForkJoinPool();

    private ObjectStorageFactory objectStorageFactory;
    private boolean compressionEnabled;
    private boolean compressionHeuristic;
    private boolean encryptionEnabled;
    private int chunkSize;
    private RsaEncryptionProvider rsaEncryptionProvider;
    private AesEncryptionProvider aesEncryptionProvider;
    private ObjectMapper mapper;
    private ChunkManager chunkManager;
    private ObjectKey objectKey;

    private SegmentManifestProvider segmentManifestProvider;

    public RemoteStorageManager() {
        this(Time.SYSTEM);
    }

    // for testing
    RemoteStorageManager(final Time time) {
        final JmxReporter reporter = new JmxReporter();
        metrics = new Metrics(
            new MetricConfig(), List.of(reporter), time,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage")
        );
        segmentCopyPerSec = metrics.sensor("segment-copy");
        segmentCopyPerSec.add(
            metrics.metricName("segment-copy-rate", "remote-storage-manager-metrics"), new Rate());
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs must not be null");
        final RemoteStorageManagerConfig config = new RemoteStorageManagerConfig(configs);
        objectStorageFactory = config.objectStorageFactory();
        objectKey = new ObjectKey(config.keyPrefix());
        encryptionEnabled = config.encryptionEnabled();
        if (encryptionEnabled) {
            rsaEncryptionProvider = RsaEncryptionProvider.of(
                config.encryptionPublicKeyFile(),
                config.encryptionPrivateKeyFile()
            );
            aesEncryptionProvider = new AesEncryptionProvider();
        }
        chunkManager = new ChunkManager(
            objectStorageFactory.fileFetcher(),
            objectKey,
            aesEncryptionProvider,
            config.chunkCache()
        );

        chunkSize = config.chunkSize();
        compressionEnabled = config.compressionEnabled();
        compressionHeuristic = config.compressionHeuristicEnabled();

        mapper = getObjectMapper();

        segmentManifestProvider = new SegmentManifestProvider(
            objectKey,
            config.segmentManifestCacheSize(),
            config.segmentManifestCacheRetention(),
            objectStorageFactory.fileFetcher(),
            mapper,
            executor);
    }

    private ObjectMapper getObjectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        if (encryptionEnabled) {
            final SimpleModule simpleModule = new SimpleModule();
            simpleModule.addSerializer(SecretKey.class, new DataKeySerializer(rsaEncryptionProvider::encryptDataKey));
            simpleModule.addDeserializer(SecretKey.class, new DataKeyDeserializer(
                b -> new SecretKeySpec(rsaEncryptionProvider.decryptDataKey(b), "AES")));
            objectMapper.registerModule(simpleModule);
        }
        return objectMapper;
    }

    @Override
    public void copyLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   final LogSegmentData logSegmentData) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentId must not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData must not be null");

        segmentCopyPerSec.record();

        try {
            TransformChunkEnumeration transformEnum = new BaseTransformChunkEnumeration(
                Files.newInputStream(logSegmentData.logSegment()), chunkSize);
            SegmentEncryptionMetadataV1 encryptionMetadata = null;
            final boolean requiresCompression = requiresCompression(logSegmentData);
            if (requiresCompression) {
                transformEnum = new CompressionChunkEnumeration(transformEnum);
            }
            if (encryptionEnabled) {
                final DataKeyAndAAD dataKeyAndAAD = aesEncryptionProvider.createDataKeyAndAAD();
                transformEnum = new EncryptionChunkEnumeration(
                    transformEnum,
                    () -> aesEncryptionProvider.encryptionCipher(dataKeyAndAAD));
                encryptionMetadata = new SegmentEncryptionMetadataV1(dataKeyAndAAD.dataKey, dataKeyAndAAD.aad);
            }
            final TransformFinisher transformFinisher =
                new TransformFinisher(transformEnum, remoteLogSegmentMetadata.segmentSizeInBytes());
            uploadSegmentLog(remoteLogSegmentMetadata, transformFinisher);

            final ChunkIndex chunkIndex = transformFinisher.chunkIndex();
            final SegmentManifest segmentManifest =
                new SegmentManifestV1(chunkIndex, requiresCompression, encryptionMetadata);
            uploadManifest(remoteLogSegmentMetadata, segmentManifest);

            final InputStream offsetIndex = Files.newInputStream(logSegmentData.offsetIndex());
            uploadIndexFile(remoteLogSegmentMetadata, offsetIndex, OFFSET);
            final InputStream timeIndex = Files.newInputStream(logSegmentData.timeIndex());
            uploadIndexFile(remoteLogSegmentMetadata, timeIndex, TIMESTAMP);
            final InputStream producerSnapshotIndex = Files.newInputStream(logSegmentData.producerSnapshotIndex());
            uploadIndexFile(remoteLogSegmentMetadata, producerSnapshotIndex, PRODUCER_SNAPSHOT);
            if (logSegmentData.transactionIndex().isPresent()) {
                final InputStream transactionIndex = Files.newInputStream(logSegmentData.transactionIndex().get());
                uploadIndexFile(remoteLogSegmentMetadata, transactionIndex, TRANSACTION);
            }
            final ByteBufferInputStream leaderEpoch = new ByteBufferInputStream(logSegmentData.leaderEpochIndex());
            uploadIndexFile(remoteLogSegmentMetadata, leaderEpoch, LEADER_EPOCH);
        } catch (final StorageBackEndException | IOException e) {
            throw new RemoteStorageException(e);
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

    private void uploadSegmentLog(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final TransformFinisher transformFinisher)
        throws IOException, StorageBackEndException {
        final String fileKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.LOG);
        try (final var sis = new SequenceInputStream(transformFinisher)) {
            objectStorageFactory.fileUploader().upload(sis, fileKey);
        }
    }

    private void uploadIndexFile(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 final InputStream index,
                                 final IndexType indexType)
        throws StorageBackEndException, IOException {
        final String key = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.fromIndexType(indexType));
        try (index) {
            objectStorageFactory.fileUploader().upload(index, key);
        }
    }

    private void uploadManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                final SegmentManifest segmentManifest)
        throws StorageBackEndException, IOException {
        final String manifest = mapper.writeValueAsString(segmentManifest);
        final String manifestFileKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST);

        try (final ByteArrayInputStream manifestContent = new ByteArrayInputStream(manifest.getBytes())) {
            objectStorageFactory.fileUploader().upload(manifestContent, manifestFileKey);
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
        try {
            final SegmentManifest segmentManifest = segmentManifestProvider.get(remoteLogSegmentMetadata);

            final BytesRange range = BytesRange.of(
                startPosition,
                Math.min(endPosition, remoteLogSegmentMetadata.segmentSizeInBytes() - 1)
            );
            final FetchChunkEnumeration fetchChunkEnumeration = new FetchChunkEnumeration(
                chunkManager,
                remoteLogSegmentMetadata,
                segmentManifest,
                range
            );
            return new SequenceInputStream(fetchChunkEnumeration);
        } catch (final StorageBackEndException | IOException e) {
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final IndexType indexType) throws RemoteStorageException {
        try {
            return objectStorageFactory.fileFetcher()
                .fetch(objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.fromIndexType(indexType)));
        } catch (final StorageBackEndException e) {
            // TODO: should be aligned with upstream implementation
            if (indexType == TRANSACTION) {
                return null;
            } else {
                throw new RemoteStorageException(e);
            }
        }

    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws RemoteStorageException {
        try {
            for (final ObjectKey.Suffix suffix : ObjectKey.Suffix.values()) {
                objectStorageFactory.fileDeleter()
                    .delete(objectKey.key(remoteLogSegmentMetadata, suffix));
            }
        } catch (final StorageBackEndException e) {
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public void close() {
        try {
            metrics.close();
        } catch (final Exception e) {
            log.warn("Error while closing metrics", e);
        }
    }
}