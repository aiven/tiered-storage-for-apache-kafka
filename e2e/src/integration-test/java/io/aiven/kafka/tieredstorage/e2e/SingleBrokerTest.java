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

package io.aiven.kafka.tieredstorage.e2e;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import io.aiven.kafka.tieredstorage.e2e.internal.RemoteLogMetadataTracker;
import io.aiven.kafka.tieredstorage.e2e.internal.RemoteSegment;

import com.github.dockerjava.api.model.Ulimit;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static java.util.stream.Collectors.groupingBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

/**
 * Tiered Storage end-to-end test on a Single Broker:
 * This test includes the following stages:
 * <ul>
 *     <li>{@link SingleBrokerTest#remoteCopy()}: ensure that Kafka TS is moving files to remote storage</li>
 *     <li>{@link SingleBrokerTest#remoteRead()}: ensure fetch request retrieve data from remote segments</li>
 *     <li>{@link SingleBrokerTest#remoteManualDelete()}: delete records manually and ensure data is removed
 *     from remote storage</li>
 *     <li>{@link SingleBrokerTest#remoteCleanupDueToRetention()}: wait for retention by size to remove data
 *     from remote storage</li>
 *     <li>{@link SingleBrokerTest#topicDelete()}: (unsupported) delete topic and validate data is removed
 *     from remote storage</li>
 * </ul>
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(SingleBrokerTest.FailFastExtension.class)
abstract class SingleBrokerTest {
    static final Logger LOG = LoggerFactory.getLogger(SingleBrokerTest.class);

    static final String KAFKA_DATA_SUBDIR_HOST = "data";
    static final String KAFKA_DATA_DIR_CONTAINER = "/var/lib/kafka/data";

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final TopicPartition TP_0_0 = new TopicPartition(TOPIC_0, 0);
    static final TopicPartition TP_0_1 = new TopicPartition(TOPIC_0, 1);
    static final TopicPartition TP_1_0 = new TopicPartition(TOPIC_1, 0);
    static final List<TopicPartition> USER_TOPIC_PARTITIONS = List.of(TP_0_0, TP_0_1, TP_1_0);

    static final int CHUNK_SIZE = 1024;  // TODO something more reasonable?
    // segment sizes not aligned to chunk size
    static final int SEGMENT_SIZE_T0 = 256 * CHUNK_SIZE + CHUNK_SIZE / 2;
    static final int SEGMENT_SIZE_T1 = 123 * CHUNK_SIZE + 123;

    static final Duration TOTAL_RETENTION = Duration.ofHours(1);
    static final Duration LOCAL_RETENTION = Duration.ofSeconds(5);

    // record value size range to be generated
    static final int RECORD_VALUE_SIZE_MIN = CHUNK_SIZE / 4 - 3;
    static final int RECORD_VALUE_SIZE_MAX = CHUNK_SIZE * 2 + 5;

    static final long RECORDS_TO_PRODUCE = 10_000;

    static final Network NETWORK = Network.newNetwork();
    static final String SOCKS5_NETWORK_ALIAS = "socks5-proxy";
    static final int SOCKS5_PORT = 1080;
    static final String SOCKS5_USER = "user";
    static final String SOCKS5_PASSWORD = "password";

    @Container
    static final GenericContainer<?> SOCKS5_PROXY =
        new GenericContainer<>(DockerImageName.parse("ananclub/ss5"))
            .withEnv(Map.of(
                "LISTEN", "0.0.0.0:" + SOCKS5_PORT,
                "USER", SOCKS5_USER,
                "PASSWORD", SOCKS5_PASSWORD
            ))
            .withExposedPorts(SOCKS5_PORT)
            .withNetwork(NETWORK)
            .withNetworkAliases(SOCKS5_NETWORK_ALIAS);

    static Path baseDir;
    // Can't use @TempDir, because it's initialized too late.
    static Path localDataDir;

    static KafkaContainer kafka;

    static AdminClient adminClient;

    static RemoteLogMetadataTracker remoteLogMetadataTracker;

    static TopicIdPartition t0p0;
    static TopicIdPartition t0p1;
    static TopicIdPartition t1p0;
    static List<TopicIdPartition> userTopicIdPartitions;

    public static void setupKafka(final Consumer<KafkaContainer> tsPluginSetup) throws Exception {

        try {
            baseDir = Files.createTempDirectory("junit");
            localDataDir = baseDir.resolve(KAFKA_DATA_SUBDIR_HOST);
            localDataDir.toFile().mkdirs();
            localDataDir.toFile().setWritable(true, false);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        kafka = new KafkaContainer(DockerImageName.parse("aivenoy/kafka-with-ts-plugin")
            .asCompatibleSubstituteFor("confluentinc/cp-kafka")
        )
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .withEnv("KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS", "10000")
            // enable tiered storage
            .withEnv("KAFKA_REMOTE_LOG_STORAGE_SYSTEM_ENABLE", "true")
            .withEnv("KAFKA_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS", "5000")
            // remote metadata manager
            .withEnv("KAFKA_REMOTE_LOG_METADATA_MANAGER_CLASS_NAME",
                "org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager")
            .withEnv("KAFKA_REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME", "BROKER")
            .withEnv("KAFKA_RLMM_CONFIG_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR", "1")
            // remote storage manager
            .withEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_NAME",
                "io.aiven.kafka.tieredstorage.RemoteStorageManager")
            .withEnv("KAFKA_RSM_CONFIG_CHUNK_SIZE", Integer.toString(CHUNK_SIZE))
            .withEnv("KAFKA_RSM_CONFIG_CUSTOM_METADATA_FIELDS_INCLUDE", "REMOTE_SIZE")
            // upload rate at smallest rate
            .withEnv("KAFKA_RSM_CONFIG_UPLOAD_RATE_LIMIT_BYTES_PER_SECOND", Integer.toString(1024 * 1024))
            // chunk caching
            .withEnv("KAFKA_RSM_CONFIG_FETCH_CHUNK_CACHE_CLASS",
                "io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache")
            .withEnv("KAFKA_RSM_CONFIG_FETCH_CHUNK_CACHE_SIZE", "-1")
            .withEnv("KAFKA_RSM_CONFIG_FETCH_CHUNK_CACHE_PATH", "/home/appuser/kafka-tiered-storage-cache")
            // other tweaks
            .withEnv("KAFKA_OPTS", "") // disable JMX exporter
            .withEnv("KAFKA_LOG4J_LOGGERS", "io.aiven.kafka.tieredstorage=DEBUG,"
                + "org.apache.kafka.server.log.remote.metadata.storage=DEBUG,"
                + "state.change.logger=INFO")
            .withCreateContainerCmdModifier(
                cmd -> cmd.getHostConfig().withUlimits(List.of(new Ulimit("nofile", 30_000L, 30_000L))))
            .withFileSystemBind(localDataDir.toString(), KAFKA_DATA_DIR_CONTAINER)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka")))
            .withNetwork(NETWORK);

        tsPluginSetup.accept(kafka);

        kafka.start();

        adminClient = AdminClient.create(Map.of(
            "bootstrap.servers", kafka.getBootstrapServers()
        ));

        remoteLogMetadataTracker = new RemoteLogMetadataTracker(kafka.getBootstrapServers());

        createTopics();
    }

    public static void stopKafka() {
        if (adminClient != null) {
            adminClient.close();
        }

        kafka.stop();
    }

    public static void cleanupStorage() {
        if (baseDir != null) {
            // TODO: failing silently atm. As Delete Topics test is disabled, and topic directories are not removed
            //   they cannot be deleted as they have different user/group. see #366
            FileUtils.deleteQuietly(baseDir.toFile());
        }
    }

    private static void createTopics() throws Exception {
        final NewTopic newTopic0 = new NewTopic(TOPIC_0, 2, (short) 1)
            .configs(Map.of(
                "remote.storage.enable", "true",
                "segment.bytes", Integer.toString(SEGMENT_SIZE_T0),
                "retention.ms", Long.toString(TOTAL_RETENTION.toMillis()),
                "local.retention.ms", Long.toString(LOCAL_RETENTION.toMillis())
            ));
        final NewTopic newTopic1 = new NewTopic(TOPIC_1, 1, (short) 1)
            .configs(Map.of(
                "remote.storage.enable", "true",
                "segment.bytes", Integer.toString(SEGMENT_SIZE_T1),
                "retention.ms", Long.toString(TOTAL_RETENTION.toMillis()),
                "local.retention.ms", Long.toString(LOCAL_RETENTION.toMillis())
            ));
        final var topicsToCreate = List.of(newTopic0, newTopic1);
        adminClient.createTopics(topicsToCreate)
            .all()
            .get(30, TimeUnit.SECONDS);

        adminClient.describeTopics(List.of(TOPIC_0, TOPIC_1))
            .allTopicNames()
            .get(30, TimeUnit.SECONDS)
            .values()
            .forEach(td -> {
                if (td.name().equals(TOPIC_0)) {
                    t0p0 = new TopicIdPartition(td.topicId(), TP_0_0);
                    t0p1 = new TopicIdPartition(td.topicId(), TP_0_1);
                } else if (td.name().equals(TOPIC_1)) {
                    t1p0 = new TopicIdPartition(td.topicId(), TP_1_0);
                } else {
                    fail("Unknown topic %s", td);
                }
            });
        userTopicIdPartitions = List.of(t0p0, t0p1, t1p0);

        LOG.info("Topics {} created successfully", topicsToCreate);
    }

    @Test
    @Order(1)
    void remoteCopy() throws Exception {
        fillTopics();

        // Prepare tracker for a minute to wait for all segments to be copied
        remoteLogMetadataTracker.poll(userTopicIdPartitions, Duration.ofMinutes(1));

        // Check remote segments are present in storage back-end
        final var allRemoteSegments = remoteLogMetadataTracker.remoteSegments();

        for (final Map.Entry<TopicIdPartition, List<RemoteSegment>> entry : allRemoteSegments.entrySet()) {
            final Map<String, List<String>> segmentFiles = remotePartitionFiles(entry.getKey()).stream()
                .collect(groupingBy(SingleBrokerTest::extractSegmentId));

            for (final RemoteSegment remoteLogSegment : entry.getValue()) {
                final String key = remoteLogSegment.remoteLogSegmentId().id().toString();
                assertThat(segmentFiles).containsKey(key);
                assertThat(segmentFiles.get(key).stream()
                    .map(SingleBrokerTest::extractSuffix))
                    .containsExactlyInAnyOrder("indexes", "log", "rsm-manifest");
            }
        }

        // Ensure local segments available in remote storage are fully deleted for following test to read from remote
        for (final TopicIdPartition tp : userTopicIdPartitions) {
            await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> localLogFiles(tp.topicPartition()).size() == 1);

            final var localLogs = localLogFiles(tp.topicPartition());
            LOG.info("Local logs for {} [{}]: \n{}", tp, localLogs.size(), localLogs);
            final var remoteSegments = allRemoteSegments.get(tp);
            LOG.info("Remote logs for {} [{}]: \n{}", tp, remoteSegments.size(), remoteSegments);
        }
    }

    private void fillTopics() throws Exception {
        try (final var producer = new KafkaProducer<>(Map.of(
            "bootstrap.servers", kafka.getBootstrapServers(),
            "linger.ms", Long.toString(Duration.ofSeconds(1).toMillis()),
            "batch.size", Integer.toString(1_000_000)
        ), new ByteArraySerializer(), new ByteArraySerializer())) {

            for (final TopicPartition topicPartition : USER_TOPIC_PARTITIONS) {
                long offset = 0;
                while (offset < RECORDS_TO_PRODUCE) {
                    final int batchSize = batchSize(offset);
                    final ArrayList<Future<RecordMetadata>> sendFutures = new ArrayList<>(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        final var record = createProducerRecord(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            offset++
                        );
                        final Future<RecordMetadata> sendFuture = producer.send(record);
                        sendFutures.add(sendFuture);
                    }
                    producer.flush();
                    for (final Future<RecordMetadata> f : sendFutures) {
                        f.get(30, TimeUnit.SECONDS);
                    }
                }

                LOG.info("{} records produced to {}", RECORDS_TO_PRODUCE, topicPartition);
            }
        }
    }

    private static List<File> localLogFiles(final TopicPartition tp) {
        final Path dir = localDataDir.resolve(String.format("%s-%d", tp.topic(), tp.partition()));
        try (final var paths = Files.list(dir)) {
            return paths
                .map(Path::toFile)
                .sorted(Comparator.comparing(File::getName))
                .filter(f -> f.getName().endsWith(".log"))
                .collect(Collectors.toList());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Order(2)
    void remoteRead() {
        validateReadWithinBatch();
        validateReadOverBatchBorders();
    }

    private void validateReadWithinBatch() {
        // Consumer configs to read messages on single batch
        try (final var consumer = new KafkaConsumer<>(
            Map.of(
                "bootstrap.servers", kafka.getBootstrapServers(),
                "fetch.max.bytes", "1"
            ),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        )) {

            // Check the beginning and end offsets.
            final Map<TopicPartition, Long> startOffsets = consumer.beginningOffsets(USER_TOPIC_PARTITIONS);
            assertThat(startOffsets).containsAllEntriesOf(
                Map.of(
                    TP_0_0, 0L,
                    TP_0_1, 0L,
                    TP_1_0, 0L));
            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(USER_TOPIC_PARTITIONS);
            assertThat(endOffsets).containsAllEntriesOf(
                Map.of(
                    TP_0_0, RECORDS_TO_PRODUCE + 1,
                    TP_0_1, RECORDS_TO_PRODUCE + 1,
                    TP_1_0, RECORDS_TO_PRODUCE + 1));

            LOG.info("start and end offsets are as expected");

            // TODO check for EARLIEST_LOCAL_TIMESTAMP when available in client

            // Read by record.
            LOG.info("Starting validation per record");

            for (final TopicPartition tp : USER_TOPIC_PARTITIONS) {
                consumer.assign(List.of(tp));
                LOG.info("Checking records from partition {}", tp);
                for (long offset = 0; offset < RECORDS_TO_PRODUCE; offset++) {
                    consumer.seek(tp, offset);
                    final var record = consumer.poll(Duration.ofSeconds(5)).records(tp).get(0);
                    final var expectedRecord = createProducerRecord(tp.topic(), tp.partition(), offset);
                    checkRecord(record, offset, expectedRecord);
                    if (offset % 500 == 0) {
                        LOG.info("{} of {} records checked", offset, RECORDS_TO_PRODUCE);
                    }
                }
                LOG.info("Records from partition {} checked", tp);
            }

            LOG.info("Validation per record completed");

            // Read by batches.
            LOG.info("Starting validation per batch");

            for (final TopicPartition tp : USER_TOPIC_PARTITIONS) {
                consumer.assign(List.of(tp));
                long offset = 0;
                while (offset < RECORDS_TO_PRODUCE) {
                    consumer.seek(tp, offset);
                    final var records = consumer.poll(Duration.ofSeconds(1)).records(tp);
                    assertThat(records).hasSize(batchSize(offset));
                    for (final var record : records) {
                        final var expectedRecord = createProducerRecord(tp.topic(), tp.partition(), offset);
                        checkRecord(record, offset, expectedRecord);
                        offset += 1;
                    }
                }
            }

            LOG.info("Validation per batch completed");
        }
    }

    private void validateReadOverBatchBorders() {
        // Read over batch borders.
        LOG.info("Starting validation over batch borders");

        final ArrayList<Long> batchBorders = new ArrayList<>();
        // Skip the first and last batches because we can't read "over" their left and right border.
        for (long offset = 1; offset < RECORDS_TO_PRODUCE - 1; ) {
            batchBorders.add(offset);
            final int batchSize = batchSize(offset);
            offset += batchSize;
        }

        try (final var consumer = new KafkaConsumer<>(
            Map.of(
                "bootstrap.servers", kafka.getBootstrapServers(),
                "fetch.max.bytes", Integer.toString(RECORD_VALUE_SIZE_MAX * 50)
            ),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        )) {
            for (final TopicPartition tp : USER_TOPIC_PARTITIONS) {
                consumer.assign(List.of(tp));
                for (final long batchBorder : batchBorders) {
                    final var offset = batchBorder - 1;
                    consumer.seek(tp, offset);
                    List<ConsumerRecord<byte[], byte[]>> records = consumer.poll(Duration.ofSeconds(1)).records(tp);
                    checkRecord(
                        records.get(0),
                        offset,
                        createProducerRecord(tp.topic(), tp.partition(), offset));
                    if (records.size() > 1) {
                        checkRecord(
                            records.get(1),
                            batchBorder,
                            createProducerRecord(tp.topic(), tp.partition(), batchBorder));
                    } else {
                        // It's possible when the batch is the last in the segment:
                        // the broker won't return records over a segment border.
                        records = consumer.poll(Duration.ofSeconds(1)).records(tp);
                        checkRecord(
                            records.get(0),
                            batchBorder,
                            createProducerRecord(tp.topic(), tp.partition(), batchBorder));
                    }
                }
            }
        }

        LOG.info("Validation over batch borders completed");
    }

    @Test
    @Order(3)
    void remoteManualDelete() throws Exception {
        // Reduce records in half by manual deletion affecting only topic t1
        final long newStartOffset = RECORDS_TO_PRODUCE / 2;

        // Collect segments to be deleted for T1
        final List<RemoteSegment> segmentsToBeDeleted = remoteLogMetadataTracker.remoteSegments()
            .get(t1p0)
            .stream()
            .filter(rs -> rs.endOffset() < newStartOffset)
            .collect(Collectors.toList());

        final TopicPartition t1p0 = TP_1_0;

        adminClient.deleteRecords(Map.of(t1p0, RecordsToDelete.beforeOffset(newStartOffset)))
            .all().get(5, TimeUnit.SECONDS);

        remoteLogMetadataTracker.waitUntilSegmentsAreDeleted(segmentsToBeDeleted);

        // Check reduced records available in partition
        try (final var consumer = new KafkaConsumer<>(
            Map.of(
                "bootstrap.servers", kafka.getBootstrapServers()
            ),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        )) {

            // Check the beginning and end offsets.
            final Map<TopicPartition, Long> startOffsets = consumer.beginningOffsets(List.of(t1p0));
            assertThat(startOffsets).containsEntry(t1p0, newStartOffset);
            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(List.of(t1p0));
            assertThat(endOffsets).containsEntry(t1p0, RECORDS_TO_PRODUCE + 1);
            // TODO check for EARLIEST_LOCAL_TIMESTAMP when available in client

            // TODO check segments deleted on the remote

            // Check what we can now consume.
            consumer.assign(List.of(t1p0));
            consumer.seekToBeginning(List.of(t1p0));
            final List<ConsumerRecord<byte[], byte[]>> records = consumer.poll(Duration.ofSeconds(5)).records(t1p0);
            assertThat(records).isNotEmpty();
            final ConsumerRecord<byte[], byte[]> record = records.get(0);
            assertThat(record.offset()).isEqualTo(newStartOffset);
        }
    }

    @Test
    @Order(4)
    void remoteCleanupDueToRetention() throws Exception {
        LOG.info("Forcing cleanup by setting bytes retention to 1 ms");

        final var alterConfigs = List.of(
            new AlterConfigOp(
                new ConfigEntry("retention.ms", "1"),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(
                new ConfigEntry("local.retention.ms", "1"),
                AlterConfigOp.OpType.SET)
        );
        final Map<ConfigResource, Collection<AlterConfigOp>> configs = Map.of(
            new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_0),
            alterConfigs
        );
        adminClient.incrementalAlterConfigs(configs)
            .all()
            .get(5, TimeUnit.SECONDS);

        LOG.info("Starting cleanup validation");

        try (
            // start a consumer to read from earliest
            final var consumer = new KafkaConsumer<>(
                Map.of("bootstrap.servers", kafka.getBootstrapServers()),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer())
        ) {
            // Get earliest offset available locally
            final long nextOffset = localLogFiles(TP_0_0).stream()
                .mapToLong(f -> Long.parseLong(f.getName().replace(".log", "")))
                .max()
                .getAsLong();
            // and wait til expected earliest offset is in place
            await()
                .pollInterval(Duration.ofSeconds(5))
                .atMost(Duration.ofSeconds(30))
                .until(() -> {
                    final var beginningOffset = consumer.beginningOffsets(List.of(TP_0_0)).get(TP_0_0);
                    LOG.info("Beginning offset found {}, expecting {}", beginningOffset, nextOffset);
                    return beginningOffset.equals(nextOffset);
                });

            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(List.of(TP_0_0));
            assertThat(endOffsets).containsEntry(TP_0_0, RECORDS_TO_PRODUCE + 1);

            // TODO check for EARLIEST_LOCAL_TIMESTAMP when available in client

            consumer.assign(List.of(TP_0_0));
            consumer.seekToBeginning(List.of(TP_0_0));

            // with current retention logic, active segment is rotated, and no data is available locally.
            final List<ConsumerRecord<byte[], byte[]>> records = consumer.poll(Duration.ofSeconds(1)).records(TP_0_0);
            assertThat(records).isEmpty();

            // Collect all remote segments, as after changing retention, all should be deleted.
            final var remoteSegmentsBefore = remoteLogMetadataTracker.remoteSegments();
            final var segmentsToBeDeleted = Stream.concat(
                remoteSegmentsBefore.get(t0p0).stream(),
                remoteSegmentsBefore.get(t0p1).stream()
            ).collect(Collectors.toList());
            remoteLogMetadataTracker.waitUntilSegmentsAreDeleted(segmentsToBeDeleted);
            await()
                .between(Duration.ofSeconds(1), Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> assertNoTopicDataOnTierStorage(t0p0.topic(), t0p0.topicId()));
        }

        LOG.info("Cleanup validation completed");
    }

    @Test
    @Order(5)
    void topicDelete() throws Exception {
        LOG.info("Starting topic delete test");

        final var remoteSegmentsBefore = remoteLogMetadataTracker.remoteSegments();
        final var segmentsToBeDeleted = remoteSegmentsBefore.get(t1p0);

        adminClient.deleteTopics(List.of(TP_1_0.topic()))
            .all().get(30, TimeUnit.SECONDS);

        remoteLogMetadataTracker.waitUntilSegmentsAreDeleted(segmentsToBeDeleted);
        await()
            .between(Duration.ofSeconds(1), Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(1))
            .until(() -> assertNoTopicDataOnTierStorage(t1p0.topic(), t1p0.topicId()));

        LOG.info("Topic delete test completed");
    }

    private static String extractSegmentId(final String fileName) {
        return fileName.substring(21, fileName.lastIndexOf('.'));
    }

    private static String extractSuffix(final String fileName) {
        return fileName.substring(fileName.lastIndexOf('.') + 1);
    }

    /**
     * Variable batch size based on the offset received
     */
    private static int batchSize(final long offset) {
        return (int) offset % 10 + 1;
    }

    private static ProducerRecord<byte[], byte[]> createProducerRecord(final String topic,
                                                                       final int partition,
                                                                       final long offset) {
        final int seed = (int) ((offset + partition) % 10);

        final int keySize = seed * 2 + 1;
        final var key = ByteBuffer.allocate(keySize);
        final var keyPattern = (topic + "-" + partition + "-" + offset).getBytes();
        while (key.remaining() >= keyPattern.length) {
            key.put(keyPattern);
        }
        key.put(keyPattern, 0, key.remaining());
        assertThat(key.hasRemaining()).isFalse();

        final int valueSize = RECORD_VALUE_SIZE_MIN + (RECORD_VALUE_SIZE_MAX - RECORD_VALUE_SIZE_MIN) / 10 * seed;
        final var value = ByteBuffer.allocate(valueSize);

        return new ProducerRecord<>(topic, partition, key.array(), value.array());
    }

    private void checkRecord(final ConsumerRecord<byte[], byte[]> actual,
                             final long offset,
                             final ProducerRecord<byte[], byte[]> expected) {
        assertThat(actual.offset()).isEqualTo(offset);
        assertThat(actual.key()).isEqualTo(expected.key());
        assertThat(actual.value()).isEqualTo(expected.value());
    }

    abstract List<String> remotePartitionFiles(final TopicIdPartition topicIdPartition);

    abstract boolean assertNoTopicDataOnTierStorage(final String topicName, final Uuid topicId);

    /**
     * Flag when a step has failed so next steps fail-fast.
     *
     * <p>Needed to allow running {@code SingleBrokerTest#stopKafka} after all tests.
     */
    public static class FailFastExtension implements TestWatcher, ExecutionCondition {
        private boolean failed;

        @Override
        public void testFailed(final ExtensionContext context, final Throwable cause) {
            LOG.error("Test failed: " + context.getDisplayName());
            failed = true;
        }

        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext extensionContext) {
            if (failed) {
                return ConditionEvaluationResult.disabled("Already failed");
            } else {
                return ConditionEvaluationResult.enabled("Continue testing");
            }
        }
    }
}
