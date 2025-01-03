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

package io.aiven.kafka.tieredstorage.e2e.internal;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

public class RemoteLogMetadataTracker {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteLogMetadataTracker.class);
    private static final String REMOTE_LOG_METADATA_TOPIC = "__remote_log_metadata";

    private final KafkaConsumer<byte[], RemoteLogMetadata> consumer;
    private List<TopicPartition> metadataPartitions;

    private final Map<TopicIdPartition, Set<RemoteSegment>> remoteSegments = new HashMap<>();
    private final Map<RemoteLogSegmentId, RemoteLogSegmentState> remoteSegmentStates = new HashMap<>();

    public RemoteLogMetadataTracker(final String bootstrapServers) {
        consumer = new KafkaConsumer<>(Map.of(
            "bootstrap.servers", bootstrapServers,
            "auto.offset.reset", "earliest"
        ), new ByteArrayDeserializer(), new RemoteLogMetadataDeserializer());
    }

    public Map<TopicIdPartition, List<RemoteSegment>> remoteSegments() {
        final Map<TopicIdPartition, List<RemoteSegment>> result = new HashMap<>();
        for (final Map.Entry<TopicIdPartition, Set<RemoteSegment>> entry : remoteSegments.entrySet()) {
            final List<RemoteSegment> list = entry.getValue().stream()
                .sorted(Comparator.comparing(RemoteSegment::startOffset))
                .collect(Collectors.toList());
            result.put(entry.getKey(), list);
        }
        return result;
    }

    /**
     * Initializes the tracker by reading metadata topic for {@code timeout} to wait for segments to be copied
     * be aware that this needs to be higher than local retention and RLM task interval ms
     * at least a few times higher to give enough time for all segments to be placed remotely.
     *
     * <p>It expects at least one record to be present in __remote_log_metadata
     * and that all remote segments are in {@code COPY_SEGMENT_FINISHED} state.
     */
    public void poll(final List<TopicIdPartition> expectedPartitions, final Duration timeout) {
        metadataPartitions = consumer.partitionsFor(REMOTE_LOG_METADATA_TOPIC)
            .stream()
            .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
            .collect(Collectors.toList());

        // waits for metadata topic to have received some events
        await().atMost(timeout)
            .pollInterval(Duration.ofSeconds(1))
            .until(() ->
                consumer.endOffsets(metadataPartitions)
                    .values()
                    .stream()
                    .mapToLong(Long::longValue)
                    .sum() > 0);

        consumer.assign(metadataPartitions);
        consumer.seekToBeginning(metadataPartitions);

        LOG.info("Initializing remote segments");

        final var startAt = System.currentTimeMillis();

        final var metadataRecords = new LinkedHashMap<Map.Entry<TopicPartition, Long>, RemoteLogMetadata>();
        boolean isReady = false;
        // supply segment states where copy has not finished
        final Supplier<Map<RemoteLogSegmentId, RemoteLogSegmentState>> segmentsStillCopying = () ->
            remoteSegmentStates.entrySet()
                .stream()
                .filter(s -> s.getValue() != RemoteLogSegmentState.COPY_SEGMENT_FINISHED)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        while (!isReady) {
            consumer.poll(Duration.ofSeconds(5)).forEach(r -> {
                final RemoteLogMetadata metadata = r.value();
                assertThat(metadata)
                    .withFailMessage("Unexpected metadata type: %s", metadata)
                    .isInstanceOfAny(RemoteLogSegmentMetadata.class, RemoteLogSegmentMetadataUpdate.class);

                remoteSegments.putIfAbsent(metadata.topicIdPartition(), new HashSet<>());
                metadataRecords.put(Map.entry(new TopicPartition(r.topic(), r.partition()), r.offset()), metadata);
                if (metadata instanceof RemoteLogSegmentMetadata) {
                    final var segmentMetadata = (RemoteLogSegmentMetadata) metadata;
                    remoteSegmentStates.put(segmentMetadata.remoteLogSegmentId(), segmentMetadata.state());
                    remoteSegments.get(metadata.topicIdPartition()).add(
                        new RemoteSegment(
                            segmentMetadata.remoteLogSegmentId(),
                            segmentMetadata.startOffset(),
                            segmentMetadata.endOffset()
                        ));
                } else if (metadata instanceof RemoteLogSegmentMetadataUpdate) {
                    final var update = (RemoteLogSegmentMetadataUpdate) metadata;
                    remoteSegmentStates.put(update.remoteLogSegmentId(), update.state());

                    // Sanity check: if we see an update, the original record should be already taken into account.
                    assertThat(remoteSegments.get(metadata.topicIdPartition()))
                        .map(RemoteSegment::remoteLogSegmentId)
                        .contains(update.remoteLogSegmentId());
                }
            });

            isReady = segmentsStillCopying.get().isEmpty() // copies have not finished
                && expectedPartitions.stream().allMatch(remoteSegments::containsKey); // AND not all segments present

            // check for timeout
            final var running = System.currentTimeMillis() - startAt;
            if (running > timeout.toMillis()) {
                LOG.warn("Timeout waiting for segments copy finished events to arrive after {} running", running);
                break;
            }
        }

        if (!isReady) { // if finished because of timeout
            final var notCopied = segmentsStillCopying.get();
            fail("Fail to receive copy metadata records for %s out of %s segments."
                    + "%nSegments missing: %n%s"
                    + "%nMetadata events received: %n%s",
                notCopied.size(), remoteSegments.size(),
                notCopied.entrySet().stream()
                    .map(e -> e.getKey().toString()
                        + " => "
                        + e.getValue())
                    .collect(Collectors.joining("\n")),
                metadataRecords.entrySet().stream()
                    .map(e -> e.getKey().getKey()
                        + "-" + e.getKey().getValue() + ":" + e.getValue() + " => "
                        + e.getValue())
                    .collect(Collectors.joining("\n"))
            );
        }

        assertThat(remoteSegments.keySet()).containsExactlyInAnyOrderElementsOf(expectedPartitions);

        LOG.info("Remote Log Metadata Tracker initialized");
    }

    public void waitUntilSegmentsAreDeleted(final List<RemoteSegment> segmentsToBeDeleted) {
        final Supplier<Map<RemoteSegment, RemoteLogSegmentState>> segmentsNotDeleted =
            () -> segmentsToBeDeleted.stream()
                .map(rs -> Map.entry(rs, remoteSegmentStates.get(rs.remoteLogSegmentId())))
                .filter(rs -> rs.getValue() != RemoteLogSegmentState.DELETE_SEGMENT_FINISHED)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        LOG.info("Starting validation of remote segments deleted");

        final var timeout = Duration.ofMinutes(1);
        final var startAt = System.currentTimeMillis();

        final var metadataRecords = new LinkedHashMap<Map.Entry<TopicPartition, Long>, RemoteLogMetadata>();

        boolean isReady = false;
        while (!isReady) {
            (consumer.poll(Duration.ofSeconds(5))).forEach(r -> {
                final RemoteLogMetadata metadata = r.value();
                metadataRecords.put(Map.entry(new TopicPartition(r.topic(), r.partition()), r.offset()), metadata);
                if (metadata instanceof RemoteLogSegmentMetadataUpdate) {
                    final var metadataUpdate = (RemoteLogSegmentMetadataUpdate) metadata;
                    remoteSegmentStates.put(metadataUpdate.remoteLogSegmentId(), metadataUpdate.state());
                    if (metadataUpdate.state() == RemoteLogSegmentState.DELETE_SEGMENT_FINISHED) {
                        remoteSegments.get(metadata.topicIdPartition())
                            .removeIf(
                                segment -> segment.remoteLogSegmentId().equals(metadataUpdate.remoteLogSegmentId()));
                    }
                }
            });

            isReady = segmentsNotDeleted.get().isEmpty();

            final var running = System.currentTimeMillis() - startAt;
            if (running > timeout.toMillis()) {
                LOG.warn("Timeout waiting for segments delete finished events to arrive after {} running", running);
                break;
            }
        }

        if (!segmentsNotDeleted.get().isEmpty()) {
            final var notDeleted = segmentsNotDeleted.get();
            fail("Fail to receive delete metadata records for %s out of %s segments."
                    + "%nSegments missing: %n%s"
                    + "%nMetadata events received: %n%s",
                notDeleted.size(), segmentsToBeDeleted.size(),
                notDeleted.entrySet().stream()
                    .map(e -> e.getKey().remoteLogSegmentId().toString()
                        + "[" + e.getKey().startOffset() + "-" + e.getKey().endOffset() + "] => "
                        + e.getValue())
                    .collect(Collectors.joining("\n")),
                metadataRecords.entrySet().stream()
                    .map(e -> e.getKey().getKey()
                        + "-" + e.getKey().getValue() + ":" + e.getValue() + " => "
                        + e.getValue())
                    .collect(Collectors.joining("\n"))
            );
        }

        LOG.info("Remote segments deleted validation complete");
    }
}
