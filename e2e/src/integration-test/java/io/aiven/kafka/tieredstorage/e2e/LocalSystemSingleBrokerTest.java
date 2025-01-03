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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class LocalSystemSingleBrokerTest extends SingleBrokerTest {
    static final String TS_DATA_SUBDIR_HOST = "ts-data";
    static final String TS_DATA_DIR_CONTAINER = "/home/appuser/kafka-tiered-storage";

    static Path tieredDataDir;

    @BeforeAll
    static void init() throws Exception {
        setupKafka(kafka -> {
            tieredDataDir = baseDir.resolve(TS_DATA_SUBDIR_HOST);
            tieredDataDir.toFile().mkdirs();
            tieredDataDir.toFile().setWritable(true, false);

            kafka
                .withEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH", "/tiered-storage-for-apache-kafka/core/*")
                .withEnv("KAFKA_RSM_CONFIG_STORAGE_BACKEND_CLASS",
                    "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage")
                .withEnv("KAFKA_RSM_CONFIG_STORAGE_ROOT", TS_DATA_DIR_CONTAINER)
                .withFileSystemBind(tieredDataDir.toString(), TS_DATA_DIR_CONTAINER);
        });
    }

    @AfterAll
    static void cleanup() {
        stopKafka();
        cleanupStorage();
    }

    @Override
    boolean assertNoTopicDataOnTierStorage(final String topicName, final Uuid topicId) {
        final String prefix = String.format("%s-%s", topicName, topicId.toString());
        try (final var files = Files.list(tieredDataDir)) {
            return files.noneMatch(path -> path.getFileName().startsWith(prefix));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    List<String> remotePartitionFiles(final TopicIdPartition topicIdPartition) {
        final Path dir = tieredDataDir.resolve(
            String.format("%s-%s/%s",
                topicIdPartition.topic(),
                topicIdPartition.topicId().toString(),
                topicIdPartition.partition()));
        try (final var paths = Files.list(dir)) {
            return paths.map(Path::getFileName)
                .map(Path::toString)
                .sorted()
                .collect(Collectors.toList());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
