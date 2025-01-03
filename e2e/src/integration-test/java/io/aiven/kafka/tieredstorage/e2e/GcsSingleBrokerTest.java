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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.KafkaContainer;

abstract class GcsSingleBrokerTest extends SingleBrokerTest {
    static final String GCS_SERVER_NETWORK_ALIAS = "fake-gcs-server";

    static final FakeGcsServerContainer GCS_SERVER = new FakeGcsServerContainer()
        .withNetwork(NETWORK)
        .withNetworkAliases(GCS_SERVER_NETWORK_ALIAS)
        .withExternalURL(String.format("http://%s:%s", GCS_SERVER_NETWORK_ALIAS, FakeGcsServerContainer.PORT));

    static Storage gcsClient;

    @BeforeAll
    static void init() throws Exception {
        GCS_SERVER.start();

        gcsClient = StorageOptions.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .setHost(GCS_SERVER.url())
            .setProjectId("test-project")
            .build()
            .getService();
    }

    @AfterAll
    static void cleanup() {
        stopKafka();

        GCS_SERVER.stop();

        cleanupStorage();
    }

    static KafkaContainer rsmPluginBasicSetup(final KafkaContainer container) {
        container
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_BACKEND_CLASS",
                "io.aiven.kafka.tieredstorage.storage.gcs.GcsStorage")
            .withEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH",
                "/tiered-storage-for-apache-kafka/core/*:/tiered-storage-for-apache-kafka/gcs/*")
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_GCS_ENDPOINT_URL", GCS_SERVER.externalUrl())
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_GCS_CREDENTIALS_DEFAULT", "false")
            .dependsOn(GCS_SERVER);
        return container;
    }

    protected abstract String bucket();

    @Override
    boolean assertNoTopicDataOnTierStorage(final String topicName, final Uuid topicId) {
        final String prefix = String.format("%s-%s", topicName, topicId.toString());

        final var list = gcsClient.list(bucket(), Storage.BlobListOption.prefix(prefix));
        return list.streamAll().findAny().isEmpty();
    }

    @Override
    List<String> remotePartitionFiles(final TopicIdPartition topicIdPartition) {
        return gcsClient.list(bucket()).streamAll()
            .map(BlobInfo::getName)
            .map(k -> k.substring(k.lastIndexOf('/') + 1))
            .sorted()
            .collect(Collectors.toList());
    }
}
