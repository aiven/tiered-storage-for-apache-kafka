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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3MinioSingleBrokerTest extends SingleBrokerTest {

    static final int MINIO_PORT = 9000;
    static final GenericContainer<?> MINIO = new GenericContainer<>(DockerImageName.parse("minio/minio"))
        .withCommand("server", "/data", "--console-address", ":9090")
        .withExposedPorts(MINIO_PORT)
        .withNetwork(NETWORK)
        .withNetworkAliases("minio");
    static final String ACCESS_KEY_ID = "minioadmin";
    static final String SECRET_ACCESS_KEY = "minioadmin";
    static final String REGION = "us-east-1";
    static final String BUCKET = "test-bucket";

    static S3Client s3Client;

    @BeforeAll
    static void init() throws Exception {
        MINIO.start();

        final String minioServerUrl = String.format("http://minio:%s", MINIO_PORT);

        createBucket(minioServerUrl);

        initializeS3Client();

        setupKafka(kafka -> kafka.withEnv("KAFKA_RSM_CONFIG_STORAGE_BACKEND_CLASS",
                "io.aiven.kafka.tieredstorage.storage.s3.S3Storage")
            .withEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH",
                "/tiered-storage-for-apache-kafka/core/*:/tiered-storage-for-apache-kafka/s3/*")
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_S3_BUCKET_NAME", BUCKET)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_S3_REGION", REGION)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_S3_PATH_STYLE_ACCESS_ENABLED", "true")
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_AWS_ACCESS_KEY_ID", ACCESS_KEY_ID)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_AWS_SECRET_ACCESS_KEY", SECRET_ACCESS_KEY)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_S3_ENDPOINT_URL", minioServerUrl)
            .dependsOn(MINIO));
    }

    private static void initializeS3Client() {
        final Integer mappedPort = MINIO.getFirstMappedPort();
        Testcontainers.exposeHostPorts(mappedPort);
        s3Client = S3Client.builder()
            .region(Region.of(REGION))
            .endpointOverride(URI.create("http://localhost:" + mappedPort))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY)
                )
            )
            .forcePathStyle(true)
            .build();
        s3Client.listBuckets().buckets()
            .forEach(bucket -> LOG.info("S3 bucket: {}", bucket.name()));
    }

    private static void createBucket(final String minioServerUrl) {
        final String cmd =
            "/usr/bin/mc config host add local "
                + minioServerUrl + " " + ACCESS_KEY_ID + " " + SECRET_ACCESS_KEY + " --api s3v4 &&"
                + "/usr/bin/mc mb local/test-bucket;\n";

        final GenericContainer<?> mcContainer = new GenericContainer<>("minio/mc")
            .withNetwork(NETWORK)
            .withStartupCheckStrategy(new OneShotStartupCheckStrategy())
            .withCreateContainerCmdModifier(containerCommand -> containerCommand
                .withTty(true)
                .withEntrypoint("/bin/sh", "-c", cmd));
        mcContainer.start();
    }

    @AfterAll
    static void cleanup() {
        stopKafka();

        MINIO.stop();

        cleanupStorage();
    }

    @Override
    boolean assertNoTopicDataOnTierStorage(final String topicName, final Uuid topicId) {
        final String prefix = String.format("%s-%s", topicName, topicId.toString());
        final var request = ListObjectsV2Request.builder().bucket(BUCKET).prefix(prefix).build();
        return s3Client.listObjectsV2(request).keyCount() == 0;
    }

    @Override
    List<String> remotePartitionFiles(final TopicIdPartition topicIdPartition) {
        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(BUCKET).build();
        final List<S3Object> s3Objects = new ArrayList<>();
        ListObjectsV2Response result;
        while ((result = s3Client.listObjectsV2(request)).isTruncated()) {
            s3Objects.addAll(result.contents());
            request = request.toBuilder().continuationToken(result.nextContinuationToken()).build();
        }
        s3Objects.addAll(result.contents());

        return s3Objects.stream()
            .map(S3Object::key)
            .map(k -> k.substring(k.lastIndexOf('/') + 1))
            .sorted()
            .collect(Collectors.toList());
    }
}
