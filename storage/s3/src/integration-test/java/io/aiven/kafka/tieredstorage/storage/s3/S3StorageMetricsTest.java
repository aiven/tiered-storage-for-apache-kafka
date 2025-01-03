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

package io.aiven.kafka.tieredstorage.storage.s3;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@Testcontainers
class S3StorageMetricsTest {
    @Container
    private static final LocalStackContainer LOCALSTACK = S3TestContainer.container();

    private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    private static final int PART_SIZE = 5 * 1024 * 1024;

    private static final String BUCKET_NAME = "test-bucket";

    private S3Storage storage;

    @BeforeAll
    static void setupS3() {
        final var clientBuilder = S3Client.builder();
        clientBuilder.region(Region.of(LOCALSTACK.getRegion()))
            .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        LOCALSTACK.getAccessKey(),
                        LOCALSTACK.getSecretKey()
                    )
                )
            );
        try (final S3Client s3Client = clientBuilder.build()) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
        }
    }

    @BeforeEach
    void setupStorage() {
        storage = new S3Storage();
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", LOCALSTACK.getRegion(),
            "s3.endpoint.url", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
            "aws.access.key.id", LOCALSTACK.getAccessKey(),
            "aws.secret.access.key", LOCALSTACK.getSecretKey(),
            "s3.path.style.access.enabled", true,
            "s3.multipart.upload.part.size", PART_SIZE
        );
        storage.configure(configs);
    }

    @Test
    void metricsShouldBeReported(@TempDir final Path tmpDir) throws Exception {
        final byte[] data = new byte[PART_SIZE + 1];

        final ObjectKey key = new TestObjectKey("x");

        storage.upload(new ByteArrayInputStream(data), key);
        try (final InputStream fetch = storage.fetch(key)) {
            fetch.readAllBytes();
        }
        try (final InputStream fetch = storage.fetch(key, BytesRange.of(0, 1))) {
            fetch.readAllBytes();
        }
        storage.delete(key);
        storage.delete(Set.of(key));

        final InputStream failingInputStream = mock(InputStream.class);
        final IOException exception = new IOException("test");
        when(failingInputStream.transferTo(any())).thenThrow(exception);
        assertThatThrownBy(() -> storage.upload(failingInputStream, key))
            .hasRootCause(exception);

        final ObjectName s3ClientMetrics = ObjectName.getInstance(
            "aiven.kafka.server.tieredstorage.s3:type=s3-client-metrics");
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "get-object-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "get-object-requests-total"))
            .isEqualTo(2.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "get-object-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "get-object-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "put-object-requests-rate"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "put-object-requests-total"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "put-object-time-avg"))
            .isEqualTo(Double.NaN);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "put-object-time-max"))
            .isEqualTo(Double.NaN);

        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "delete-object-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "delete-object-requests-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "delete-object-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "delete-object-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "delete-objects-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "delete-objects-requests-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "delete-objects-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "delete-objects-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "create-multipart-upload-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "create-multipart-upload-requests-total"))
            .isEqualTo(2.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "create-multipart-upload-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "create-multipart-upload-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "upload-part-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "upload-part-requests-total"))
            .isEqualTo(2.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "upload-part-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "upload-part-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "complete-multipart-upload-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "complete-multipart-upload-requests-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "complete-multipart-upload-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "complete-multipart-upload-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "abort-multipart-upload-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "abort-multipart-upload-requests-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "abort-multipart-upload-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "abort-multipart-upload-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        final var tmpPath = tmpDir.resolve("test.log");
        final var testContent = "test".getBytes(StandardCharsets.UTF_8);
        Files.write(tmpPath, testContent);

        final var anotherKey = new TestObjectKey("y");
        storage.upload(tmpPath, testContent.length, anotherKey);

        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "put-object-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "put-object-requests-total"))
            .asInstanceOf(DOUBLE)
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "put-object-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(s3ClientMetrics, "put-object-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }
}
