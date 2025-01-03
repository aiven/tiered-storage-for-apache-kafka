/*
 * Copyright 2024 Aiven Oy
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

import java.lang.reflect.Method;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BaseSocks5Test;
import io.aiven.kafka.tieredstorage.storage.TestUtils;

import com.github.dockerjava.api.model.ContainerNetwork;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Testcontainers
class S3Socks5Test extends BaseSocks5Test<S3Storage> {
    static final Network NETWORK = Network.newNetwork();

    @Container
    private static final LocalStackContainer LOCALSTACK = S3TestContainer.container()
        .withNetwork(NETWORK);

    @Container
    static final GenericContainer<?> PROXY_AUTHENTICATED = proxyContainer(true).withNetwork(NETWORK);
    @Container
    static final GenericContainer<?> PROXY_UNAUTHENTICATED = proxyContainer(false).withNetwork(NETWORK);

    private static S3Client s3Client;
    private String bucketName;

    @BeforeAll
    static void setUpClass() {
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
            )
            .build();
        s3Client = clientBuilder.build();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        bucketName = TestUtils.testNameToBucketName(testInfo);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }

    static String internalLocalstackEndpoint() {
        try {
            final String networkName = LOCALSTACK.getDockerClient()
                .inspectNetworkCmd().withNetworkId(NETWORK.getId()).exec().getName();
            final ContainerNetwork containerNetwork = LOCALSTACK.getContainerInfo()
                .getNetworkSettings().getNetworks().get(networkName);
            final String ipAddress = containerNetwork.getIpAddress();
            final Method getServicePortField = LocalStackContainer.class
                .getDeclaredMethod("getServicePort", LocalStackContainer.EnabledService.class);
            getServicePortField.setAccessible(true);
            final int port = (int) getServicePortField.invoke(LOCALSTACK, LocalStackContainer.Service.S3);
            return String.format("http://%s:%d", ipAddress, port);
        } catch (final ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected S3Storage createStorageBackend() {
        return new S3Storage();
    }

    @Override
    protected Map<String, Object> storageConfigForAuthenticatedProxy() {
        final var proxy = PROXY_AUTHENTICATED;
        return Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", LOCALSTACK.getRegion(),
            "s3.endpoint.url", internalLocalstackEndpoint(),
            "aws.access.key.id", LOCALSTACK.getAccessKey(),
            "aws.secret.access.key", LOCALSTACK.getSecretKey(),
            "s3.path.style.access.enabled", true,
            "proxy.host", proxy.getHost(),
            "proxy.port", proxy.getMappedPort(SOCKS5_PORT),
            "proxy.username", SOCKS5_USER,
            "proxy.password", SOCKS5_PASSWORD
        );
    }

    @Override
    protected Map<String, Object> storageConfigForUnauthenticatedProxy() {
        final var proxy = PROXY_UNAUTHENTICATED;
        return Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", LOCALSTACK.getRegion(),
            "s3.endpoint.url", internalLocalstackEndpoint(),
            "aws.access.key.id", LOCALSTACK.getAccessKey(),
            "aws.secret.access.key", LOCALSTACK.getSecretKey(),
            "s3.path.style.access.enabled", true,
            "proxy.host", proxy.getHost(),
            "proxy.port", proxy.getMappedPort(SOCKS5_PORT)
        );
    }

    @Override
    protected Map<String, Object> storageConfigForNoProxy() {
        return Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", LOCALSTACK.getRegion(),
            "s3.endpoint.url", internalLocalstackEndpoint(),
            "aws.access.key.id", LOCALSTACK.getAccessKey(),
            "aws.secret.access.key", LOCALSTACK.getSecretKey(),
            "s3.path.style.access.enabled", true
        );
    }

    @Disabled("Not applicable for S3")
    @Override
    protected void doesNotWorkWithoutProxy() {
        // Unfortunately, S3 does the client-side hostname resolution,
        // so the trick with using the hostname visible only in Docker (i.e. to the proxy containers) won't work.
    }

    @Override
    protected Iterable<String> possibleRootCauseMessagesWhenNoProxy() {
        return null;
    }
}
