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

package io.aiven.kafka.tieredstorage.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public abstract class BaseSocks5Test<T extends StorageBackend> {
    protected static final int SOCKS5_PORT = 1080;
    protected static final String SOCKS5_USER = "user";
    protected static final String SOCKS5_PASSWORD = "password";

    protected static final ObjectKey OBJECT_KEY = () -> "aaa";
    protected static final byte[] TEST_DATA = {0, 1, 2, 3};

    protected static GenericContainer<?> proxyContainer(final boolean authenticated) {
        final Map<String, String> config = new HashMap<>();
        config.put("LISTEN", "0.0.0.0:" + SOCKS5_PORT);
        if (authenticated) {
            config.put("USER", SOCKS5_USER);
            config.put("PASSWORD", SOCKS5_PASSWORD);
        }

        return new GenericContainer<>(DockerImageName.parse("ananclub/ss5"))
            .withEnv(config)
            .withExposedPorts(SOCKS5_PORT);
    }

    protected abstract T createStorageBackend();


    @Test
    void worksWithAuthenticatedProxy() throws StorageBackendException, IOException {
        // Ensure we can access the storage through the proxy with authentication.
        final T storage = createStorageBackend();
        storage.configure(storageConfigForAuthenticatedProxy());

        storage.upload(new ByteArrayInputStream(TEST_DATA), OBJECT_KEY);
        assertThat(storage.fetch(OBJECT_KEY).readAllBytes()).isEqualTo(TEST_DATA);
    }

    protected abstract Map<String, Object> storageConfigForAuthenticatedProxy();

    @Test
    void worksWithUnauthenticatedProxy() throws StorageBackendException, IOException {
        // Ensure we can access the storage through the proxy without authentication.
        final T storage = createStorageBackend();
        storage.configure(storageConfigForUnauthenticatedProxy());

        storage.upload(new ByteArrayInputStream(TEST_DATA), OBJECT_KEY);
        assertThat(storage.fetch(OBJECT_KEY).readAllBytes()).isEqualTo(TEST_DATA);
    }

    protected abstract Map<String, Object> storageConfigForUnauthenticatedProxy();

    @Test
    protected void doesNotWorkWithoutProxy() {
        // This test accompanies the other ones by ensuring that _without_ a proxy
        // we cannot even resolve the host name of the server, which is internal to the Docker network.

        // Due to the nature to the Java name resolution and/or getaddrinfo,
        // for some storage implementations this has a pretty long unconfigurable timeout,
        // so the test may take a couple of minutes.

        final T storage = createStorageBackend();
        storage.configure(storageConfigForNoProxy());

        assertThatThrownBy(() -> storage.upload(new ByteArrayInputStream(TEST_DATA), OBJECT_KEY))
            .isExactlyInstanceOf(StorageBackendException.class)
            .hasRootCauseInstanceOf(UnknownHostException.class)
            .rootCause().message()
            .isIn(possibleRootCauseMessagesWhenNoProxy());
        assertThatThrownBy(() -> storage.fetch(OBJECT_KEY))
            .isExactlyInstanceOf(StorageBackendException.class)
            .hasRootCauseInstanceOf(UnknownHostException.class)
            .rootCause().message()
            .isIn(possibleRootCauseMessagesWhenNoProxy());
    }

    protected abstract Map<String, Object> storageConfigForNoProxy();

    protected abstract Iterable<String> possibleRootCauseMessagesWhenNoProxy();
}
