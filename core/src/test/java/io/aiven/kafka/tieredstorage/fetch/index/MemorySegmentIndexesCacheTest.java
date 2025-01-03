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

package io.aiven.kafka.tieredstorage.fetch.index;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.ObjectKeyFactory;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MemorySegmentIndexesCacheTest {

    static final RemoteLogSegmentMetadata REMOTE_LOG_SEGMENT_METADATA =
        new RemoteLogSegmentMetadata(
            new RemoteLogSegmentId(
                new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0)),
                Uuid.randomUuid()),
            1, 100, -1, -1, 1L,
            10, Collections.singletonMap(1, 100L));
    private static final byte[] OFFSET_INDEX = "0123456789".getBytes();
    private static final byte[] TIME_INDEX = "1011121314".getBytes();

    private MemorySegmentIndexesCache cache;
    @Mock
    private Supplier<byte[]> offsetIndexSupplier;
    @Mock
    private Supplier<byte[]> timeIndexSupplier;
    private final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("", false);

    @BeforeEach
    void setUp() {
        cache = spy(new MemorySegmentIndexesCache());
    }

    @SuppressWarnings("unchecked")
    @AfterEach
    void tearDown() {
        reset(offsetIndexSupplier);
        reset(timeIndexSupplier);
    }

    @Nested
    class CacheTests {
        @Mock
        RemovalListener<SegmentIndexKey, ?> removalListener;

        @BeforeEach
        void setUp() {
            doAnswer(invocation -> removalListener).when(cache).removalListener();
            when(offsetIndexSupplier.get()).thenAnswer(invocation -> OFFSET_INDEX);
            when(timeIndexSupplier.get()).thenAnswer(invocation -> TIME_INDEX);
        }

        @SuppressWarnings("unchecked")
        @AfterEach
        void tearDown() {
            reset(removalListener);
        }

        @Test
        void noEviction() throws IOException, StorageBackendException {
            cache.configure(Map.of(
                "size", "-1",
                "retention.ms", "-1"
            ));
            assertThat(cache.cache.asMap()).isEmpty();

            final ObjectKey key = objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.INDEXES);
            final InputStream offsetIndex = cache.get(
                key,
                IndexType.OFFSET,
                offsetIndexSupplier
            );
            assertThat(offsetIndex).hasBinaryContent(OFFSET_INDEX);
            assertThat(cache.cache.asMap()).hasSize(1);
            final InputStream cachedOffsetIndex = cache.get(
                key,
                IndexType.OFFSET,
                offsetIndexSupplier
            );
            assertThat(cachedOffsetIndex).hasBinaryContent(OFFSET_INDEX);
            verifyNoMoreInteractions(offsetIndexSupplier);
            assertThat(cache.cache.asMap()).hasSize(1);

            final InputStream timeIndex = cache.get(
                key,
                IndexType.TIMESTAMP,
                timeIndexSupplier
            );
            assertThat(timeIndex).hasBinaryContent(TIME_INDEX);
            assertThat(cache.cache.asMap()).hasSize(2);
            final InputStream cachedTimeIndex = cache.get(
                key,
                IndexType.TIMESTAMP,
                timeIndexSupplier
            );
            assertThat(cachedTimeIndex).hasBinaryContent(TIME_INDEX);
            verifyNoMoreInteractions(timeIndexSupplier);
            assertThat(cache.cache.asMap()).hasSize(2);

            verifyNoMoreInteractions(removalListener);
        }

        @Test
        void timeBasedEviction() throws IOException, StorageBackendException, InterruptedException {
            cache.configure(Map.of(
                "size", "-1",
                "retention.ms", "100"
            ));
            assertThat(cache.cache.asMap()).isEmpty();

            final ObjectKey key = objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.INDEXES);
            final InputStream offsetIndex = cache.get(
                key,
                IndexType.OFFSET,
                offsetIndexSupplier
            );
            assertThat(offsetIndex).hasBinaryContent(OFFSET_INDEX);
            assertThat(cache.cache.asMap()).hasSize(1);
            final InputStream cachedOffsetIndex = cache.get(
                key,
                IndexType.OFFSET,
                offsetIndexSupplier
            );
            assertThat(cachedOffsetIndex).hasBinaryContent(OFFSET_INDEX);
            verifyNoMoreInteractions(offsetIndexSupplier);
            assertThat(cache.cache.asMap()).hasSize(1);

            // Wait enough for the cache entry to be candidate to expire
            Thread.sleep(100);

            final InputStream timeIndex = cache.get(
                key,
                IndexType.TIMESTAMP,
                timeIndexSupplier
            );
            assertThat(timeIndex).hasBinaryContent(TIME_INDEX);
            assertThat(cache.cache.asMap()).isNotEmpty();
            final InputStream cachedTimeIndex = cache.get(
                key,
                IndexType.TIMESTAMP,
                timeIndexSupplier
            );
            assertThat(cachedTimeIndex).hasBinaryContent(TIME_INDEX);
            verifyNoMoreInteractions(timeIndexSupplier);
            assertThat(cache.cache.asMap()).isNotEmpty();

            await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(100))
                .until(() -> !mockingDetails(removalListener).getInvocations().isEmpty());

            assertThat(cache.cache.asMap()).hasSize(1);
            verify(removalListener)
                .onRemoval(
                    argThat(argument -> argument.indexType == IndexType.OFFSET),
                    any(),
                    eq(RemovalCause.EXPIRED));
        }

        @Test
        void sizeBasedEviction() throws IOException, StorageBackendException {
            cache.configure(Map.of(
                "size", "18",
                "retention.ms", String.valueOf(Duration.ofSeconds(10).toMillis())
            ));
            assertThat(cache.cache.asMap()).isEmpty();

            final ObjectKey key = objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.INDEXES);
            final InputStream offsetIndex = cache.get(
                key,
                IndexType.OFFSET,
                offsetIndexSupplier
            );
            assertThat(offsetIndex).hasBinaryContent(OFFSET_INDEX);
            assertThat(cache.cache.asMap()).hasSize(1);
            assertThat(cache.get(
                key,
                IndexType.OFFSET,
                offsetIndexSupplier
            )).hasBinaryContent(OFFSET_INDEX);

            // Fetching chunk 0 multiple times from the cache to guarantee that during the next fetch of not-yet-cached
            // chunk 0 will not be evicted as least frequently accessed.
            for (int i = 0; i < 50; i++) {
                assertThat(cache.get(
                    key,
                    IndexType.OFFSET,
                    offsetIndexSupplier
                )).hasBinaryContent(OFFSET_INDEX);
            }
            verifyNoMoreInteractions(offsetIndexSupplier);
            assertThat(cache.cache.asMap()).hasSize(1);

            final InputStream timeIndex = cache.get(
                key,
                IndexType.TIMESTAMP,
                timeIndexSupplier
            );
            assertThat(timeIndex).hasBinaryContent(TIME_INDEX);
            assertThat(cache.cache.asMap()).isNotEmpty();

            // because of the retention ms, it may be deleting cached values 1, 2 or both.
            await()
                .atMost(Duration.ofSeconds(30))
                .pollDelay(Duration.ofSeconds(2))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> !mockingDetails(removalListener).getInvocations().isEmpty());

            assertThat(cache.cache.asMap().size()).isLessThanOrEqualTo(1);
            verify(removalListener).onRemoval(any(SegmentIndexKey.class), any(), eq(RemovalCause.SIZE));
        }
    }

    private static final String TEST_EXCEPTION_MESSAGE = "test_message";

    static Stream<Arguments> failedFetching() {
        return Stream.of(
            Arguments.of(new StorageBackendException(TEST_EXCEPTION_MESSAGE), StorageBackendException.class),
            Arguments.of(new IOException(TEST_EXCEPTION_MESSAGE), IOException.class)
        );
    }

    @ParameterizedTest
    @MethodSource
    void failedFetching(final Throwable exception, final Class<? extends Throwable> expectedExceptionClass) {
        when(offsetIndexSupplier.get())
            .thenThrow(new RuntimeException(exception));

        final Map<String, String> configs = Map.of(
            "retention.ms", "-1",
            "size", "-1"
        );
        cache.configure(configs);

        final ObjectKey key = objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.INDEXES);
        assertThatThrownBy(() -> cache
            .get(key, IndexType.OFFSET, offsetIndexSupplier))
            .isInstanceOf(expectedExceptionClass)
            .hasMessage(TEST_EXCEPTION_MESSAGE);
    }
}
