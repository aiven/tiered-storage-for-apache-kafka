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

package io.aiven.kafka.tieredstorage.fetch.cache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.fetch.FetchManager;
import io.aiven.kafka.tieredstorage.fetch.FetchPart;
import io.aiven.kafka.tieredstorage.fetch.FetchPartKey;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FetchCacheTest {

    private static final byte[] CHUNK_0 = "0123456789".getBytes();
    private static final byte[] CHUNK_1 = "1011121314".getBytes();
    private static final FixedSizeChunkIndex FIXED_SIZE_CHUNK_INDEX = new FixedSizeChunkIndex(10, 20, 12, 24);
    private static final SegmentIndexesV1 SEGMENT_INDEXES = SegmentIndexesV1.builder()
        .add(IndexType.OFFSET, 1)
        .add(IndexType.TIMESTAMP, 1)
        .add(IndexType.PRODUCER_SNAPSHOT, 1)
        .add(IndexType.LEADER_EPOCH, 1)
        .add(IndexType.TRANSACTION, 1)
        .build();

    private static final SegmentManifest SEGMENT_MANIFEST =
        new SegmentManifestV1(FIXED_SIZE_CHUNK_INDEX, SEGMENT_INDEXES, false, null, null);
    private static final String TEST_EXCEPTION_MESSAGE = "test_message";
    private static final ObjectKey SEGMENT_OBJECT_KEY = () -> "topic/segment";

    @Mock
    private FetchManager fetchManager;
    private FetchCache<?> fetchCache;

    @BeforeEach
    void setUp() {
        fetchCache = spy(new InMemoryFetchCache(fetchManager));
    }

    @AfterEach
    void tearDown() {
        reset(fetchManager);
    }

    @Nested
    class CacheTests {
        @Mock
        RemovalListener<FetchPartKey, ?> removalListener;

        FetchPart firstPart;
        FetchPart nextPart;

        @BeforeEach
        void setUp() throws Exception {
            doAnswer(invocation -> removalListener).when(fetchCache).removalListener();
            final var chunkIndex = SEGMENT_MANIFEST.chunkIndex();
            firstPart = new FetchPart(chunkIndex, chunkIndex.chunks().get(0), 1);
            when(fetchManager.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart))
                .thenAnswer(invocation -> new ByteArrayInputStream(CHUNK_0));
            nextPart = firstPart.next().get();
            when(fetchManager.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart))
                .thenAnswer(invocation -> new ByteArrayInputStream(CHUNK_1));
        }

        @Test
        void noEviction() throws IOException, StorageBackendException {
            fetchCache.configure(Map.of(
                "retention.ms", "-1",
                "size", "-1"
            ));

            final InputStream part0 = fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThat(part0).hasBinaryContent(CHUNK_0);
            verify(fetchManager).fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart);
            final InputStream cachedPart0 =
                fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThat(cachedPart0).hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(fetchManager);

            final InputStream part1 = fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart);
            assertThat(part1).hasBinaryContent(CHUNK_1);
            verify(fetchManager).fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart);
            final InputStream cachedPart1 =
                fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart);
            assertThat(cachedPart1).hasBinaryContent(CHUNK_1);
            verifyNoMoreInteractions(fetchManager);

            verifyNoInteractions(removalListener);
        }

        @Test
        void timeBasedEviction() throws IOException, StorageBackendException, InterruptedException {
            fetchCache.configure(Map.of(
                "retention.ms", "100",
                "size", "-1"
            ));

            assertThat(fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            verify(fetchManager).fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThat(fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(fetchManager);

            Thread.sleep(100);

            assertThat(fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart))
                .hasBinaryContent(CHUNK_1);
            verify(fetchManager).fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart);
            assertThat(fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart))
                .hasBinaryContent(CHUNK_1);
            verifyNoMoreInteractions(fetchManager);

            await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(100))
                .until(() -> !mockingDetails(removalListener).getInvocations().isEmpty());

            verify(removalListener)
                .onRemoval(
                    argThat(argument -> argument.range.from == 0),
                    any(),
                    eq(RemovalCause.EXPIRED));

            assertThat(fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            verify(fetchManager, times(2)).fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart);
        }

        @Test
        void sizeBasedEviction() throws IOException, StorageBackendException {
            fetchCache.configure(Map.of(
                "retention.ms", "-1",
                "size", "18"
            ));

            assertThat(fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            verify(fetchManager).fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThat(fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(fetchManager);

            assertThat(fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart))
                .hasBinaryContent(CHUNK_1);
            verify(fetchManager).fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart);

            await().atMost(Duration.ofMillis(5000))
                .pollDelay(Duration.ofSeconds(2))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> !mockingDetails(removalListener).getInvocations().isEmpty());

            verify(removalListener).onRemoval(any(FetchPartKey.class), any(), eq(RemovalCause.SIZE));

            assertThat(fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            assertThat(fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart))
                .hasBinaryContent(CHUNK_1);
            verify(fetchManager, times(3)).fetchPartContent(eq(SEGMENT_OBJECT_KEY), eq(SEGMENT_MANIFEST), any());
        }

        @Test
        void preparingParts() throws Exception {
            fetchCache.configure(Map.of(
                "retention.ms", "-1",
                "size", "-1"
            ));
            fetchCache.prepareParts(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, Set.of(firstPart, nextPart));
            await().pollInterval(Duration.ofMillis(5)).until(() -> fetchCache.statsCounter.snapshot().loadCount() == 2);
            verify(fetchManager, times(2)).fetchPartContent(eq(SEGMENT_OBJECT_KEY), eq(SEGMENT_MANIFEST), any());

            final InputStream cachedPart0 =
                fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThat(cachedPart0).hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(fetchManager);

            final InputStream cachedPart1 =
                fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart);
            assertThat(cachedPart1).hasBinaryContent(CHUNK_1);
            verifyNoMoreInteractions(fetchManager);
        }

        @Test
        void preparingFirstPart() throws Exception {
            fetchCache.configure(Map.of(
                "retention.ms", "-1",
                "size", "-1"
            ));
            fetchCache.prepareParts(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, Set.of(firstPart));
            await().pollInterval(Duration.ofMillis(5))
                .until(() -> fetchCache.statsCounter.snapshot().loadCount() == 1);
            verify(fetchManager).fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart);

            final InputStream cachedPart0 =
                fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThat(cachedPart0).hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(fetchManager);

            final InputStream cachedPart1 =
                fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart);
            assertThat(cachedPart1).hasBinaryContent(CHUNK_1);
            verify(fetchManager).fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart);
        }
    }

    @Nested
    class ErrorHandlingTests {
        FetchPart firstPart;
        FetchPart nextPart;

        private final Map<String, String> configs = Map.of(
            "retention.ms", "-1",
            "size", "-1"
        );

        @BeforeEach
        void setUp() {
            fetchCache.configure(configs);

            firstPart = new FetchPart(SEGMENT_MANIFEST.chunkIndex(), SEGMENT_MANIFEST.chunkIndex().chunks().get(0), 1);
            nextPart = firstPart.next().get();
        }

        @Test
        void failedFetching() throws Exception {
            when(fetchManager.fetchPartContent(eq(SEGMENT_OBJECT_KEY), eq(SEGMENT_MANIFEST), any()))
                .thenThrow(new StorageBackendException(TEST_EXCEPTION_MESSAGE))
                .thenThrow(new IOException(TEST_EXCEPTION_MESSAGE));

            assertThatThrownBy(() -> fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart))
                .isInstanceOf(StorageBackendException.class)
                .hasMessage(TEST_EXCEPTION_MESSAGE);
            assertThatThrownBy(() -> fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, nextPart))
                .isInstanceOf(IOException.class)
                .hasMessage(TEST_EXCEPTION_MESSAGE);
        }

        @Test
        void failedReadingCachedValueWithInterruptedException() throws Exception {
            when(fetchManager.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart))
                .thenReturn(new ByteArrayInputStream(CHUNK_0));

            doCallRealMethod().doAnswer(invocation -> {
                throw new InterruptedException(TEST_EXCEPTION_MESSAGE);
            }).when(fetchCache).readCachedPartContent(any());

            fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThatThrownBy(() -> fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(InterruptedException.class)
                .hasRootCauseMessage(TEST_EXCEPTION_MESSAGE);
        }

        @Test
        void failedReadingCachedValueWithExecutionException() throws Exception {
            when(fetchManager.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart)).thenReturn(
                new ByteArrayInputStream(CHUNK_0));
            doCallRealMethod().doAnswer(invocation -> {
                throw new ExecutionException(new RuntimeException(TEST_EXCEPTION_MESSAGE));
            }).when(fetchCache).readCachedPartContent(any());

            fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThatThrownBy(() -> fetchCache.fetchPartContent(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, firstPart))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage(TEST_EXCEPTION_MESSAGE);
        }
    }
}
