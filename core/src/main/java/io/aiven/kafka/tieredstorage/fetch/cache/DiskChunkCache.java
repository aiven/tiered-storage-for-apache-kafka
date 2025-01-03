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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.common.utils.Time;

import io.aiven.kafka.tieredstorage.config.DiskChunkCacheConfig;
import io.aiven.kafka.tieredstorage.fetch.ChunkKey;
import io.aiven.kafka.tieredstorage.fetch.ChunkManager;

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

public class DiskChunkCache extends ChunkCache<Path> {
    private static final Logger log = LoggerFactory.getLogger(DiskChunkCache.class);

    private final DiskChunkCacheMetrics metrics;

    private DiskChunkCacheConfig config;

    public DiskChunkCache(final ChunkManager chunkManager) {
        this(chunkManager, Time.SYSTEM);
    }

    DiskChunkCache(final ChunkManager chunkManager, final Time time) {
        super(chunkManager);
        metrics = new DiskChunkCacheMetrics(time);
    }

    @Override
    public InputStream cachedChunkToInputStream(final Path cachedChunk) {
        try {
            return Files.newInputStream(cachedChunk);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes the chunk to specified directory on disk.
     * To be able to understand if the files are fully written to disk during a potential recovery process,
     * this implementation initially writes them to temporary directory and then atomically moves them
     * to the actual caching directory.
     */
    @Override
    public Path cacheChunk(final ChunkKey chunkKey, final InputStream chunk) throws IOException {
        final var chunkKeyPath = chunkKey.path();
        final Path tempChunkPath = config.tempCachePath().resolve(chunkKeyPath);
        final Path tempCached = writeToDisk(chunk, tempChunkPath);
        log.trace("Chunk file has been stored to temporary caching directory {}", tempCached);
        final Path cachedChunkPath = config.cachePath().resolve(chunkKeyPath);
        try {
            final Path newPath = Files.move(tempCached, cachedChunkPath, ATOMIC_MOVE);
            log.trace("Chunk file has been moved to cache directory {}", newPath);
            return newPath;
        } finally {
            // In case of exception during the move, the chunk file should be cleaned from temporary cache directory.
            if (Files.exists(tempCached)) {
                log.error("Failed to move chunk file {} to cache directory from temporary one.", tempCached);
                Files.delete(tempCached);
            }
        }
    }

    private Path writeToDisk(final InputStream chunk, final Path tempChunkPath) throws IOException {
        try (chunk; final var out = Files.newOutputStream(tempChunkPath)) {
            final long bytesTransferred = chunk.transferTo(out);
            metrics.chunkWritten(bytesTransferred);
        }
        return tempChunkPath;
    }

    @Override
    public RemovalListener<ChunkKey, Path> removalListener() {
        return (key, path, cause) -> {
            try {
                if (path != null) {
                    if (Files.exists(path)) {
                        final long fileSize = Files.size(path);
                        Files.delete(path);
                        metrics.chunkDeleted(fileSize);
                        log.trace("Deleted cached file for key {} with path {} from cache directory."
                            + " The reason of the deletion is {}", key, path, cause);
                    } else {
                        log.debug("Deletion of cached file for key {} with "
                            + "path {} is requested by file is not found", key, path);
                    }
                } else {
                    log.warn("Path not present when trying to delete cached file for key {} from cache directory."
                        + " The reason of the deletion is {}", key, cause);
                }
            } catch (final IOException e) {
                log.error("Failed to delete cached file for key {} with path {} from cache directory."
                    + " The reason of the deletion is {}", key, path, cause, e);
            }
        };
    }

    @Override
    public Weigher<ChunkKey, Path> weigher() {
        return (key, value) -> {
            try {
                final var fileSize = Files.size(value);
                if (fileSize <= Integer.MAX_VALUE) {
                    return (int) fileSize;
                } else {
                    log.warn(
                        "Cache size calculation have been inaccurate "
                            + "because size of a cached file was bigger than Integer.MAX_VALUE. "
                            + "This should never happen.");
                    return Integer.MAX_VALUE;
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new DiskChunkCacheConfig(configs);
        this.cache = buildCache(config);
    }
}
