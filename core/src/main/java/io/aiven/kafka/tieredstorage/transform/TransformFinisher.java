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

package io.aiven.kafka.tieredstorage.transform;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import io.aiven.kafka.tieredstorage.manifest.index.AbstractChunkIndexBuilder;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndexBuilder;
import io.aiven.kafka.tieredstorage.manifest.index.VariableSizeChunkIndexBuilder;

import io.aiven.kafka.tieredstorage.util.RemoteEnumeration;
import io.github.bucket4j.Bucket;

/**
 * The transformation finisher.
 *
 * <p>It converts enumeration of {@code byte[]} into enumeration of {@link InputStream},
 * so that it could be used in {@link SequenceInputStream}.
 *
 * <p>It's responsible for building the chunk index.
 * The chunk index is empty (i.e. null) if chunking has been disabled (i.e. chunk size is zero),
 * but could also have a single chunk if the chunk size is equal or higher to the original file size.
 * Otherwise, the chunk index will contain more than one chunk.
 */
public class TransformFinisher extends RemoteEnumeration {
    private final TransformChunkEnumeration inner;
    private final AbstractChunkIndexBuilder chunkIndexBuilder;
    private final Path originalFilePath;
    private final int originalFileSize;
    private ChunkIndex chunkIndex = null;

    public static Builder newBuilder(final TransformChunkEnumeration inner, final int originalFileSize) {
        return new Builder(inner, originalFileSize);
    }

    private TransformFinisher(
        final TransformChunkEnumeration inner,
        final boolean chunkingEnabled,
        final Path originalFilePath,
        final int originalFileSize,
        final Bucket rateLimitingBucket
    ) {
        super(rateLimitingBucket);
        this.inner = Objects.requireNonNull(inner, "inner cannot be null");

        final int originalChunkSize = chunkingEnabled ? inner.originalChunkSize() : originalFileSize;
        this.chunkIndexBuilder = chunkIndexBuilder(inner, originalChunkSize, originalFileSize);
        this.originalFilePath = originalFilePath;
        this.originalFileSize = originalFileSize;
    }

    private static AbstractChunkIndexBuilder chunkIndexBuilder(
        final TransformChunkEnumeration inner,
        final int originalChunkSize,
        final int originalFileSize
    ) {
        final Integer transformedChunkSize = inner.transformedChunkSize();
        if (transformedChunkSize == null) {
            return new VariableSizeChunkIndexBuilder(
                originalChunkSize,
                originalFileSize
            );
        } else {
            return new FixedSizeChunkIndexBuilder(
                originalChunkSize,
                originalFileSize,
                transformedChunkSize
            );
        }
    }

    @Override
    public boolean hasMoreElements() {
        return inner.hasMoreElements();
    }

    @Override
    public InputStream nextElement() {
        final var chunk = inner.nextElement();
        if (hasMoreElements()) {
            this.chunkIndexBuilder.addChunk(chunk.length);
        } else {
            this.chunkIndex = this.chunkIndexBuilder.finish(chunk.length);
        }

        return new ByteArrayInputStream(chunk);
    }

    public ChunkIndex chunkIndex() {
        if (chunkIndex == null) {
            if (isBaseTransform()) {
                // as chunk index will not be built by consuming the input stream, calculate it from source file
                return calculateChunkIndex();
            } else {
                throw new IllegalStateException("Chunk index was not built, was finisher used?");
            }
        }
        return this.chunkIndex;
    }

    private ChunkIndex calculateChunkIndex() {
        final var chunkSize = inner.transformedChunkSize();
        var size = originalFileSize;
        while (size > chunkSize) {
            chunkIndexBuilder.addChunk(chunkSize);
            size -= chunkSize;
        }
        return chunkIndexBuilder.finish(size);
    }

    public InputStream toInputStream() throws IOException {
        if (isBaseTransform() && originalFilePath != null) {
            // close inner input stream (based on source file)
            final var sequenceInputStream = new SequenceInputStream(this);
            sequenceInputStream.close();

            return maybeToRateLimitedInputStream(Files.newInputStream(originalFilePath));
        } else {
            return maybeToRateLimitedInputStream(new SequenceInputStream(this));
        }
    }

    private boolean isBaseTransform() {
        return inner instanceof BaseTransformChunkEnumeration;
    }

    Optional<Path> maybeOriginalFilePath() {
        return Optional.ofNullable(originalFilePath);
    }

    public static class Builder {
        final TransformChunkEnumeration inner;
        final Integer originalFileSize;
        boolean chunkingEnabled = true;
        Path originalFilePath = null;
        Bucket rateLimitingBucket;

        public Builder(final TransformChunkEnumeration inner, final int originalFileSize) {
            this.inner = inner;

            if (originalFileSize < 0) {
                throw new IllegalArgumentException(
                    "originalFileSize must be non-negative, " + originalFileSize + " given");
            }

            this.originalFileSize = originalFileSize;
        }

        public Builder withRateLimitingBucket(final Bucket rateLimitingBucket) {
            this.rateLimitingBucket = rateLimitingBucket;
            return this;
        }

        public Builder withChunkingDisabled() {
            this.chunkingEnabled = false;
            return this;
        }

        public Builder withOriginalFilePath(final Path originalFilePath) {
            this.originalFilePath = Objects.requireNonNull(originalFilePath, "originalFilePath cannot be null");
            return this;
        }

        public TransformFinisher build() {
            return new TransformFinisher(inner, chunkingEnabled, originalFilePath, originalFileSize,
                rateLimitingBucket);
        }
    }
}
