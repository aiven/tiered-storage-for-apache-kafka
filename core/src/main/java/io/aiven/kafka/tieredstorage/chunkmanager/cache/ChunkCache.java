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

package io.aiven.kafka.tieredstorage.chunkmanager.cache;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.kafka.common.Configurable;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

public interface ChunkCache<T> extends Configurable {
    InputStream getChunk(final ChunkKey chunkKey,
                         final Supplier<CompletableFuture<InputStream>> chunkSupplier)
        throws StorageBackendException, IOException;

    void supplyIfAbsent(final ChunkKey chunkKey,
                        final Supplier<CompletableFuture<InputStream>> chunkSupplier);
}
