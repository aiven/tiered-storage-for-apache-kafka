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

package io.aiven.kafka.tieredstorage.commons;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.commons.manifest.SegmentEncryptionMetadata;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.commons.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.commons.security.EncryptionProvider;
import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;
import io.aiven.kafka.tieredstorage.commons.transform.BaseDetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.DecompressionChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.DecryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.DetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.DetransformFinisher;

public class ChunkManager {
    private final ObjectStorageFactory objectStorageFactory;
    private final ObjectKey objectKey;
    private final EncryptionProvider encryptionProvider;

    public ChunkManager(final ObjectStorageFactory objectStorageFactory,
                        final ObjectKey objectKey,
                        final EncryptionProvider encryptionProvider) {
        this.objectStorageFactory = objectStorageFactory;
        this.objectKey = objectKey;
        this.encryptionProvider = encryptionProvider;
    }

    /**
     * Gets a chunk of a segment.
     *
     * @return an {@link InputStream} of the chunk, plain text (i.e. decrypted and decompressed).
     */
    public InputStream getChunk(final RemoteLogSegmentMetadata remoteLogSegmentMetadata, final SegmentManifest manifest,
            final int chunkId) throws IOException {
        final Chunk chunk = manifest.chunkIndex().chunks().get(chunkId);
        final String segmentKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.LOG);
        final InputStream chunkContent = objectStorageFactory.fileFetcher().fetch(segmentKey, chunk.range());
        DetransformChunkEnumeration detransformEnum = new BaseDetransformChunkEnumeration(chunkContent, List.of(chunk));
        if (manifest.encryption().isPresent()) {
            final SegmentEncryptionMetadata encryptionMetadata = manifest.encryption().get();
            final DataKeyAndAAD dataKeyAndAAD = new DataKeyAndAAD(
                encryptionMetadata.dataKey(),
                encryptionMetadata.aad()
            );
            detransformEnum = new DecryptionChunkEnumeration(
                detransformEnum,
                encryptionProvider.ivSize(),
                encryptedChunk -> encryptionProvider.decryptionCipher(encryptedChunk, dataKeyAndAAD)
            );
        }
        if (manifest.compression()) {
            detransformEnum = new DecompressionChunkEnumeration(detransformEnum);
        }
        final DetransformFinisher detransformFinisher = new DetransformFinisher(detransformEnum);
        return detransformFinisher.nextElement();
    }
}
