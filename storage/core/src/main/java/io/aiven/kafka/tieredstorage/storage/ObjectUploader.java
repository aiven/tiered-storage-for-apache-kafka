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

package io.aiven.kafka.tieredstorage.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public interface ObjectUploader {
    /**
     * @param inputStream content to upload. Not closed as part of the upload.
     * @param key         destination path to an object within a storage backend.
     * @return number of bytes uploaded
     */
    long upload(InputStream inputStream, ObjectKey key) throws StorageBackendException;

    /**
     * Enable backend to use optimized uploading implementations based on source files
     *
     * @param path source path to the object to upload
     * @param size size of the object to upload
     * @param key  destination path to an object within a storage backend
     */
    default long upload(Path path, int size, ObjectKey key) throws StorageBackendException {
        try (final var inputStream = Files.newInputStream(path)) {
            return upload(inputStream, key);
        } catch (IOException e) {
            throw new StorageBackendException("Error uploading file from path: " + path, e);
        }
    }
}
