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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3Storage implements StorageBackend {

    private S3Client s3Client;
    private String bucketName;
    private int partSize;

    @Override
    public void configure(final Map<String, ?> configs) {
        final S3StorageConfig config = new S3StorageConfig(configs);
        this.s3Client = S3ClientBuilder.build(config);
        this.bucketName = config.bucketName();
        this.partSize = config.uploadPartSize();
    }

    @Override
    public long upload(final InputStream inputStream, final ObjectKey key) throws StorageBackendException {
        final var out = s3OutputStream(key);
        try (out) {
            inputStream.transferTo(out);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }
        // getting the processed bytes after close to account last flush.
        return out.processedBytes();
    }

    @Override
    public long upload(final Path path, final int size, final ObjectKey key) throws StorageBackendException {
        final var request = PutObjectRequest.builder().bucket(bucketName).key(key.value()).build();
        s3Client.putObject(request, path);
        return size;
    }

    S3MultiPartOutputStream s3OutputStream(final ObjectKey key) {
        return new S3MultiPartOutputStream(bucketName, key, partSize, s3Client);
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            final var deleteRequest = DeleteObjectRequest.builder().bucket(bucketName).key(key.value()).build();
            s3Client.deleteObject(deleteRequest);
        } catch (final SdkClientException e) {
            throw new StorageBackendException("Failed to delete " + key, e);
        }
    }

    @Override
    public void delete(final Set<ObjectKey> keys) throws StorageBackendException {
        try {
            final Set<ObjectIdentifier> ids = keys.stream()
                .map(k -> ObjectIdentifier.builder().key(k.value()).build())
                .collect(Collectors.toSet());
            final Delete delete = Delete.builder().objects(ids).build();
            final DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(delete)
                .build();
            s3Client.deleteObjects(deleteObjectsRequest);
        } catch (final SdkClientException e) {
            throw new StorageBackendException("Failed to delete keys " + keys, e);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key) throws StorageBackendException {
        final GetObjectRequest getRequest = GetObjectRequest.builder().bucket(bucketName).key(key.value()).build();
        try {
            return s3Client.getObject(getRequest);
        } catch (final AwsServiceException e) {
            if (e.statusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            } else {
                throw new StorageBackendException("Failed to fetch " + key, e);
            }
        } catch (final SdkClientException e) {
            throw new StorageBackendException("Failed to fetch " + key, e);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key, final BytesRange range) throws StorageBackendException {
        try {
            if (range.isEmpty()) {
                return InputStream.nullInputStream();
            }
            
            final GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key.value())
                .range(formatRange(range))
                .build();
            return s3Client.getObject(getRequest);
        } catch (final AwsServiceException e) {
            if (e.statusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            }
            if (e.statusCode() == 416) {
                throw new InvalidRangeException("Invalid range " + range, e);
            }

            throw new StorageBackendException("Failed to fetch " + key, e);
        } catch (final SdkClientException e) {
            throw new StorageBackendException("Failed to fetch " + key, e);
        }
    }

    private String formatRange(final BytesRange range) {
        return "bytes=" + range.firstPosition() + "-" + range.lastPosition();
    }

    @Override
    public String toString() {
        return "S3Storage{"
            + "bucketName='" + bucketName + '\''
            + ", partSize=" + partSize
            + '}';
    }
}
