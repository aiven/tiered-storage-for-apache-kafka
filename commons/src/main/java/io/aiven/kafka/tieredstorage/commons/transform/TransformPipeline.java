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

package io.aiven.kafka.tieredstorage.commons.transform;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import io.aiven.kafka.tieredstorage.commons.Chunk;
import io.aiven.kafka.tieredstorage.commons.UniversalRemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentEncryptionMetadata;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentEncryptionMetadataV1;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.commons.manifest.serde.DataKeyDeserializer;
import io.aiven.kafka.tieredstorage.commons.manifest.serde.DataKeySerializer;
import io.aiven.kafka.tieredstorage.commons.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.commons.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.commons.security.RsaEncryptionProvider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

public class TransformPipeline {

    final int chunkSize;
    final boolean withCompression;
    final boolean withEncryption;
    final DataKeyAndAAD dataKeyAndAAD;
    final int ivSize;
    final Supplier<Cipher> inboundCipherSupplier;
    final BiFunction<byte[], SegmentEncryptionMetadata, Cipher> outboundCipherSupplier;
    final ObjectMapper objectMapper;

    public TransformPipeline(final int chunkSize,
                             final boolean withCompression,
                             final boolean withEncryption,
                             final DataKeyAndAAD dataKeyAndAAD,
                             final int ivSize,
                             final Supplier<Cipher> inboundCipherSupplier,
                             final BiFunction<byte[], SegmentEncryptionMetadata, Cipher> outboundCipherSupplier,
                             final ObjectMapper objectMapper) {
        this.chunkSize = chunkSize;
        this.withCompression = withCompression;
        this.withEncryption = withEncryption;
        this.dataKeyAndAAD = dataKeyAndAAD;
        this.ivSize = ivSize;
        this.inboundCipherSupplier = inboundCipherSupplier;
        this.outboundCipherSupplier = outboundCipherSupplier;
        this.objectMapper = objectMapper;
    }


    public static TransformPipeline.Builder newBuilder() {
        return new Builder();
    }

    public SegmentManifest segmentManifest(final TransformFinisher transformFinisher) {
        SegmentEncryptionMetadataV1 encryption = null;
        if (withEncryption) {
            encryption = new SegmentEncryptionMetadataV1(dataKeyAndAAD.dataKey, dataKeyAndAAD.aad);
        }
        return new SegmentManifestV1(transformFinisher.chunkIndex(), withCompression, encryption);
    }

    public InboundTransformChain inboundTransformChain(final Path logPath) throws IOException {
        return inboundTransformChain(Files.newInputStream(logPath), (int) Files.size(logPath));
    }

    public InboundTransformChain inboundTransformChain(final byte[] original) {
        return inboundTransformChain(new ByteArrayInputStream(original), original.length);
    }

    public InboundTransformChain inboundTransformChain(final InputStream content, final int size) {
        final Function<InboundTransformChain, InboundTransformChain> inboundFunction = inboundTransformChain -> {
            if (withCompression) {
                inboundTransformChain.chain(CompressionChunkEnumeration::new);
            }
            if (withEncryption) {
                inboundTransformChain.chain(inboundTransform ->
                    new EncryptionChunkEnumeration(inboundTransform, inboundCipherSupplier));
            }
            return inboundTransformChain;
        };
        return inboundFunction.apply(new InboundTransformChain(content, size, chunkSize));
    }

    public OutboundTransformChain outboundTransformChain(final byte[] uploadedData,
                                                         final SegmentManifest manifest,
                                                         final List<Chunk> chunks) {
        return outboundTransformChain(new ByteArrayInputStream(uploadedData), manifest, chunks);
    }

    public OutboundTransformChain outboundTransformChain(final InputStream uploadedData,
                                                         final SegmentManifest manifest,
                                                         final List<Chunk> chunks) {
        final Function<OutboundTransformChain, OutboundTransformChain> outboundFunction =
            outboundTransformChain -> {
                if (withEncryption) {
                    outboundTransformChain.chain(
                        outboundTransform ->
                            new DecryptionChunkEnumeration(
                                outboundTransform,
                                ivSize,
                                bytes -> outboundCipherSupplier.apply(bytes, manifest.encryption().get())));
                }
                if (withCompression) {
                    outboundTransformChain.chain(DecompressionChunkEnumeration::new);
                }
                return outboundTransformChain;
            };
        return outboundFunction.apply(new OutboundTransformChain(uploadedData, chunks));
    }

    public InputStream serializeSegmentManifest(final SegmentManifest segmentManifest) throws JsonProcessingException {
        return new ByteArrayInputStream(objectMapper.writeValueAsBytes(segmentManifest));
    }

    public SegmentManifest deserializeSegmentManifestContent(final InputStream content) throws IOException {
        return objectMapper.readValue(content, SegmentManifestV1.class);
    }

    public static class Builder {
        private int chunkSize;
        private boolean withEncryption = false;
        private int ivSize = -1;
        private Supplier<Cipher> inboundCipherSupplier = null;
        private BiFunction<byte[], SegmentEncryptionMetadata, Cipher> outboundCipherSupplier = null;
        private boolean withCompression = false;
        private DataKeyAndAAD dataKeyAndAAD;
        private RsaEncryptionProvider rsaEncryptionProvider;

        private ObjectMapper getObjectMapper() {
            final ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new Jdk8Module());
            if (withEncryption) {
                final SimpleModule simpleModule = new SimpleModule();
                simpleModule.addSerializer(SecretKey.class,
                    new DataKeySerializer(rsaEncryptionProvider::encryptDataKey));
                simpleModule.addDeserializer(SecretKey.class, new DataKeyDeserializer(
                    b -> new SecretKeySpec(rsaEncryptionProvider.decryptDataKey(b), "AES")));
                objectMapper.registerModule(simpleModule);
            }
            return objectMapper;
        }

        public Builder withChunkSize(final int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder withEncryption(final Path publicKeyFile, final Path privateKeyFile) {
            rsaEncryptionProvider = RsaEncryptionProvider.of(publicKeyFile, privateKeyFile);
            final AesEncryptionProvider aesEncryptionProvider = AesEncryptionProvider.of(rsaEncryptionProvider);
            dataKeyAndAAD = aesEncryptionProvider.createDataKeyAndAAD();
            ivSize = aesEncryptionProvider.encryptionCipher(dataKeyAndAAD).getIV().length;
            withEncryption = true;
            inboundCipherSupplier = () -> aesEncryptionProvider.encryptionCipher(dataKeyAndAAD);
            outboundCipherSupplier = aesEncryptionProvider::decryptionCipher;
            return this;
        }

        public Builder withCompression() {
            withCompression = true;
            return this;
        }

        public Builder fromConfig(final UniversalRemoteStorageManagerConfig config) {
            withChunkSize(config.chunkSize());
            if (config.compressionEnabled()) {
                withCompression();
            }
            if (config.encryptionEnabled()) {
                withEncryption(config.encryptionPublicKeyFile(), config.encryptionPrivateKeyFile());
            }
            return this;
        }

        public TransformPipeline build() {
            return new TransformPipeline(
                chunkSize,
                withCompression,
                withEncryption,
                dataKeyAndAAD,
                ivSize,
                inboundCipherSupplier,
                outboundCipherSupplier,
                getObjectMapper());
        }
    }
}