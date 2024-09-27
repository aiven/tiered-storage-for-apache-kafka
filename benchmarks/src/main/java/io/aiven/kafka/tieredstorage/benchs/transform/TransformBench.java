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

package io.aiven.kafka.tieredstorage.benchs.transform;

import java.io.IOException;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import io.aiven.kafka.tieredstorage.benchs.AesKeyAware;
import io.aiven.kafka.tieredstorage.transform.BaseTransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.CompressionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.EncryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.TransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.TransformFinisher;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 4)
@Measurement(iterations = 16)
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TransformBench extends AesKeyAware {
    static Path segmentPath;
    @Param({"10485760", "104857600", "1073741824"})
    public int contentLength; // 10MiB, 100MiB, 1GiB
    @Param({"102400", "1048576", "5242880"})
    public int chunkSize; // 100KiB, 1MiB, 5MiB
    @Param({"false", "true"})
    public boolean compression;
    @Param({"false", "true"})
    public boolean encryption;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        segmentPath = Files.createTempFile("segment", ".log");
        // to fill with random bytes.
        final SecureRandom secureRandom = new SecureRandom();
        try (final var out = Files.newOutputStream(segmentPath)) {
            final byte[] bytes = new byte[contentLength];
            secureRandom.nextBytes(bytes);
            out.write(bytes);
        }
        if (encryption) {
            initCrypto();
        }
    }

    @TearDown
    public void teardown() throws IOException {
        Files.deleteIfExists(segmentPath);
    }

    @Benchmark
    public byte[] test() throws IOException {
        // Transform.
        TransformChunkEnumeration transformEnum = new BaseTransformChunkEnumeration(
            Files.newInputStream(segmentPath),
            chunkSize
        );
        if (compression) {
            transformEnum = new CompressionChunkEnumeration(transformEnum);
        }
        if (encryption) {
            transformEnum = new EncryptionChunkEnumeration(transformEnum, AesKeyAware::encryptionCipherSupplier);
        }
        final var transformFinisher = new TransformFinisher(transformEnum, contentLength);
        try (final var sis = new SequenceInputStream(transformFinisher)) {
            return sis.readAllBytes();
        }
    }

    public static void main(final String[] args) throws Exception {
        final Options opts = new OptionsBuilder()
            .include(TransformBench.class.getSimpleName())
            .addProfiler(AsyncProfiler.class, "output=flamegraph")
            .build();
        new Runner(opts).run();
    }
}
