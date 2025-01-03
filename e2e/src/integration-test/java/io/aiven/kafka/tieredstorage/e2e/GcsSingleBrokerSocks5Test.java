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

package io.aiven.kafka.tieredstorage.e2e;

import com.google.cloud.storage.BucketInfo;
import org.junit.jupiter.api.BeforeAll;

public class GcsSingleBrokerSocks5Test extends GcsSingleBrokerTest {
    static final String BUCKET = "test-bucket-socks5";

    @BeforeAll
    static void createBucket() {
        gcsClient.create(BucketInfo.newBuilder(BUCKET).build());
    }

    @BeforeAll
    static void startKafka() throws Exception {
        setupKafka(kafka -> rsmPluginBasicSetup(kafka)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_GCS_BUCKET_NAME", BUCKET)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_PROXY_HOST", SOCKS5_NETWORK_ALIAS)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_PROXY_PORT", Integer.toString(SOCKS5_PORT))
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_PROXY_USERNAME", SOCKS5_USER)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_PROXY_PASSWORD", SOCKS5_PASSWORD));
    }

    @Override
    protected String bucket() {
        return BUCKET;
    }
}
