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

package io.aiven.kafka.tieredstorage.storage.s3;

import io.aiven.kafka.tieredstorage.storage.proxy.ProxyConfig;
import io.aiven.kafka.tieredstorage.storage.s3.proxy.ProxyInstaller;

import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.utils.AttributeMap;

class SdkHttpClientBuilder implements SdkHttpClient.Builder<SdkHttpClientBuilder> {
    private boolean trustAllCertificates = false;
    private ProxyConfig proxyConfig = null;

    void trustAllCertificates() {
        this.trustAllCertificates = true;
    }

    void withProxy(final ProxyConfig proxyConfig) {
        this.proxyConfig = proxyConfig;
    }

    @Override
    public SdkHttpClient buildWithDefaults(final AttributeMap serviceDefaults) {
        final var defaultSdkHttpClientBuilder = new DefaultSdkHttpClientBuilder();

        final AttributeMap actualServiceDefaults = this.trustAllCertificates
            ? serviceDefaults.toBuilder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true).build()
            : serviceDefaults;
        final ApacheHttpClient client =
            (ApacheHttpClient) defaultSdkHttpClientBuilder.buildWithDefaults(actualServiceDefaults);

        if (proxyConfig != null) {
            try {
                ProxyInstaller.install(client, proxyConfig);
            } catch (final ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        return client;
    }
}
