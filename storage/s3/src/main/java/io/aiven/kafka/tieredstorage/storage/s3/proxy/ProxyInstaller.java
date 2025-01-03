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

package io.aiven.kafka.tieredstorage.storage.s3.proxy;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;

import io.aiven.kafka.tieredstorage.storage.proxy.ProxyConfig;
import io.aiven.kafka.tieredstorage.storage.proxy.Socks5ProxyAuthenticator;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.DefaultHttpClientConnectionOperator;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.internal.conn.SdkTlsSocketFactory;
import software.amazon.awssdk.http.apache.internal.impl.ApacheSdkHttpClient;

/**
 * Makes the {@link ApacheHttpClient} connect via a SOCKS5 proxy.
 *
 * <p>This operation heavily depends on reflection.
 * This is not ideal, but this is the only way to do this in the present state of the AWS SDK configurability.
 */
public class ProxyInstaller {
    public static void install(
        final ApacheHttpClient client, final ProxyConfig proxyConfig
    ) throws ReflectiveOperationException {
        final var defaultHttpClientConnectionOperator = extractClientConnectionOperator(client);

        final InetSocketAddress proxyAddr = new InetSocketAddress(proxyConfig.host(), proxyConfig.port());
        final Proxy proxy = new Proxy(Proxy.Type.SOCKS, proxyAddr);

        final var socketFactoryRegistryField = PrivateField.of(
            DefaultHttpClientConnectionOperator.class, defaultHttpClientConnectionOperator,
            Registry.class, "socketFactoryRegistry");

        @SuppressWarnings("unchecked")
        final var originalConnectionSocketFactoryRegistry =
            (Registry<ConnectionSocketFactory>) socketFactoryRegistryField.getValue();

        final var proxiedConnectionSocketFactoryRegistry =
            createProxiedConnectionSocketFactoryRegistry(originalConnectionSocketFactoryRegistry, proxy);
        socketFactoryRegistryField.setValue(proxiedConnectionSocketFactoryRegistry);

        if (proxyConfig.username() != null) {
            Socks5ProxyAuthenticator.register(
                proxyConfig.host(), proxyConfig.port(), proxyConfig.username(), proxyConfig.password());
        }
    }

    private static DefaultHttpClientConnectionOperator extractClientConnectionOperator(
        final ApacheHttpClient apacheHttpClient
    ) throws ReflectiveOperationException {
        final var apacheSdkHttpClient = PrivateField.of(
            ApacheHttpClient.class, apacheHttpClient, ApacheSdkHttpClient.class, "httpClient").getValue();
        final var poolingHttpClientConnectionManager = PrivateField.of(
                ApacheSdkHttpClient.class, apacheSdkHttpClient, PoolingHttpClientConnectionManager.class, "cm")
            .getValue();
        return PrivateField.of(
                PoolingHttpClientConnectionManager.class, poolingHttpClientConnectionManager,
                DefaultHttpClientConnectionOperator.class, "connectionOperator")
            .getValue();
    }

    private static Registry<ConnectionSocketFactory> createProxiedConnectionSocketFactoryRegistry(
        final Registry<ConnectionSocketFactory> originalConnectionSocketFactoryRegistry,
        final Proxy proxy
    ) throws ReflectiveOperationException {
        if (originalConnectionSocketFactoryRegistry.lookup("http") == null) {
            throw new RuntimeException("Connection factory for HTTP doesn't exist");
        }

        final SdkTlsSocketFactory httpsConnectionSocketFactory =
            (SdkTlsSocketFactory) originalConnectionSocketFactoryRegistry.lookup("https");
        if (httpsConnectionSocketFactory == null) {
            throw new RuntimeException("Connection factory for HTTPS doesn't exist");
        }

        final var sslContext = PrivateField.of(
            SdkTlsSocketFactory.class, httpsConnectionSocketFactory, SSLContext.class, "sslContext").getValue();
        final var hostnameVerifier = PrivateField.of(
                SSLConnectionSocketFactory.class, httpsConnectionSocketFactory,
                HostnameVerifier.class, "hostnameVerifier")
            .getValue();
        return RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http", new ProxiedPlainConnectionSocketFactory(proxy))
            .register("https", new ProxiedSdkTlsSocketFactory(sslContext, hostnameVerifier, proxy))
            .build();
    }

    private static class ProxiedPlainConnectionSocketFactory extends PlainConnectionSocketFactory {
        private final Proxy proxy;

        private ProxiedPlainConnectionSocketFactory(final Proxy proxy) {
            this.proxy = proxy;
        }

        @Override
        public Socket createSocket(final HttpContext context) throws IOException {
            return new Socket(proxy);
        }

    }

    private static class ProxiedSdkTlsSocketFactory extends SdkTlsSocketFactory {
        private final Proxy proxy;

        private ProxiedSdkTlsSocketFactory(
            final SSLContext sslContext, final HostnameVerifier hostnameVerifier, final Proxy proxy
        ) {
            super(sslContext, hostnameVerifier);
            this.proxy = proxy;
        }

        @Override
        public Socket createSocket(final HttpContext context) throws IOException {
            return new Socket(proxy);
        }

    }
}
