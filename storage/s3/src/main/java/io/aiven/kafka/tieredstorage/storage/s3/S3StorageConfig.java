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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.tieredstorage.config.validators.NonEmptyPassword;
import io.aiven.kafka.tieredstorage.config.validators.Null;
import io.aiven.kafka.tieredstorage.config.validators.Subclass;
import io.aiven.kafka.tieredstorage.config.validators.ValidUrl;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.builder.Buildable;

public class S3StorageConfig extends AbstractConfig {

    public static final String S3_BUCKET_NAME_CONFIG = "s3.bucket.name";
    private static final String S3_BUCKET_NAME_DOC = "S3 bucket to store log segments";
    public static final String S3_ENDPOINT_URL_CONFIG = "s3.endpoint.url";
    private static final String S3_ENDPOINT_URL_DOC = "Custom S3 endpoint URL. "
        + "To be used with custom S3-compatible backends (e.g. minio).";
    public static final String S3_REGION_CONFIG = "s3.region";
    private static final String S3_REGION_DOC = "AWS region where S3 bucket is placed";
    static final String S3_PATH_STYLE_ENABLED_CONFIG = "s3.path.style.access.enabled";
    private static final String S3_PATH_STYLE_ENABLED_DOC = "Whether to use path style access or virtual hosts. "
        + "By default, empty value means S3 library will auto-detect. "
        + "Amazon S3 uses virtual hosts by default (true), but other S3-compatible backends may differ (e.g. minio).";

    private static final String S3_MULTIPART_UPLOAD_PART_SIZE_CONFIG = "s3.multipart.upload.part.size";
    // AWS limits to 5GiB, but 2GiB are used here as ByteBuffer allocation is based on int
    private static final String S3_MULTIPART_UPLOAD_PART_SIZE_DOC = "Size of parts in bytes to use when uploading. "
        + "All parts but the last one will have this size. "
        + "Valid values: between 5MiB and 2GiB";

    public static final String S3_MULTIPART_UPLOAD_DIRECT_BUFFERS_CONFIG = "s3.multipart.upload.direct.buffers";
    public static final String S3_MULTIPART_UPLOAD_DIRECT_BUFFERS_DOC =
        "Allocate multipart upload buffers as direct buffers (off-heap)";

    static final int S3_MULTIPART_UPLOAD_PART_SIZE_MIN = 5 * 1024 * 1024; // 5MiB
    static final int S3_MULTIPART_UPLOAD_PART_SIZE_MAX = Integer.MAX_VALUE;
    static final int S3_MULTIPART_UPLOAD_PART_SIZE_DEFAULT = S3_MULTIPART_UPLOAD_PART_SIZE_MIN;

    private static final String S3_API_CALL_TIMEOUT_CONFIG = "s3.api.call.timeout";
    private static final String S3_API_CALL_TIMEOUT_DOC = "AWS S3 API call timeout in milliseconds, "
        + "including all retries";
    private static final String S3_API_CALL_ATTEMPT_TIMEOUT_CONFIG = "s3.api.call.attempt.timeout";
    private static final String S3_API_CALL_ATTEMPT_TIMEOUT_DOC = "AWS S3 API call attempt "
        + "(single retry) timeout in milliseconds";
    public static final String AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG = "aws.credentials.provider.class";
    private static final String AWS_CREDENTIALS_PROVIDER_CLASS_DOC = "AWS credentials provider. "
        + "If not set, AWS SDK uses the default "
        + "software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain";
    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    private static final String AWS_ACCESS_KEY_ID_DOC = "AWS access key ID. "
        + "To be used when static credentials are provided.";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    private static final String AWS_SECRET_ACCESS_KEY_DOC = "AWS secret access key. "
        + "To be used when static credentials are provided.";
    public static final String AWS_CERTIFICATE_CHECK_ENABLED_CONFIG = "aws.certificate.check.enabled";
    private static final String AWS_CERTIFICATE_CHECK_ENABLED_DOC =
        "This property is used to enable SSL certificate checking for AWS services. "
            + "When set to \"false\", the SSL certificate checking for AWS services will be bypassed. "
            + "Use with caution and always only in a test environment, as disabling certificate lead the storage "
            + "to be vulnerable to man-in-the-middle attacks.";

    public static final String AWS_CHECKSUM_CHECK_ENABLED_CONFIG = "aws.checksum.check.enabled";
    private static final String AWS_CHECKSUM_CHECK_ENABLED_DOC =
        "This property is used to enable checksum validation done by AWS library. "
            + "When set to \"false\", there will be no validation. "
            + "It is disabled by default as Kafka already validates integrity of the files.";


    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(
                S3_BUCKET_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                S3_BUCKET_NAME_DOC)
            .define(
                S3_ENDPOINT_URL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ValidUrl(),
                ConfigDef.Importance.LOW,
                S3_ENDPOINT_URL_DOC)
            .define(
                S3_REGION_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.MEDIUM,
                S3_REGION_DOC)
            .define(
                S3_PATH_STYLE_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                null,
                ConfigDef.Importance.LOW,
                S3_PATH_STYLE_ENABLED_DOC)
            .define(
                S3_MULTIPART_UPLOAD_DIRECT_BUFFERS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                S3_MULTIPART_UPLOAD_DIRECT_BUFFERS_DOC)
            .define(
                S3_MULTIPART_UPLOAD_PART_SIZE_CONFIG,
                ConfigDef.Type.INT,
                S3_MULTIPART_UPLOAD_PART_SIZE_DEFAULT,
                ConfigDef.Range.between(S3_MULTIPART_UPLOAD_PART_SIZE_MIN, S3_MULTIPART_UPLOAD_PART_SIZE_MAX),
                ConfigDef.Importance.MEDIUM,
                S3_MULTIPART_UPLOAD_PART_SIZE_DOC)
            .define(
                S3_API_CALL_TIMEOUT_CONFIG,
                ConfigDef.Type.LONG,
                null,
                Null.or(ConfigDef.Range.between(1, Long.MAX_VALUE)),
                ConfigDef.Importance.LOW,
                S3_API_CALL_TIMEOUT_DOC)
            .define(
                S3_API_CALL_ATTEMPT_TIMEOUT_CONFIG,
                ConfigDef.Type.LONG,
                null,
                Null.or(ConfigDef.Range.between(1, Long.MAX_VALUE)),
                ConfigDef.Importance.LOW,
                S3_API_CALL_ATTEMPT_TIMEOUT_DOC)
            .define(
                AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                null,
                Subclass.of(AwsCredentialsProvider.class),
                ConfigDef.Importance.LOW,
                AWS_CREDENTIALS_PROVIDER_CLASS_DOC)
            .define(
                AWS_ACCESS_KEY_ID_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                AWS_ACCESS_KEY_ID_DOC)
            .define(
                AWS_SECRET_ACCESS_KEY_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                AWS_SECRET_ACCESS_KEY_DOC)
            .define(AWS_CERTIFICATE_CHECK_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                AWS_CERTIFICATE_CHECK_ENABLED_DOC
            )
            .define(AWS_CHECKSUM_CHECK_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.MEDIUM,
                AWS_CHECKSUM_CHECK_ENABLED_DOC
            );
    }

    public S3StorageConfig(final Map<String, ?> props) {
        super(configDef(), props);
        validate();
    }

    private void validate() {
        if (getPassword(AWS_ACCESS_KEY_ID_CONFIG) != null
            ^ getPassword(AWS_SECRET_ACCESS_KEY_CONFIG) != null) {
            throw new ConfigException(AWS_ACCESS_KEY_ID_CONFIG
                + " and "
                + AWS_SECRET_ACCESS_KEY_CONFIG
                + " must be defined together");
        }
        if (getClass(AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG) != null
            && getPassword(AWS_ACCESS_KEY_ID_CONFIG) != null) {
            throw new ConfigException("Either "
                + " static credential pair "
                + AWS_ACCESS_KEY_ID_CONFIG + " and " + AWS_SECRET_ACCESS_KEY_CONFIG
                + " must be set together, or a custom provider class "
                + AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG
                + ". If both are null, default S3 credentials provider is used.");
        }
    }

    Region region() {
        return Region.of(getString(S3_REGION_CONFIG));
    }

    Duration apiCallTimeout() {
        return getDuration(S3_API_CALL_TIMEOUT_CONFIG);
    }

    Duration apiCallAttemptTimeout() {
        return getDuration(S3_API_CALL_ATTEMPT_TIMEOUT_CONFIG);
    }

    AwsCredentialsProvider credentialsProvider() {
        @SuppressWarnings("unchecked") final Class<? extends AwsCredentialsProvider> providerClass =
            (Class<? extends AwsCredentialsProvider>) getClass(AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG);
        final boolean credentialsProvided =
            getPassword(AWS_ACCESS_KEY_ID_CONFIG) != null
                && getPassword(AWS_SECRET_ACCESS_KEY_CONFIG) != null;
        if (credentialsProvided) {
            final AwsCredentials staticCredentials = AwsBasicCredentials.create(
                getPassword(AWS_ACCESS_KEY_ID_CONFIG).value(),
                getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value()
            );
            return StaticCredentialsProvider.create(staticCredentials);
        } else if (Objects.isNull(providerClass)) {
            return null; // to use S3 default provider chain. no public constructor
        } else if (StaticCredentialsProvider.class.isAssignableFrom(providerClass)) {
            throw new ConfigException("With " + StaticCredentialsProvider.class.getName()
                + " AWS credentials must be provided");
        } else {
            final Class<?> klass = getClass(AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG);
            try {
                final var builder = klass.getMethod("builder");
                return (AwsCredentialsProvider) ((Buildable) builder.invoke(klass)).build();
            } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                try {
                    final Method create = klass.getMethod("create");
                    return (AwsCredentialsProvider) create.invoke(klass);
                } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
                    throw new RuntimeException("Specified AWS credentials provider is unsupported", ex);
                }
            }
        }
    }

    public Boolean certificateCheckEnabled() {
        return getBoolean(AWS_CERTIFICATE_CHECK_ENABLED_CONFIG);
    }

    public Boolean checksumCheckEnabled() {
        return getBoolean(AWS_CHECKSUM_CHECK_ENABLED_CONFIG);
    }

    public String bucketName() {
        return getString(S3_BUCKET_NAME_CONFIG);
    }

    public Boolean pathStyleAccessEnabled() {
        return getBoolean(S3_PATH_STYLE_ENABLED_CONFIG);
    }

    public Boolean multipartDirectBuffers() {
        return getBoolean(S3_MULTIPART_UPLOAD_DIRECT_BUFFERS_CONFIG);
    }

    public int uploadPartSize() {
        return getInt(S3_MULTIPART_UPLOAD_PART_SIZE_CONFIG);
    }

    URI s3ServiceEndpoint() {
        final String url = getString(S3_ENDPOINT_URL_CONFIG);
        if (url != null) {
            return URI.create(url);
        } else {
            return null;
        }
    }

    private Duration getDuration(final String key) {
        final var value = getLong(key);
        if (value != null) {
            return Duration.ofMillis(value);
        } else {
            return null;
        }
    }
}
