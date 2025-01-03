# Demo

The plugin demos.

Docker images built from Aiven Kafka fork (e.g. https://github.com/aiven/kafka/tree/3.3-2022-10-06-tiered-storage) is used.

## Requirements

To run the demos, you need:
- Docker Compose
- `make`
- AWS S3 command line tool (optional)
- `jq` (optional)

## Running

### Local filesystem as "remote" storage: `compose-local-fs.yml`

This scenario uses `FileSystemStorage` as the "remote" storage.

```bash
# Start the compose
make run_local_fs

# Create the topic with any variation
make create_topic_by_size_ts
# or
# make create_topic_by_time_ts
# or with TS disabled
# make create_topic_*_no_ts

# Fill the topic
make fill_topic

# See that segments are uploaded to the remote storage
# (this may take several seconds)
make show_remote_data_fs

# Check that early segments are deleted
# (completely or renamed with `.deleted` suffix)
# from the local storage (this may take several seconds)
make show_local_data

# Check the data is consumable
make consume
```

### AWS S3 as remote storage: `compose-s3-aws.yml`

This scenario uses `S3Storage` with the real AWS S3 as the remote storage.

For this scenario you need to have:
1. Valid AWS S3 credentials (e.g. `AWS_ACCESS_KEY_ID` and `AWS_SECRET_KEY`).
2. A test bucket.

The easiest way to pass these to Kafka and the plugin is using an environment file. Generate one with `make .env` and then set:
```
AWS_ACCESS_KEY_ID=<access key ID>
AWS_SECRET_KEY=<secret access key>
KAFKA_RSM_CONFIG_STORAGE_S3_BUCKET_NAME=<bucket name>`
KAFKA_RSM_CONFIG_STORAGE_S3_REGION=<bucket region>`
```

Then perform the following steps:

```bash
# Start the compose
make run_s3_aws

# Create the topic with any variation
make create_topic_by_size_ts
# or
# make create_topic_by_time_ts
# or with TS disabled
# make create_topic_*_no_ts

# Fill the topic
make fill_topic

# See that segments are uploaded to the remote storage
# (needs AWS S3 command line tool installed and authenticated)
# (this may take several seconds)
aws s3 ls --recursive s3://<bucket_name>

# Check that early segments are deleted
# (completely or renamed with `.deleted` suffix)
# from the local storage (this may take several seconds)
make show_local_data

# Check the data is consumable
make consume
```

You can also see the remote data in https://s3.console.aws.amazon.com/s3/buckets/<bucket_name>.

### MinIO S3 as remote storage: `compose-s3-minio.yml`

This scenario uses `S3Storage` with MinIO S3 as the remote storage.

```bash
# Start the compose
make run_s3_minio

# Create the topic with any variation
make create_topic_by_size_ts
# or
# make create_topic_by_time_ts
# or with TS disabled
# make create_topic_*_no_ts

# Fill the topic
make fill_topic

# See that segments are uploaded to the remote storage
# (this may take several seconds)
make show_remote_data_s3_minio

# Check that early segments are deleted
# (completely or renamed with `.deleted` suffix)
# from the local storage (this may take several seconds)
make show_local_data

# Check the data is consumable
make consume
```

You can also see the remote data in http://localhost:9090/browser/test-bucket (login: `minioadmin`, password: `minioadmin`).

### fake-gcs-server as remote storage: `compose-gcs-fake-gcs-server.yml`

This scenario uses `GcsStorage` with [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) as the remote storage.

```bash
# Start the compose
make run_gcs_fake_gcs_server

# Create the topic with any variation
make create_topic_by_size_ts
# or
# make create_topic_by_time_ts
# or with TS disabled
# make create_topic_*_no_ts

# Fill the topic
make fill_topic

# See that segments are uploaded to the remote storage
# (this may take several seconds)
make show_remote_data_gcs_fake_gcs_server

# Check that early segments are deleted
# (completely or renamed with `.deleted` suffix)
# from the local storage (this may take several seconds)
make show_local_data

# Check the data is consumable
make consume
```

### Azurite as remote storage: `compose-azure-blob-azurite.yml`

This scenario uses `AzureBlobStorage` with [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite) as the remote storage.

```bash
# Start the compose
make run_azure_blob_azurite

# Create the topic with any variation
make create_topic_by_size_ts
# or
# make create_topic_by_time_ts
# or with TS disabled
# make create_topic_*_no_ts

# Fill the topic
make fill_topic

# See that segments are uploaded to the remote storage
# (this may take several seconds)
make show_remote_data_azurite

# Check that early segments are deleted
# (completely or renamed with `.deleted` suffix)
# from the local storage (this may take several seconds)
make show_local_data

# Check the data is consumable
make consume
```

## Additional features

### Encryption

Generate RSA key pair:

```shell
make rsa_keys
```

and set paths on `compose.yml` file:

```yaml
  kafka:
    # ...
    volumes:
      # ...
      - ./public.pem:/kafka/plugins/public.pem
      - ./private.pem:/kafka/plugins/private.pem
    environment:
      # ...
      KAFKA_RSM_CONFIG_STORAGE_ENCRYPTION_ENABLED: true
      KAFKA_RSM_CONFIG_STORAGE_ENCRYPTION_PUBLIC_KEY_FILE: /kafka/plugins/public.pem
      KAFKA_RSM_CONFIG_STORAGE_ENCRYPTION_PRIVATE_KEY_FILE: /kafka/plugins/private.pem
      # ...
```

### Metrics

Metrics are available as JMX MBeans, and exposed using [JMX Exporter](https://github.com/prometheus/jmx_exporter) at port `7001`

```shell
curl http://localhost:7001/metrics | grep kafka_tiered
```
