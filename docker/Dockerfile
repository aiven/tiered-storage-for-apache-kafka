##
# Copyright 2023 Aiven Oy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##

# Base image based 3.8.0 + minor changes unrelated to TS included on this branch:
# https://github.com/apache/kafka/compare/3.8...aiven:kafka:3.8.1-2024-11-21
# See commits and Dockerfile for more details on base image
FROM docker.io/aivenoy/kafka:3.8.1-2024-11-21

ARG _VERSION

USER root

RUN mkdir -p /tiered-storage-for-apache-kafka/core \
    &&  mkdir -p /tiered-storage-for-apache-kafka/s3 \
    &&  mkdir -p /tiered-storage-for-apache-kafka/gcs \
    &&  mkdir -p /tiered-storage-for-apache-kafka/azure

COPY build/distributions/tiered-storage-for-apache-kafka-${_VERSION}.tgz /tiered-storage-for-apache-kafka/core
RUN cd /tiered-storage-for-apache-kafka/core \
    && tar -xf tiered-storage-for-apache-kafka-${_VERSION}.tgz --strip-components=1 \
    && rm tiered-storage-for-apache-kafka-${_VERSION}.tgz

COPY storage/s3/build/distributions/s3-${_VERSION}.tgz /tiered-storage-for-apache-kafka/s3
RUN cd /tiered-storage-for-apache-kafka/s3 \
    && tar -xf s3-${_VERSION}.tgz --strip-components=1 \
    && rm s3-${_VERSION}.tgz

COPY storage/gcs/build/distributions/gcs-${_VERSION}.tgz /tiered-storage-for-apache-kafka/gcs
RUN cd /tiered-storage-for-apache-kafka/gcs \
    && tar -xf gcs-${_VERSION}.tgz --strip-components=1 \
    && rm gcs-${_VERSION}.tgz

COPY storage/azure/build/distributions/azure-${_VERSION}.tgz /tiered-storage-for-apache-kafka/azure
RUN cd /tiered-storage-for-apache-kafka/azure \
    && tar -xf azure-${_VERSION}.tgz --strip-components=1 \
    && rm azure-${_VERSION}.tgz

# Installing JMX exporter agent
ARG JMX_EXPORTER_VERSION=1.0.1
RUN mkdir -p /opt/prometheus/jmx-exporter
RUN wget https://repo.maven.apache.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
    -O /opt/prometheus/jmx-exporter/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar
COPY docker/kafka-jmx-exporter.yml /opt/prometheus/jmx-exporter/.
ENV KAFKA_OPTS="-javaagent:/opt/prometheus/jmx-exporter/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar=7001:/opt/prometheus/jmx-exporter/kafka-jmx-exporter.yml"

# Restore the user.
USER appuser

RUN mkdir /home/appuser/kafka-tiered-storage-cache
