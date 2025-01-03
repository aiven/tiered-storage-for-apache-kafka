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
VERSION := $(shell grep -o -E '^version=[0-9]+\.[0-9]+\.[0-9]+(-SNAPSHOT)?' gradle.properties | cut -c9-)
IMAGE_NAME := aivenoy/kafka-with-ts-plugin
IMAGE_VERSION := latest
IMAGE_TAG := $(IMAGE_NAME):$(IMAGE_VERSION)

.PHONY: all clean checkstyle build test integration_test e2e_test docker_image docker_push

all: clean build test

clean:
	./gradlew clean

checkstyle:
	./gradlew checkstyleMain checkstyleTest checkstyleIntegrationTest

build: build/distributions/tiered-storage-for-apache-kafka-$(VERSION).tgz storage/s3/build/distributions/s3-$(VERSION).tgz storage/gcs/build/distributions/gcs-$(VERSION).tgz storage/azure/build/distributions/azure-$(VERSION).tgz

build/distributions/tiered-storage-for-apache-kafka-$(VERSION).tgz:
	./gradlew build distTar -x test -x integrationTest

storage/s3/build/distributions/s3-$(VERSION).tgz:
	./gradlew build :storage:s3:distTar -x test -x integrationTest

storage/gcs/build/distributions/gcs-$(VERSION).tgz:
	./gradlew build :storage:gcs:distTar -x test -x integrationTest

storage/azure/build/distributions/azure-$(VERSION).tgz:
	./gradlew build :storage:azure:distTar -x test -x integrationTest

.PHONY: docs
docs:
	./gradlew :docs:genConfigsDocs
	./gradlew :docs:genMetricsDocs

test: build
	./gradlew test

integration_test: build
	./gradlew integrationTest -x :e2e:integrationTest

E2E_TEST=LocalSystem

e2e_test: build
	./gradlew :e2e:integrationTest --tests $(E2E_TEST)*

.PHONY: docker_image
docker_image: build
	docker build . \
		-f docker/Dockerfile \
		--build-arg _VERSION=$(VERSION) \
		-t $(IMAGE_TAG)

.PHONY: docker_push
docker_push:
	docker push $(IMAGE_TAG)
