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
package io.aiven.kafka.tieredstorage.util;

import java.io.InputStream;
import java.util.Enumeration;
import io.aiven.kafka.tieredstorage.transform.RateLimitedInputStream;
import io.github.bucket4j.Bucket;

public abstract class RemoteEnumeration implements Enumeration<InputStream> {

	protected final Bucket rateLimitingBucket;

	public RemoteEnumeration(final Bucket rateLimitingBucket) {
		this.rateLimitingBucket = rateLimitingBucket;
	}

	protected InputStream maybeToRateLimitedInputStream(final InputStream delegated) {
		if (rateLimitingBucket == null) {
			return delegated;
		}
		return new RateLimitedInputStream(delegated, rateLimitingBucket);
	}

}
