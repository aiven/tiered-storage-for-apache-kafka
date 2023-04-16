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

package io.aiven.kafka.tieredstorage.commons.storage;

import java.io.IOException;
import java.io.InputStream;

public interface FileFetcher {
    /**
     * Fetch file.
     * @param key file key.
     */
    InputStream fetch(String key) throws IOException;

    /**
     * Fetch file.
     * @param key file key.
     * @param from start position, inclusive.
     * @param to end position, inclusive.
     */
    InputStream fetch(String key, int from, int to) throws IOException;
}