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

package io.aiven.kafka.tieredstorage.commons.transform;

import java.io.ByteArrayInputStream;
import java.util.function.Function;

import io.aiven.kafka.tieredstorage.commons.manifest.index.ChunkIndex;

public final class OutboundTransformChain {
    DetransformChunkEnumeration inner;

    OutboundTransformChain(final byte[] uploadedData, final ChunkIndex chunkIndex) {
        this.inner = new BaseDetransformChunkEnumeration(new ByteArrayInputStream(uploadedData), chunkIndex.chunks());
    }

    public void chain(final Function<DetransformChunkEnumeration, DetransformChunkEnumeration> transform) {
        this.inner = transform.apply(this.inner);
    }

    public DetransformFinisher complete() {
        return new DetransformFinisher(inner);
    }
}
