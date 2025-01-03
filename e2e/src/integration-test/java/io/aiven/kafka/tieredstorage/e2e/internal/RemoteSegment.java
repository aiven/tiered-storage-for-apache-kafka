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

package io.aiven.kafka.tieredstorage.e2e.internal;

import java.util.Objects;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;

public final class RemoteSegment {
    private final RemoteLogSegmentId remoteLogSegmentId;
    private final long startOffset;
    private final long endOffset;

    RemoteSegment(final RemoteLogSegmentId remoteLogSegmentId,
                  final long startOffset,
                  final long endOffset) {
        this.remoteLogSegmentId = remoteLogSegmentId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public RemoteLogSegmentId remoteLogSegmentId() {
        return remoteLogSegmentId;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        final var that = (RemoteSegment) obj;
        return Objects.equals(this.remoteLogSegmentId, that.remoteLogSegmentId)
            && this.startOffset == that.startOffset
            && this.endOffset == that.endOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegmentId, startOffset, endOffset);
    }

    @Override
    public String toString() {
        return "RemoteSegment["
            + "remoteLogSegmentId=" + remoteLogSegmentId + ", "
            + "startOffset=" + startOffset + ", "
            + "endOffset=" + endOffset + ']';
    }

}
