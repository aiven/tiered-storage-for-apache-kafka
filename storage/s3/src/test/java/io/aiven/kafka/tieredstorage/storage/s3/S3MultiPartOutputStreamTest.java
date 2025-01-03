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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3MultiPartOutputStreamTest {
    private static final String BUCKET_NAME = "some_bucket";
    private static final ObjectKey FILE_KEY = new TestObjectKey("some_key");
    private static final String UPLOAD_ID = "some_upload_id";

    @Mock
    S3Client mockedS3;

    @Captor
    ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartUploadRequestCaptor;
    @Captor
    ArgumentCaptor<AbortMultipartUploadRequest> abortMultipartUploadRequestCaptor;
    @Captor
    ArgumentCaptor<UploadPartRequest> uploadPartRequestCaptor;
    @Captor
    ArgumentCaptor<RequestBody> requestBodyCaptor;

    final Random random = new Random();

    @BeforeEach
    void setUp() {
        when(mockedS3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());
    }

    @Test
    void sendAbortForAnyExceptionWhileWriting() {
        final RuntimeException testException = new RuntimeException("test");
        when(mockedS3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenThrow(testException);

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 1, mockedS3, false);
        assertThatThrownBy(() -> out.write(new byte[] {1, 2, 3}))
            .isInstanceOf(IOException.class)
            .hasRootCause(testException);

        assertThat(out.isClosed()).isTrue();
        // retry close to validate no exception is thrown and the number of calls to complete/upload does not change
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedS3).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedS3).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(mockedS3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void sendAbortForAnyExceptionWhenClosingUpload() throws Exception {
        when(mockedS3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenThrow(RuntimeException.class);

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 10, mockedS3, false);

        final byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        out.write(buffer, 0, buffer.length);

        assertThatThrownBy(out::close)
            .isInstanceOf(IOException.class)
            .rootCause()
            .isInstanceOf(RuntimeException.class);

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedS3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void sendAbortForAnyExceptionWhenClosingComplete() throws Exception {
        when(mockedS3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenReturn(newUploadPartResponse("SOME_ETAG#1"));
        when(mockedS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenThrow(RuntimeException.class);

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 10, mockedS3, false);

        final byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        out.write(buffer, 0, buffer.length);

        assertThatThrownBy(out::close)
            .isInstanceOf(IOException.class)
            .hasRootCauseInstanceOf(RuntimeException.class);

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedS3).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(mockedS3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void writesOneByte() throws Exception {
        when(mockedS3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenReturn(newUploadPartResponse("SOME_ETAG"));
        when(mockedS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().eTag("SOME_ETAG").build());

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 100, mockedS3, false);
        out.write(1);
        out.close();

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedS3).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedS3).uploadPart(uploadPartRequestCaptor.capture(), requestBodyCaptor.capture());
        verify(mockedS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        assertUploadPartRequest(
            uploadPartRequestCaptor.getValue(),
            requestBodyCaptor.getValue(),
            1,
            1,
            new byte[] {1}
        );
        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            List.of(CompletedPart.builder().partNumber(1).eTag("SOME_ETAG").build())
        );
    }

    @Test
    void writesMultipleMessages() throws Exception {
        final int bufferSize = 10;

        final List<UploadPartRequest> uploadPartRequests = new ArrayList<>();
        final List<RequestBody> requestBodies = new ArrayList<>();
        when(mockedS3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenAnswer(invocation -> {
                final UploadPartRequest upload = invocation.getArgument(0);
                final RequestBody originalBody = invocation.getArgument(1);
                //emulate the behavior of S3 client otherwise we will get a wrong array in the memory
                try (final InputStream inputStream = originalBody.contentStreamProvider().newStream()) {
                    final RequestBody requestBody = RequestBody.fromBytes(inputStream.readAllBytes());
                    requestBodies.add(requestBody);
                }
                uploadPartRequests.add(upload);

                return newUploadPartResponse("SOME_ETAG#" + upload.partNumber());
            });
        when(mockedS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().build());

        final List<byte[]> expectedMessagesList = new ArrayList<>();
        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, bufferSize, mockedS3, false);
        for (int i = 0; i < 3; i++) {
            final byte[] message = new byte[bufferSize];
            random.nextBytes(message);
            out.write(message, 0, message.length);
            expectedMessagesList.add(message);
        }
        out.close();

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedS3).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedS3, times(3)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(mockedS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        int counter = 0;
        for (final byte[] expectedMessage : expectedMessagesList) {
            assertUploadPartRequest(
                uploadPartRequests.get(counter),
                requestBodies.get(counter),
                bufferSize,
                counter + 1,
                expectedMessage);
            counter++;
        }
        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            List.of(
                CompletedPart.builder().partNumber(1).eTag("SOME_ETAG#1").build(),
                CompletedPart.builder().partNumber(2).eTag("SOME_ETAG#2").build(),
                CompletedPart.builder().partNumber(3).eTag("SOME_ETAG#3").build()
            )
        );
    }

    @Test
    void writesTailMessages() throws Exception {
        final int messageSize = 20;

        final List<UploadPartRequest> uploadPartRequests = new ArrayList<>();
        final List<RequestBody> requestBodies = new ArrayList<>();

        when(mockedS3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenAnswer(invocation -> {
                final UploadPartRequest upload = invocation.getArgument(0);
                final RequestBody originalBody = invocation.getArgument(1);
                //emulate the behavior of S3 client otherwise we will get a wrong array in the memory
                try (final InputStream inputStream = originalBody.contentStreamProvider().newStream()) {
                    final RequestBody requestBody = RequestBody.fromBytes(inputStream.readAllBytes());
                    requestBodies.add(requestBody);
                }
                uploadPartRequests.add(upload);

                return newUploadPartResponse("SOME_ETAG#" + upload.partNumber());
            });
        when(mockedS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().build());
        final byte[] expectedFullMessage = new byte[messageSize + 10];
        final byte[] expectedTailMessage = new byte[10];

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, messageSize + 10, mockedS3, false);
        final byte[] message = new byte[messageSize];
        random.nextBytes(message);
        out.write(message);
        System.arraycopy(message, 0, expectedFullMessage, 0, message.length);
        random.nextBytes(message);
        out.write(message);
        System.arraycopy(message, 0, expectedFullMessage, 20, 10);
        System.arraycopy(message, 10, expectedTailMessage, 0, 10);
        out.close();

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        assertUploadPartRequest(uploadPartRequests.get(0), requestBodies.get(0), 30, 1, expectedFullMessage);
        assertUploadPartRequest(uploadPartRequests.get(1), requestBodies.get(1), 10, 2, expectedTailMessage);

        verify(mockedS3).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedS3, times(2)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(mockedS3, times(1)).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());
        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            List.of(
                CompletedPart.builder().partNumber(1).eTag("SOME_ETAG#1").build(),
                CompletedPart.builder().partNumber(2).eTag("SOME_ETAG#2").build()
            )
        );
    }

    @Test
    void writesTailMessagesFromInputStreamBufferSmallerThanSize() throws Exception {
        final int messageSize = 10;

        final List<UploadPartRequest> uploadPartRequests = new ArrayList<>();
        final List<RequestBody> requestBodies = new ArrayList<>();

        when(mockedS3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenAnswer(invocation -> {
                final UploadPartRequest upload = invocation.getArgument(0);
                final RequestBody originalBody = invocation.getArgument(1);
                //emulate the behavior of S3 client otherwise we will get a wrong array in the memory
                try (final InputStream inputStream = originalBody.contentStreamProvider().newStream()) {
                    final RequestBody requestBody = RequestBody.fromBytes(inputStream.readAllBytes());
                    requestBodies.add(requestBody);
                }
                uploadPartRequests.add(upload);

                return newUploadPartResponse("SOME_ETAG#" + upload.partNumber());
            });
        when(mockedS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().build());
        final byte[] expectedFullMessage0 = new byte[8];
        final byte[] expectedFullMessage1 = new byte[8];
        final byte[] expectedTailMessage = new byte[4];

        final byte[] message0 = new byte[messageSize];
        random.nextBytes(message0);
        System.arraycopy(message0, 0, expectedFullMessage0, 0, 8);
        System.arraycopy(message0, 8, expectedFullMessage1, 0, 2);
        final byte[] message1 = new byte[messageSize];
        random.nextBytes(message1);
        System.arraycopy(message1, 0, expectedFullMessage1, 2, 6);
        System.arraycopy(message1, 6, expectedTailMessage, 0, 4);
        final var in = new SequenceInputStream(new ByteArrayInputStream(message0), new ByteArrayInputStream(message1));
        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 8, mockedS3);
        try (in; out) {
            in.transferTo(out);
        }

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        assertUploadPartRequest(uploadPartRequests.get(0), requestBodies.get(0), 8, 1, expectedFullMessage0);
        assertUploadPartRequest(uploadPartRequests.get(1), requestBodies.get(1), 8, 2, expectedFullMessage1);
        assertUploadPartRequest(uploadPartRequests.get(2), requestBodies.get(2), 4, 3, expectedTailMessage);

        verify(mockedS3).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedS3, times(3)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(mockedS3, times(1)).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());
        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            List.of(
                CompletedPart.builder().partNumber(1).eTag("SOME_ETAG#1").build(),
                CompletedPart.builder().partNumber(2).eTag("SOME_ETAG#2").build(),
                CompletedPart.builder().partNumber(3).eTag("SOME_ETAG#3").build()
            )
        );
    }

    @Test
    void writesTailMessagesFromInputStreamSizeSmallerThanBuffer() throws Exception {
        final int messageSize = 10;

        final List<UploadPartRequest> uploadPartRequests = new ArrayList<>();
        final List<RequestBody> requestBodies = new ArrayList<>();

        when(mockedS3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenAnswer(invocation -> {
                final UploadPartRequest upload = invocation.getArgument(0);
                final RequestBody originalBody = invocation.getArgument(1);
                //emulate the behavior of S3 client otherwise we will get a wrong array in the memory
                try (final InputStream inputStream = originalBody.contentStreamProvider().newStream()) {
                    final RequestBody requestBody = RequestBody.fromBytes(inputStream.readAllBytes());
                    requestBodies.add(requestBody);
                }
                uploadPartRequests.add(upload);

                return newUploadPartResponse("SOME_ETAG#" + upload.partNumber());
            });
        when(mockedS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().build());
        final byte[] expectedFullMessage0 = new byte[12];
        final byte[] expectedTailMessage = new byte[8];

        final byte[] message0 = new byte[messageSize];
        random.nextBytes(message0);
        System.arraycopy(message0, 0, expectedFullMessage0, 0, 10);
        final byte[] message1 = new byte[messageSize];
        random.nextBytes(message1);
        System.arraycopy(message1, 0, expectedFullMessage0, 10, 2);
        System.arraycopy(message1, 2, expectedTailMessage, 0, 8);
        final var in = new SequenceInputStream(new ByteArrayInputStream(message0), new ByteArrayInputStream(message1));
        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 12, mockedS3);
        try (in; out) {
            in.transferTo(out);
        }

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        assertUploadPartRequest(uploadPartRequests.get(0), requestBodies.get(0), 12, 1, expectedFullMessage0);
        assertUploadPartRequest(uploadPartRequests.get(1), requestBodies.get(1), 8, 2, expectedTailMessage);

        verify(mockedS3).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedS3, times(2)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(mockedS3, times(1)).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());
        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            List.of(
                CompletedPart.builder().partNumber(1).eTag("SOME_ETAG#1").build(),
                CompletedPart.builder().partNumber(2).eTag("SOME_ETAG#2").build()
            )
        );
    }

    @Test
    void sendAbortIfNoWritingHappened() throws IOException {
        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 100, mockedS3, false);
        out.close();

        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());
        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();
    }

    @Test
    void failWhenUploadingPartAfterStreamIsClosed() throws IOException {
        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 100, mockedS3, false);
        out.close();

        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());
        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        assertThatThrownBy(() -> out.write(1))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Already closed");
    }

    private static CreateMultipartUploadResponse newInitiateMultipartUploadResult() {
        final CreateMultipartUploadResponse.Builder resultBuilder = CreateMultipartUploadResponse.builder();
        resultBuilder.uploadId(UPLOAD_ID);
        return resultBuilder.build();
    }

    private static UploadPartResponse newUploadPartResponse(final String etag) {
        return UploadPartResponse.builder().eTag(etag).build();
    }

    private static void assertUploadPartRequest(final UploadPartRequest uploadPartRequest,
                                                final RequestBody requestBody,
                                                final long expectedPartSize,
                                                final int expectedPartNumber,
                                                final byte[] expectedBytes) {
        assertThat(uploadPartRequest.uploadId()).isEqualTo(UPLOAD_ID);
        assertThat(uploadPartRequest.partNumber()).isEqualTo(expectedPartNumber);
        assertThat(uploadPartRequest.bucket()).isEqualTo(BUCKET_NAME);
        assertThat(uploadPartRequest.key()).isEqualTo(FILE_KEY.value());
        assertThat(requestBody.optionalContentLength()).hasValue(expectedPartSize);
        assertThat(requestBody.contentStreamProvider().newStream()).hasBinaryContent(expectedBytes);
    }

    private static void assertCompleteMultipartUploadRequest(final CompleteMultipartUploadRequest request,
                                                             final List<CompletedPart> expectedETags) {
        assertThat(request.bucket()).isEqualTo(BUCKET_NAME);
        assertThat(request.key()).isEqualTo(FILE_KEY.value());
        assertThat(request.uploadId()).isEqualTo(UPLOAD_ID);
        assertThat(request.multipartUpload().parts()).containsExactlyElementsOf(expectedETags);
    }

    private static void assertAbortMultipartUploadRequest(final AbortMultipartUploadRequest request) {
        assertThat(request.bucket()).isEqualTo(BUCKET_NAME);
        assertThat(request.key()).isEqualTo(FILE_KEY.value());
        assertThat(request.uploadId()).isEqualTo(UPLOAD_ID);
    }
}
