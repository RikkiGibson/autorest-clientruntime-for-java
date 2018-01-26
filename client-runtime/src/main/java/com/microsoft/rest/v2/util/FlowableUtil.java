/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.rest.v2.util;

import com.google.common.reflect.TypeToken;
import com.microsoft.rest.v2.http.PooledBuffer;
import io.reactivex.Completable;
import io.reactivex.CompletableEmitter;
import io.reactivex.CompletableOnSubscribe;
import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.Single;
import io.reactivex.internal.util.BackpressureHelper;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Contains helper methods for dealing with Flowables.
 */
public class FlowableUtil {
    /**
     * Checks if a type is Flowable&lt;PooledBuffer&gt;.
     *
     * @param entityTypeToken the type to check
     * @return whether the type represents a Flowable that emits byte arrays
     */
    public static boolean isFlowablePooledBuffer(TypeToken entityTypeToken) {
        if (entityTypeToken.isSubtypeOf(Flowable.class)) {
            final Type innerType = ((ParameterizedType) entityTypeToken.getType()).getActualTypeArguments()[0];
            final TypeToken innerTypeToken = TypeToken.of(innerType);
            if (innerTypeToken.isSubtypeOf(PooledBuffer.class)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if a type is Flowable&lt;ByteBuffer&gt;.
     *
     * @param entityTypeToken the type to check
     * @return whether the type represents a Flowable that emits byte arrays
     */
    public static boolean isFlowableByteBuffer(TypeToken entityTypeToken) {
        if (entityTypeToken.isSubtypeOf(Flowable.class)) {
            final Type innerType = ((ParameterizedType) entityTypeToken.getType()).getActualTypeArguments()[0];
            final TypeToken innerTypeToken = TypeToken.of(innerType);
            if (innerTypeToken.isSubtypeOf(ByteBuffer.class)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Collects pooled buffers emitted by a Flowable into a Single. Does not free the buffers.
     * @param content A stream which emits byte arrays.
     * @return A Single which emits the concatenation of all the byte arrays given by the source Flowable.
     */
    public static Single<byte[]> collectBytes(Flowable<ByteBuffer> content) {
        throw new Error();

//        return content.collectInto(ByteStreams.newDataOutput(), new BiConsumer<ByteArrayDataOutput, PooledBuffer>() {
//            @Override
//            public void accept(ByteArrayDataOutput out, PooledBuffer chunk) throws Exception {
//                // FIXME reduce copying
//                byte[] arrayChunk;
//                if (chunk.byteBuffer().hasArray()) {
//                    arrayChunk = chunk.byteBuffer().array();
//                } else {
//                    arrayChunk = new byte[chunk.byteBuffer().position()];
//                    chunk.byteBuffer().get(arrayChunk);
//                }
//                out.write(arrayChunk);
//            }
//        }).map(new Function<ByteArrayDataOutput, byte[]>() {
//            @Override
//            public byte[] apply(ByteArrayDataOutput out) throws Exception {
//                return out.toByteArray();
//            }
//        });
    }

    /**
     * Writes the bytes emitted by a Flowable to an AsynchronousFileChannel.
     * @param content the Flowable content
     * @param fileChannel the file channel
     * @return a Completable which performs the write operation when subscribed
     */
    public static Completable writeFile(final Flowable<PooledBuffer> content, final AsynchronousFileChannel fileChannel) {
        return Completable.create(new CompletableOnSubscribe() {
            @Override
            public void subscribe(final CompletableEmitter emitter) throws Exception {
                content.subscribe(new FlowableSubscriber<PooledBuffer>() {
                    // volatile ensures that writes to these fields by one thread will be immediately visible to other threads.
                    // An I/O pool thread will write to isWriting and read isCompleted,
                    // while another thread may read isWriting and write to isCompleted.
                    volatile boolean isWriting = false;
                    volatile boolean isCompleted = false;
                    volatile Subscription subscription;
                    volatile long position = 0;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        s.request(1);
                    }

                    @Override
                    public void onNext(PooledBuffer bytes) {
                        isWriting = true;
                        fileChannel.write(bytes.byteBuffer(), position, bytes, onWriteCompleted);
                    }


                    CompletionHandler<Integer, PooledBuffer> onWriteCompleted = new CompletionHandler<Integer, PooledBuffer>() {
                        @Override
                        public void completed(Integer bytesRead, PooledBuffer attachment) {
                            // TODO: return Flowable<PooledBuffer> so that user can control releasing.
                            attachment.release();
                            isWriting = false;
                            if (isCompleted) {
                                emitter.onComplete();
                            }
                            //noinspection NonAtomicOperationOnVolatileField
                            position += bytesRead;
                            subscription.request(1);
                        }

                        @Override
                        public void failed(Throwable exc, PooledBuffer attachment) {
                            attachment.release();
                            subscription.cancel();
                            emitter.onError(exc);
                        }
                    };

                    @Override
                    public void onError(Throwable throwable) {
                        subscription.cancel();
                        emitter.onError(throwable);
                    }

                    @Override
                    public void onComplete() {
                        isCompleted = true;
                        if (!isWriting) {
                            emitter.onComplete();
                        }
                    }
                });
            }
        });
    }

    /**
     * Creates a Flowable emitting ByteBuffer from an AsynchronousFileChannel.
     *
     * @param fileChannel The file channel.
     * @param offset The offset in the file to begin reading.
     * @param length The number of bytes of data to read from the file.
     * @return The AsyncInputStream.
     */
    public static Flowable<ByteBuffer> readFile(final AsynchronousFileChannel fileChannel, final long offset, final long length) {
        Flowable<ByteBuffer> fileStream = new FileReadFlowable(fileChannel, offset, length);
        return fileStream;
    }

    /**
     * Creates a {@link Flowable} from an {@link AsynchronousFileChannel} which reads the entire file.
     * @param fileChannel The file channel.
     * @throws IOException if an error occurs when determining file size
     * @return The AsyncInputStream.
     */
    public static Flowable<ByteBuffer> readFile(AsynchronousFileChannel fileChannel) throws IOException {
        long size = fileChannel.size();
        return readFile(fileChannel, 0, size);
    }

    private static final int CHUNK_SIZE = 8192;
    private static class FileReadFlowable extends Flowable<ByteBuffer> {
        private final AsynchronousFileChannel fileChannel;
        private final long offset;
        private final long length;
        private final ByteBuffer dst = ByteBuffer.allocateDirect(CHUNK_SIZE);

        FileReadFlowable(AsynchronousFileChannel fileChannel, long offset, long length) {
            this.fileChannel = fileChannel;
            this.offset = offset;
            this.length = length;
        }

        @Override
        protected void subscribeActual(Subscriber<? super ByteBuffer> s) {
            s.onSubscribe(new FileReadSubscription(s));
        }

        private class FileReadSubscription implements Subscription {
            final Subscriber<? super ByteBuffer> subscriber;
            final AtomicLong requested = new AtomicLong();
            volatile boolean cancelled = false;

            // I/O callbacks are serialized, but not guaranteed to happen on the same thread, which makes volatile necessary.
            volatile long position = offset;

            FileReadSubscription(Subscriber<? super ByteBuffer> subscriber) {
                this.subscriber = subscriber;
            }

            @Override
            public void request(long n) {
                if (BackpressureHelper.add(requested, n) == 0L) {
                    doRead();
                }
            }

            void doRead() {
                dst.clear();
                fileChannel.read(dst, position, null, onReadComplete);
            }

            private final CompletionHandler<Integer, Object> onReadComplete = new CompletionHandler<Integer, Object>() {
                @Override
                public void completed(Integer bytesRead, Object attachment) {
                    if (!cancelled) {
                        if (bytesRead == -1) {
                            subscriber.onComplete();
                        } else {
                            int bytesWanted = (int) Math.min(bytesRead, offset + length - position);
                            //noinspection NonAtomicOperationOnVolatileField
                            position += bytesWanted;

                            dst.flip();
                            subscriber.onNext(dst);
                            if (position >= offset + length) {
                                subscriber.onComplete();
                            } else if (requested.decrementAndGet() > 0) {
                                doRead();
                            }
                        }
                    }
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    if (!cancelled) {
                        subscriber.onError(exc);
                    }
                }
            };

            @Override
            public void cancel() {
                cancelled = true;
            }
        }
    }
}
