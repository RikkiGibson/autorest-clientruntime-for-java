/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.rest.v2.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

/**
 * A wrapper around a java.nio.ByteBuffer which handles buffer pooling.
 */
public class PooledBuffer {
    static {
        System.setProperty("io.netty.leakDetection.acquireAndReleaseOnly", "true");
    }

    /**
     * Allocates a pooled buffer of a given size.
     * @param size the size of buffer to allocate.
     * @return the pooled buffer.
     */
    public static PooledBuffer allocate(int size) {
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.ioBuffer(size);
        return new PooledBuffer(byteBuf);
    }

    /**
     * Wraps a {@link ByteBuffer} in a pooled buffer.
     *
     * @param buffer the buffer to wrap
     * @return a {@link PooledBuffer} containing the given buffer
     */
    public static PooledBuffer wrap(ByteBuffer buffer) {
        return new PooledBuffer(Unpooled.wrappedBuffer(buffer));
    }

    /**
     * Wraps a byte[] in a pooled buffer.
     * @param bytes the byte array to wrap
     * @return a {@link PooledBuffer} containing the given byte array
     */
    public static PooledBuffer wrap(byte[] bytes) {
        return new PooledBuffer(Unpooled.wrappedBuffer(bytes));
    }

    private final ByteBuf byteBuf;

    PooledBuffer(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }

    /**
     * Returns a ByteBuffer which provides a view into the same memory as this PooledBuffer.
     * Modifications to the returned ByteBuffer require calling sync(ByteBuffer) to make the PooledBuffer aware of the recently read or written content.
     *
     * @return a view of the PooledBuffer's memory.
     */
    public ByteBuffer byteBuffer() {
        ByteBuffer nioBuffer = byteBuf.nioBuffer(byteBuf.readerIndex(), byteBuf.capacity());
        return nioBuffer;
    }

    /**
     * Synchronizes the read and write positions of this PooledBuffer with a {@link ByteBuffer} that references the same underlying memory.
     *
     * @param view the byte buffer
     */
    public void sync(ByteBuffer view) {
        byteBuf.writerIndex(view.position());
    }

    /**
     * Releases the memory contained in this PooledBuffer.
     */
    public void release() {
        byteBuf.release();
    }

    /**
     * Internal use only for giving to Netty.
     */
    ByteBuf byteBuf() {
        return byteBuf;
    }
}
