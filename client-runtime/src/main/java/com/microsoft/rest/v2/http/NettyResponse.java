/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.rest.v2.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.internal.functions.Functions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map.Entry;

/**
 * A HttpResponse that is implemented using Netty.
 */
class NettyResponse extends HttpResponse {
    private final io.netty.handler.codec.http.HttpResponse rxnRes;
    private final Flowable<ByteBuffer> contentStream;

    NettyResponse(io.netty.handler.codec.http.HttpResponse rxnRes, Flowable<ByteBuffer> emitter) {
        this.rxnRes = rxnRes;
        this.contentStream = emitter;
    }

    @Override
    public int statusCode() {
        return rxnRes.status().code();
    }

    @Override
    public String headerValue(String headerName) {
        return rxnRes.headers().get(headerName);
    }

    @Override
    public HttpHeaders headers() {
        HttpHeaders headers = new HttpHeaders();
        for (Entry<String, String> header : rxnRes.headers()) {
            headers.set(header.getKey(), header.getValue());
        }
        return headers;
    }

    private Single<ByteBuf> collectContent() {
        ByteBuf allContent = null;
        String contentLengthString = headerValue("Content-Length");
        if (contentLengthString != null) {
            try {
                int contentLength = Integer.parseInt(contentLengthString);
                allContent = Unpooled.buffer(contentLength);
            } catch (NumberFormatException ignored) {
            }
        }

        if (allContent == null) {
            allContent = Unpooled.buffer();
        }

        return contentStream.collectInto(allContent, ByteBuf::writeBytes);
    }

    @Override
    public Single<byte[]> bodyAsByteArrayAsync() {
        return collectContent().map(byteBuf -> {
            byte[] result;
            if (byteBuf.readableBytes() == byteBuf.array().length) {
                result = byteBuf.array();
            } else {
                byte[] dst = new byte[byteBuf.readableBytes()];
                byteBuf.readBytes(dst);
                result = dst;
            }

            // This byteBuf is not pooled but Netty uses ref counting to track allocation metrics
            byteBuf.release();
            return result;
        });
    }

    @Override
    public Flowable<ByteBuffer> streamBodyAsync() {
        return contentStream;
    }

    @Override
    public Single<String> bodyAsStringAsync() {
        return collectContent().map(byteBuf -> {
            String result = byteBuf.toString(StandardCharsets.UTF_8);
            byteBuf.release();
            return result;
        });
    }

    @Override
    public void close() {
        contentStream.subscribe(Functions.emptyConsumer(), Functions.emptyConsumer()).dispose();
    }
}
