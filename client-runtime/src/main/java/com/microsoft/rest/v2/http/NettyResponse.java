/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.rest.v2.http;

import com.google.common.base.Charsets;
import com.microsoft.rest.v2.util.FlowableUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.io.InputStream;
import java.nio.channels.Channel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map.Entry;

/**
 * A HttpResponse that is implemented using Netty.
 */
class NettyResponse extends HttpResponse {
    private static final String HEADER_CONTENT_LENGTH = "Content-Length";
    private final io.netty.handler.codec.http.HttpResponse rxnRes;
    private final long contentLength;
    private final Flowable<ByteBuf> contentStream;
    private boolean isSubscribed = false;

    NettyResponse(io.netty.handler.codec.http.HttpResponse nettyRes, Flowable<ByteBuf> emitter) {
        this.rxnRes = nettyRes;
        this.contentLength = getContentLength(nettyRes);
        this.contentStream = emitter;
    }

    private static long getContentLength(io.netty.handler.codec.http.HttpResponse nettyRes) {
        long result;
        try {
            result = Long.parseLong(nettyRes.headers().get(HEADER_CONTENT_LENGTH));
        } catch (NullPointerException | NumberFormatException e) {
            result = 0;
        }
        return result;
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

    @Override
    public Single<byte[]> bodyAsByteArrayAsync() {
        return FlowableUtil.collectBytes(streamBodyAsync());
    }

    private static byte[] toByteArray(ByteBuf byteBuf) {
        if (byteBuf.hasArray()) {
            return byteBuf.array();
        } else {
            byte[] res = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(res);
            byteBuf.release();
            return res;
        }
    }

    @Override
    public Flowable<byte[]> streamBodyAsync() {
        isSubscribed = true;
        return contentStream.map(new Function<ByteBuf, byte[]>() {
            @Override
            public byte[] apply(ByteBuf byteBuf) {
                return toByteArray(byteBuf);
            }
        });
    }

    @Override
    public Single<String> bodyAsStringAsync() {
        isSubscribed = true;
        return contentStream.toList().map(new Function<List<ByteBuf>, String>() {
            @Override
            public String apply(List<ByteBuf> l) {
                ByteBuf[] bufs = new ByteBuf[l.size()];
                String result = Unpooled.wrappedBuffer(l.toArray(bufs)).toString(Charsets.UTF_8);
                for (ByteBuf buf : bufs) {
                    buf.release();
                }
                return result;
            }
        });
    }

    @Override
    public void close() {
        if (!isSubscribed) {
            isSubscribed = true;
            streamBodyAsync().subscribe().dispose();
        }
    }
}
