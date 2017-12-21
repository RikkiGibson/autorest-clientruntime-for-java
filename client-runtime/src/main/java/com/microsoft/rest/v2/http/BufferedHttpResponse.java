/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.rest.v2.http;

import com.google.common.base.Charsets;
import com.microsoft.rest.v2.util.FlowableUtil;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * HTTP response which will buffer the response's body when/if it is read.
 */
public final class BufferedHttpResponse extends HttpResponse {
    private final HttpResponse innerHttpResponse;
    private final Single<byte[]> body;

    /**
     * Creates a buffered HTTP response.
     * @param innerHttpResponse The HTTP response to buffer.
     */
    public BufferedHttpResponse(HttpResponse innerHttpResponse) {
        this.innerHttpResponse = innerHttpResponse;
        this.body = FlowableUtil.collectBytes(innerHttpResponse.streamBodyAsync().replay().autoConnect());
    }

    @Override
    public int statusCode() {
        return innerHttpResponse.statusCode();
    }

    @Override
    public String headerValue(String headerName) {
        return innerHttpResponse.headerValue(headerName);
    }

    @Override
    public HttpHeaders headers() {
        return innerHttpResponse.headers();
    }

    @Override
    public Single<byte[]> bodyAsByteArrayAsync() {
        return body;
    }

    @Override
    public Flowable<byte[]> streamBodyAsync() {
        return body.toFlowable();
    }

    @Override
    public Single<String> bodyAsStringAsync() {
        return body.map(new Function<byte[], String>() {
            @Override
            public String apply(byte[] bytes) throws Exception {
                return new String(bytes, Charsets.UTF_8);
            }
        });
    }

    @Override
    public BufferedHttpResponse buffer() {
        return this;
    }

    @Override
    public void close() {
        innerHttpResponse.close();
    }
}
