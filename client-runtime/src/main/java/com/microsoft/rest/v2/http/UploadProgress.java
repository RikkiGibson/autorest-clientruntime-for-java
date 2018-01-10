/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.rest.v2.http;

/**
 * Indicates the progress of an upload.
 */
public class UploadProgress {
    private final long bytesUploaded;
    private final long totalBytes;
    private final String url;

    /**
     * @return the number of request body bytes that have been uploaded so far
     */
    public long bytesUploaded() {
        return bytesUploaded;
    }

    /**
     * @return the total number of bytes to upload
     */
    public long totalBytes() {
        return totalBytes;
    }

    /**
     * @return the url the bytes are being uploaded to
     */
    public String url() {
        return url;
    }

    /**
     * Create an UploadProgress instance.
     * @param bytesUploaded the number of bytes uploaded so far
     * @param totalBytes the total number of bytes to upload
     * @param url the url being uploaded to
     */
    public UploadProgress(long bytesUploaded, long totalBytes, String url) {
        this.bytesUploaded = bytesUploaded;
        this.totalBytes = totalBytes;
        this.url = url;
    }
}
