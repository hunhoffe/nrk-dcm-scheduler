/*
 * Copyright 2022 University of Colorado. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

import com.vmware.bespin.rpc.Utils;

public class ReleaseRequest {
    public static final int BYTE_LEN = Long.BYTES * 4;

    final long nodeId;
    final long application;
    final long cores;
    final long memslices;

    public ReleaseRequest(final byte nodeId, final byte application, final byte cores, final byte memslices) {
        this.nodeId = nodeId;
        this.application = application;
        this.cores = cores;
        this.memslices = memslices;
    }

    public ReleaseRequest(final byte[] data) {
        assert (data.length == ReleaseRequest.BYTE_LEN);

        this.nodeId = Utils.bytesToLong(data, 0);
        this.application = Utils.bytesToLong(data, Long.BYTES);
        this.cores = Utils.bytesToLong(data, Long.BYTES * 2);
        this.memslices = Utils.bytesToLong(data, Long.BYTES * 3);
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[ReleaseRequest.BYTE_LEN];
        Utils.longToBytes(this.nodeId, buff, 0);
        Utils.longToBytes(this.application, buff, Long.BYTES);
        Utils.longToBytes(this.cores, buff, Long.BYTES * 2);
        Utils.longToBytes(this.memslices, buff, Long.BYTES * 3);
        return buff;
    }

    @Override
    public String toString() {
        return "ReleaseRequest(nodeId=" + this.nodeId + ", app=" + this.application + ", cores=" + this.cores +
                ", memslices=" + this.memslices + ")";
    }
}
