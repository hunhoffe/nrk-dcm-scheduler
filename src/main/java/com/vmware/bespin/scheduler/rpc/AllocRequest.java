/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

import com.vmware.bespin.rpc.Utils;

public class AllocRequest {
    public static final int BYTE_LEN = Long.BYTES * 3;

    final long application;
    final long cores;
    final long memslices;

    public AllocRequest(final byte application, final byte cores, final byte memslices) {
        this.application = application;
        this.cores = cores;
        this.memslices = memslices;
    }

    public AllocRequest(final byte[] data) {
        assert (data.length == AllocRequest.BYTE_LEN);

        this.application = Utils.bytesToLong(data, 0);
        this.cores = Utils.bytesToLong(data, Long.BYTES);
        this.memslices = Utils.bytesToLong(data, Long.BYTES * 2);
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[AllocRequest.BYTE_LEN];
        Utils.longToBytes(this.application, buff, 0);
        Utils.longToBytes(this.cores, buff, Long.BYTES);
        Utils.longToBytes(this.memslices, buff, Long.BYTES * 2);
	return buff;
    }

    @Override
    public String toString() {
        return "AllocRequest(app=" + this.application + ", cores=" + this.cores +
            ", memslices=" + this.memslices + ")";
    }
}
