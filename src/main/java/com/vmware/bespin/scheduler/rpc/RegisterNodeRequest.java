/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

import com.vmware.bespin.rpc.Utils;

public class RegisterNodeRequest {
    public static final int BYTE_LEN = Long.BYTES * 2;

    final long cores;
    final long memslices;

    public RegisterNodeRequest(final byte cores, final byte memslices) {
        this.cores = cores;
        this.memslices = memslices;
    }

    public RegisterNodeRequest(final byte[] data) {
        assert (data.length == RegisterNodeRequest.BYTE_LEN);
        this.cores = Utils.bytesToLong(data, 0);
        this.memslices = Utils.bytesToLong(data, Long.BYTES);
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[RegisterNodeRequest.BYTE_LEN];
        Utils.longToBytes(this.cores, buff, 0);
        Utils.longToBytes(this.memslices, buff, Long.BYTES);
	return buff;
    }

    @Override
    public String toString() {
        return "RegisterNodeRequest(cores=" + this.cores + ", memslices=" + this.memslices + ")";
    }
}
