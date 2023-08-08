/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos.rpc;

import com.vmware.bespin.rpc.Utils;

public class RegisterNodeRequest {
    public static final int BYTE_LEN = Long.BYTES * 3;

    final long id;
    final long cores;
    final long memslices;

    public RegisterNodeRequest(final long id, final byte cores, final byte memslices) {
        this.id = id;
        this.cores = cores;
        this.memslices = memslices;
    }

    public RegisterNodeRequest(final byte[] data) {
        assert (data.length == RegisterNodeRequest.BYTE_LEN);
        this.id = Utils.bytesToLong(data, 0);
        this.cores = Utils.bytesToLong(data, Long.BYTES);
        this.memslices = Utils.bytesToLong(data, Long.BYTES * 2);
        
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[RegisterNodeRequest.BYTE_LEN];
        Utils.longToBytes(this.id, buff, 0);
        Utils.longToBytes(this.cores, buff, Long.BYTES);
        Utils.longToBytes(this.memslices, buff, Long.BYTES * 2);
    return buff;
    }

    @Override
    public String toString() {
        return "RegisterNodeRequest(id=" + this.id + ", cores=" + this.cores + ", memslices=" + this.memslices + ")";
    }
}
