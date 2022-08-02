/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

public class RegisterNodeRequest {
    public static final int BYTE_LEN = 2;

    final byte cores;
    final byte memslices;

    public RegisterNodeRequest(final byte cores, final byte memslices) {
        this.cores = cores;
        this.memslices = memslices;
    }

    public RegisterNodeRequest(final byte[] data) {
        assert (data.length == RegisterNodeRequest.BYTE_LEN);

        this.cores = data[0];
        this.memslices = data[1];
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[RegisterNodeRequest.BYTE_LEN];
        buff[0] = this.cores;
        buff[1] = this.memslices;
        return buff;
    }
}