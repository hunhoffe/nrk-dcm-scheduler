/*
 * Copyright 2022 University of Colorado. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

import com.vmware.bespin.rpc.Utils;

public class ReleaseResponse {
    public static final int BYTE_LEN = Long.BYTES;

    final long isSuccess;

    public ReleaseResponse(final long isSuccess) {
        this.isSuccess = isSuccess;
    }

    public ReleaseResponse(final byte[] data) {
        assert (data.length == ReleaseResponse.BYTE_LEN);
        this.isSuccess = Utils.bytesToLong(data, 0);
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[AllocResponse.BYTE_LEN];
        Utils.longToBytes(this.isSuccess, buff, 0);
        return buff;
    }
}