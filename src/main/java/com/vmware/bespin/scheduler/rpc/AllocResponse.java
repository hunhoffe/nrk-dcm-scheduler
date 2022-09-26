/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

import com.vmware.bespin.rpc.Utils;

public class AllocResponse {
    public static final int BYTE_LEN = Long.BYTES;

    final long requestId;

    public AllocResponse(final long requestId) {
        this.requestId = requestId;
    }

    public AllocResponse(final byte[] data) {
        assert (data.length == AllocResponse.BYTE_LEN);
        this.requestId = Utils.bytesToLong(data, 0);
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[AllocResponse.BYTE_LEN];
        Utils.longToBytes(this.requestId, buff, 0);
        return buff;
    }
}
