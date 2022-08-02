/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

import com.vmware.bespin.rpc.Utils;

public class RegisterNodeResponse {
    public static final int BYTE_LEN = Long.BYTES;

    final long nodeId;

    public RegisterNodeResponse(final long nodeId) {
        this.nodeId = nodeId;
    }

    public RegisterNodeResponse(final byte[] data) {
        assert (data.length == RegisterNodeResponse.BYTE_LEN);
        this.nodeId = Utils.bytesToLong(data, 0);
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[RegisterNodeResponse.BYTE_LEN];
        Utils.longToBytes(this.nodeId, buff, 0);
        return buff;
    }
}
