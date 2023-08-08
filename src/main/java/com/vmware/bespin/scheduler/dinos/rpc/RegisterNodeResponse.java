/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos.rpc;

public class RegisterNodeResponse {
     public static final int BYTE_LEN = 1;

    final byte nodeCreated;

    public RegisterNodeResponse(final boolean nodeCreated) {
        this.nodeCreated = (byte) (nodeCreated ? 1 : 0);
    }

    public RegisterNodeResponse(final byte[] data) {
        assert (data.length == RegisterNodeResponse.BYTE_LEN);
        this.nodeCreated = data[0];
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[RegisterNodeResponse.BYTE_LEN];
        buff[0] = this.nodeCreated;
        return buff;
    }
}
