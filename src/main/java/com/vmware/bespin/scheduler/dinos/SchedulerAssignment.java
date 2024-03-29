/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos;

import com.vmware.bespin.rpc.Utils;

public class SchedulerAssignment {
    public static final int BYTE_LEN = Long.BYTES * 2;

    final long requestId;
    final long node;

    public SchedulerAssignment(final long requestId, final long node) {
        this.requestId = requestId;
        this.node = node;
    }

    public SchedulerAssignment(final byte[] data) {
        assert (data.length == SchedulerAssignment.BYTE_LEN);
        this.requestId = Utils.bytesToLong(data, 0);
        this.node = Utils.bytesToLong(data, Long.BYTES);
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[SchedulerAssignment.BYTE_LEN];
        Utils.longToBytes(this.requestId, buff, 0);
        Utils.longToBytes(this.node, buff, Long.BYTES);
        return buff;
    }
}
