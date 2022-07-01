package com.vmware.bespin.scheduler;

import com.vmware.bespin.rpc.Utils;

public class SchedulerAssignment {
    public static final int BYTE_LEN = Long.BYTES * 2;

    final long requestId;
    final long node;

    public SchedulerAssignment(long requestId, long node) {
        this.requestId = requestId;
        this.node = node;
    }

    public SchedulerAssignment(byte[] data) {
        assert(data.length == SchedulerAssignment.BYTE_LEN);
        this.requestId = Utils.bytesToLong(data, 0);
        this.node = Utils.bytesToLong(data, Long.BYTES);
    }

    public byte[] toBytes() {
        byte[] buff = new byte[SchedulerAssignment.BYTE_LEN];
        Utils.longToBytes(this.requestId, buff, 0);
        Utils.longToBytes(this.node, buff, Long.BYTES);
        return buff;
    }
}
