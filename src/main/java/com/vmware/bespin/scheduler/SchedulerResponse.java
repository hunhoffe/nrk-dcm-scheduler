package com.vmware.bespin.scheduler;

import com.vmware.bespin.rpc.Utils;

public class SchedulerResponse {
    public static final int BYTE_LEN = Long.BYTES;

    final long requestId;

    public SchedulerResponse(long requestId) {
        this.requestId = requestId;
    }

    public SchedulerResponse(byte[] data) {
        assert(data.length == SchedulerResponse.BYTE_LEN);
        this.requestId = Utils.bytesToLong(data, 0);
    }

    public byte[] toBytes() {
        byte[] buff = new byte[SchedulerResponse.BYTE_LEN];
        Utils.longToBytes(this.requestId, buff, 0);
        return buff;
    }
}
