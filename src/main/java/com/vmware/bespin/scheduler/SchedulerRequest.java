package com.vmware.bespin.scheduler;

public class SchedulerRequest {
    public static final int BYTE_LEN = 3;

    final byte application;
    final byte cores;
    final byte memslices;

    public SchedulerRequest(final byte application, final byte cores, final byte memslices) {
        this.application = application;
        this.cores = cores;
        this.memslices = memslices;
    }

    public SchedulerRequest(final byte[] data) {
        assert (data.length == SchedulerRequest.BYTE_LEN);

        this.application = data[0];
        this.cores = data[1];
        this.memslices = data[2];
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[SchedulerRequest.BYTE_LEN];
        buff[0] = this.application;
        buff[1] = this.cores;
        buff[2] = this.memslices;
        return buff;
    }
}