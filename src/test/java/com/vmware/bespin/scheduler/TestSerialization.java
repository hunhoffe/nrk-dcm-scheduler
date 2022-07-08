/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler;

import org.junit.jupiter.api.Test;

public class TestSerialization {
    
    @Test
    public void testAssignment() {
        SchedulerAssignment assignment = new SchedulerAssignment(1, 2);
        byte[] b = assignment.toBytes();
        assert(b.length == SchedulerAssignment.BYTE_LEN);
        SchedulerAssignment assignment2 = new SchedulerAssignment(b);
        assert(assignment2.requestId == assignment.requestId);
        assert(assignment2.node == assignment.node);
    }

    @Test
    public void testRequest() {
        SchedulerRequest req = new SchedulerRequest((byte) 3, (byte) 4, (byte) 5);
        byte[] b = req.toBytes();
        assert(b.length == SchedulerRequest.BYTE_LEN);
        SchedulerRequest req2 = new SchedulerRequest(b);
        assert(req2.application == req.application);
        assert(req2.cores == req.cores);
        assert(req2.memslices == req.memslices);
    }

    @Test
    public void testResponse() {
        SchedulerResponse res = new SchedulerResponse(35);
        byte[] b = res.toBytes();
        assert(b.length == SchedulerResponse.BYTE_LEN);
        SchedulerResponse res2 = new SchedulerResponse(b);
        assert(res2.requestId == res.requestId);
    }
}
