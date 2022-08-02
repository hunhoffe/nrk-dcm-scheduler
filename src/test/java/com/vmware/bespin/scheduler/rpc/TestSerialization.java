/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

import org.junit.jupiter.api.Test;

public class TestSerialization {

    @Test
    public void testSchedulerRequest() {
        SchedulerRequest req = new SchedulerRequest((byte) 3, (byte) 4, (byte) 5);
        byte[] b = req.toBytes();
        assert(b.length == SchedulerRequest.BYTE_LEN);
        SchedulerRequest req2 = new SchedulerRequest(b);
        assert(req2.application == req.application);
        assert(req2.cores == req.cores);
        assert(req2.memslices == req.memslices);
    }

    @Test
    public void testSchedulerResponse() {
        SchedulerResponse res = new SchedulerResponse(35);
        byte[] b = res.toBytes();
        assert(b.length == SchedulerResponse.BYTE_LEN);
        SchedulerResponse res2 = new SchedulerResponse(b);
        assert(res2.requestId == res.requestId);
    }

    @Test
    public void testRegisterNodeRequest() {
        RegisterNodeRequest req = new RegisterNodeRequest((byte) 3, (byte) 4);
        byte[] b = req.toBytes();
        assert(b.length == RegisterNodeRequest.BYTE_LEN);
        RegisterNodeRequest req2 = new RegisterNodeRequest(b);
        assert(req2.cores == req.cores);
        assert(req2.memslices == req.memslices);
    }

    @Test
    public void testRegisterNodeResponse() {
        RegisterNodeResponse res = new RegisterNodeResponse(35);
        byte[] b = res.toBytes();
        assert(b.length == RegisterNodeResponse.BYTE_LEN);
        RegisterNodeResponse res2 = new RegisterNodeResponse(b);
        assert(res2.nodeId == res.nodeId);
    }
}
