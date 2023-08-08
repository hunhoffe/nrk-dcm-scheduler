/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos.rpc;

import org.junit.jupiter.api.Test;

public class TestSerialization {
    @Test
    public void testReleaseRequest() {
        ReleaseRequest req = new ReleaseRequest((byte) 2, (byte) 3, (byte) 4, (byte) 5);
        byte[] b = req.toBytes();
        assert(b.length == ReleaseRequest.BYTE_LEN);
        ReleaseRequest req2 = new ReleaseRequest(b);
        assert(req2.nodeId == req.nodeId);
        assert(req2.application == req.application);
        assert(req2.cores == req.cores);
        assert(req2.memslices == req.memslices);
    }

    @Test
    public void testReleaseResponse() {
        ReleaseResponse res = new ReleaseResponse(35);
        byte[] b = res.toBytes();
        assert(b.length == ReleaseResponse.BYTE_LEN);
        ReleaseResponse res2 = new ReleaseResponse(b);
        assert(res2.isSuccess == res.isSuccess);
    }

    @Test
    public void testAllocRequest() {
        AllocRequest req = new AllocRequest((byte) 3, (byte) 4, (byte) 5);
        byte[] b = req.toBytes();
        assert(b.length == AllocRequest.BYTE_LEN);
        AllocRequest req2 = new AllocRequest(b);
        assert(req2.application == req.application);
        assert(req2.cores == req.cores);
        assert(req2.memslices == req.memslices);
    }

    @Test
    public void testAllocResponse() {
        AllocResponse res = new AllocResponse(35);
        byte[] b = res.toBytes();
        assert(b.length == AllocResponse.BYTE_LEN);
        AllocResponse res2 = new AllocResponse(b);
        assert(res2.requestId == res.requestId);
    }

    @Test
    public void testRegisterNodeRequest() {
        RegisterNodeRequest req = new RegisterNodeRequest(2, (byte) 3, (byte) 4);
        byte[] b = req.toBytes();
        assert(b.length == RegisterNodeRequest.BYTE_LEN);
        RegisterNodeRequest req2 = new RegisterNodeRequest(b);
        assert(req2.id == req.id);
        assert(req2.cores == req.cores);
        assert(req2.memslices == req.memslices);
    }

    @Test
    public void testRegisterNodeResponse() {
        RegisterNodeResponse res = new RegisterNodeResponse(true);
        byte[] b = res.toBytes();
        assert(b.length == RegisterNodeResponse.BYTE_LEN);
        RegisterNodeResponse res2 = new RegisterNodeResponse(b);
        assert(res2.nodeCreated == res.nodeCreated);

        res = new RegisterNodeResponse(false);
        b = res.toBytes();
        assert(b.length == RegisterNodeResponse.BYTE_LEN);
        res2 = new RegisterNodeResponse(b);
        assert(res2.nodeCreated == res.nodeCreated);
    }

    @Test
    public void testAffinityRequest() {
        AffinityRequest req = new AffinityRequest((byte) 6, (byte) 7, (byte) 8);
        byte[] b = req.toBytes();
        assert(b.length == AffinityRequest.BYTE_LEN);
        AffinityRequest req2 = new AffinityRequest(b);
        assert(req2.nodeId == req.nodeId);
        assert(req2.cores == req.cores);
        assert(req2.memslices == req.memslices);
    }

    @Test
    public void testAffinityResponse() {
        AffinityResponse res = new AffinityResponse(true);
        byte[] b = res.toBytes();
        assert(b.length == AffinityResponse.BYTE_LEN);
        AffinityResponse res2 = new AffinityResponse(b);
        assert(res2.requestFulfilled == res.requestFulfilled);
    }
}
