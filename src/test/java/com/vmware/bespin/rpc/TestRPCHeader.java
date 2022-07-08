/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.rpc;

import org.junit.jupiter.api.Test;

public class TestRPCHeader {

    @Test
    public void testConversion() {
        RPCHeader hdr = new RPCHeader(1L, 2L, 3L, (byte) 4, 5L);
        byte[] b = hdr.toBytes();
        assert(b.length == RPCHeader.BYTE_LEN);
        RPCHeader hdr2 = new RPCHeader(b);
        assert(hdr2.clientId == hdr.clientId);
        assert(hdr2.pid == hdr.pid);
        assert(hdr2.reqId == hdr.reqId);
        assert(hdr2.getType() == hdr.getType());
        assert(hdr2.msgLen == hdr.msgLen);
    }
}