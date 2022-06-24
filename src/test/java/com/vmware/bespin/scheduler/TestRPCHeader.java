package com.vmware.bespin.scheduler;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

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
        assert(hdr2.msgType == hdr.msgType);
        assert(hdr2.msgLen == hdr.msgLen);
    }
}