/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

import com.vmware.bespin.rpc.RPCHandler;
import com.vmware.bespin.rpc.RPCHeader;
import com.vmware.bespin.rpc.RPCMessage;
import com.vmware.bespin.scheduler.DCMRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

public class RegisterNodeHandler extends RPCHandler {
    private static final Logger LOG = LogManager.getLogger(RegisterNodeHandler.class);

    private int requestId = 0;
    private DSLContext conn;

    public RegisterNodeHandler(final DSLContext conn) {
        this.conn = conn;
    }

    @Override
    public RPCMessage handleRPC(final RPCMessage msg) {
        final RPCHeader hdr = msg.hdr();
        assert (hdr.msgLen == RegisterNodeRequest.BYTE_LEN);
        final RegisterNodeRequest req = new RegisterNodeRequest(msg.payload());

        conn.insertInto(DCMRunner.NODE_TABLE)
                .set(DCMRunner.NODE_TABLE.ID, requestId)
                .set(DCMRunner.NODE_TABLE.CORES, (int) req.cores)
                .set(DCMRunner.NODE_TABLE.MEMSLICES, (int) req.memslices)
                .execute();
        LOG.info("Handled register node request: {}, assigned id {}", req, requestId);

        hdr.msgLen = RegisterNodeResponse.BYTE_LEN;
        return new RPCMessage(hdr, new RegisterNodeResponse(requestId++).toBytes());
    }
}
