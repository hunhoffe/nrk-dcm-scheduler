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
import org.jooq.Field;

public class AllocHandler extends RPCHandler {
    private static final Logger LOG = LogManager.getLogger(AllocHandler.class);
    private long requestId = 0;
    private DSLContext conn;

    public AllocHandler(final DSLContext conn) {
        this.conn = conn;
    }

    @Override
    public RPCMessage handleRPC(final RPCMessage msg) {
        final RPCHeader hdr = msg.hdr();
        final AllocRequest req = new AllocRequest(msg.payload());

        // Add application to application table if new
        conn.insertInto(DCMRunner.APP_TABLE)
                .set(DCMRunner.APP_TABLE.ID, (int) req.application)
                .onDuplicateKeyIgnore()
                .execute();

        // Add request to pending table
        conn.insertInto(DCMRunner.PENDING_TABLE)
                .set(DCMRunner.PENDING_TABLE.ID, requestId)
                .set(DCMRunner.PENDING_TABLE.APPLICATION, (int) req.application)
                .set(DCMRunner.PENDING_TABLE.CORES, (int) req.cores)
                .set(DCMRunner.PENDING_TABLE.MEMSLICES, (int) req.memslices)
                .set(DCMRunner.PENDING_TABLE.STATUS, "PENDING")
                .set(DCMRunner.PENDING_TABLE.CURRENT_NODE, -1)
                .set(DCMRunner.PENDING_TABLE.CONTROLLABLE__NODE, (Field<Integer>) null)
                .execute();
        LOG.info("Processed scheduler request: {}", req);

        hdr.msgLen = AllocResponse.BYTE_LEN;
        return new RPCMessage(hdr, new AllocResponse(requestId++).toBytes());
    }
}
