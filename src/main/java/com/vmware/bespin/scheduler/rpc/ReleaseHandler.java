/*
 * Copyright 2022 University of Colorado. All Rights Reserved.
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
import static org.jooq.impl.DSL.and;

public class ReleaseHandler extends RPCHandler {
    private static final Logger LOG = LogManager.getLogger(ReleaseHandler.class);
    private DSLContext conn;

    public ReleaseHandler(final DSLContext conn) {
        this.conn = conn;
    }

    @Override
    public RPCMessage handleRPC(final RPCMessage msg) {
        final RPCHeader hdr = msg.hdr();
        final ReleaseRequest req = new ReleaseRequest(msg.payload());

        // Add application to application table if new
        conn.insertInto(DCMRunner.APP_TABLE)
                .set(DCMRunner.APP_TABLE.ID, (int) req.application)
                .onDuplicateKeyIgnore()
                .execute();

        // TODO: should double check we don't go out of range
        // Select all placed (used resources) belonging to this (application, node)
        conn.update(DCMRunner.PLACED_TABLE)
                .set(DCMRunner.PLACED_TABLE.CORES, DCMRunner.PLACED_TABLE.CORES.sub(req.cores))
                .set(DCMRunner.PLACED_TABLE.MEMSLICES, DCMRunner.PLACED_TABLE.MEMSLICES.sub(req.cores))
                .where(and(DCMRunner.PLACED_TABLE.APPLICATION.eq((int) req.application),
                    DCMRunner.PLACED_TABLE.NODE.eq((int) req.nodeId)))
                .execute();
        LOG.info("Processed release request: {}", req);

        hdr.msgLen = ReleaseResponse.BYTE_LEN;
        return new RPCMessage(hdr, new ReleaseResponse(0).toBytes());
    }
}