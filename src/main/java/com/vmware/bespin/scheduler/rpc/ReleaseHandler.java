/*
 * Copyright 2022 University of Colorado. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

import com.vmware.bespin.rpc.RPCHandler;
import com.vmware.bespin.rpc.RPCHeader;
import com.vmware.bespin.rpc.RPCMessage;
import com.vmware.bespin.scheduler.Scheduler;
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
        conn.insertInto(Scheduler.APPLICATION_TABLE)
                .set(Scheduler.APPLICATION_TABLE.ID, (int) req.application)
                .onDuplicateKeyIgnore()
                .execute();

        // TODO: should double check we don't go out of range
        // Select all placed (used resources) belonging to this (application, node)
        conn.update(Scheduler.PLACED_TABLE)
                .set(Scheduler.PLACED_TABLE.CORES, Scheduler.PLACED_TABLE.CORES.sub(req.cores))
                .set(Scheduler.PLACED_TABLE.MEMSLICES, Scheduler.PLACED_TABLE.MEMSLICES.sub(req.cores))
                .where(and(Scheduler.PLACED_TABLE.APPLICATION.eq((int) req.application),
                        Scheduler.PLACED_TABLE.NODE.eq((int) req.nodeId)))
                .execute();
        LOG.info("Processed release request: {}", req);

        hdr.msgLen = ReleaseResponse.BYTE_LEN;
        return new RPCMessage(hdr, new ReleaseResponse(0).toBytes());
    }
}