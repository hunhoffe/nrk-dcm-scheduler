package com.vmware.bespin.scheduler.rpc;

import com.vmware.bespin.rpc.RPCHandler;
import com.vmware.bespin.rpc.RPCHeader;
/*
 * Copyright 2023 University of Colorado, VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

import com.vmware.bespin.rpc.RPCMessage;
import com.vmware.bespin.scheduler.Scheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;


public class AffinityReleaseHandler extends RPCHandler {
    private static final Logger LOG = LogManager.getLogger(AffinityReleaseHandler.class);
    private DSLContext conn;

    public AffinityReleaseHandler(final DSLContext conn) {
        this.conn = conn;
    }

    @Override
    public RPCMessage handleRPC(final RPCMessage msg) {
        final RPCHeader hdr = msg.hdr();
        final AffinityRequest req = new AffinityRequest(msg.payload());

        // Add to the node capacity
        conn.update(Scheduler.NODE_TABLE)
            .set(Scheduler.NODE_TABLE.CORES, Scheduler.NODE_TABLE.CORES.add(req.cores))
            .set(Scheduler.NODE_TABLE.MEMSLICES, Scheduler.NODE_TABLE.MEMSLICES.add(req.memslices))
            .where(Scheduler.NODE_TABLE.ID.eq((int) req.nodeId))
            .execute();

        LOG.info("Processed scheduler affinity release request: {}", req); 
        hdr.msgLen = AffinityResponse.BYTE_LEN;
        return new RPCMessage(hdr, new AffinityResponse(true).toBytes());
    }
}
