/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
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
import org.jooq.Field;

public class SchedulerHandler extends RPCHandler {
    private static final Logger LOG = LogManager.getLogger(SchedulerHandler.class);
    private long requestId = 0;
    private DSLContext conn;

    public SchedulerHandler(final DSLContext conn) {
        this.conn = conn;
    }

    @Override
    public RPCMessage handleRPC(final RPCMessage msg) {
        final RPCHeader hdr = msg.hdr();
        assert (hdr.msgLen == SchedulerRequest.BYTE_LEN);
        final SchedulerRequest req = new SchedulerRequest(msg.payload());

        // Add application to application table if new
        conn.insertInto(Scheduler.APPLICATION_TABLE)
                .set(Scheduler.APPLICATION_TABLE.ID, (int) req.application)
                .onDuplicateKeyIgnore()
                .execute();

        // Add request to pending table
        conn.insertInto(Scheduler.PENDING_TABLE)
                .set(Scheduler.PENDING_TABLE.ID, requestId)
                .set(Scheduler.PENDING_TABLE.APPLICATION, (int) req.application)
                .set(Scheduler.PENDING_TABLE.CORES, (int) req.cores)
                .set(Scheduler.PENDING_TABLE.MEMSLICES, (int) req.memslices)
                .set(Scheduler.PENDING_TABLE.STATUS, "PENDING")
                .set(Scheduler.PENDING_TABLE.CURRENT_NODE, -1)
                .set(Scheduler.PENDING_TABLE.CONTROLLABLE__NODE, (Field<Integer>) null)
                .execute();
        LOG.info("Processed scheduler request for app {} with {} cores, {} memslices",
                req.application, req.cores, req.memslices);

        hdr.msgLen = SchedulerResponse.BYTE_LEN;
        return new RPCMessage(hdr, new SchedulerResponse(requestId++).toBytes());
    }
}