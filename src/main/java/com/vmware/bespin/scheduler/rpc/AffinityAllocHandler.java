/*
 * Copyright 2023 University of Colorado, VMware, Inc. All Rights Reserved.
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
import org.jooq.Record;
import org.jooq.Result;


public class AffinityAllocHandler extends RPCHandler {
    private static final Logger LOG = LogManager.getLogger(AffinityAllocHandler.class);
    private DSLContext conn;

    public AffinityAllocHandler(final DSLContext conn) {
        this.conn = conn;
    }

    @Override
    public RPCMessage handleRPC(final RPCMessage msg) {
        final RPCHeader hdr = msg.hdr();
        final AffinityRequest req = new AffinityRequest(msg.payload());
        boolean requestFulfilled = false;

        // Fetch capacity of the node
        final Result<Record> memsliceData = this.conn.fetch(
            String.format("select memslices from nodes where id = %d", req.nodeId));
        final Result<Record> coreData = this.conn.fetch(
            String.format("select cores from nodes where id = %d", req.nodeId));
        final Result<Record> usedMemsliceData = this.conn.fetch(String.format(
                "select sum(placed.memslices) from placed where node = %d", req.nodeId));
        final Result<Record> usedCoreData = this.conn.fetch(String.format(
            "select sum(placed.cores) from placed where node = %d", req.nodeId));

        if (null != memsliceData && memsliceData.isNotEmpty() && null != coreData && coreData.isNotEmpty()
                && null != usedMemsliceData && usedMemsliceData.isNotEmpty() && 
                null != usedCoreData && usedCoreData.isNotEmpty()) {
            try {
                final long memslices = Integer.toUnsignedLong((int) memsliceData.get(0).getValue(0));
                final long cores = Integer.toUnsignedLong((int) coreData.get(0).getValue(0));

                // If no pending, these values may be null. That's fine, we know the value is zero in that case.
                long usedMemslices = 0;
                long usedCores = 0;
                try {
                    usedCores += (long) usedCoreData.get(0).getValue(0);
                } catch (final NullPointerException e) { }
                try {
                    usedMemslices += (long) usedMemsliceData.get(0).getValue(0);
                } catch (final NullPointerException e) { }

                // There is enough memory to fulfill the alloc request!
                if (memslices >= usedMemslices + req.memslices && cores >= usedCores + req.cores) {
                    // Subtract from the overall node resource
                    conn.update(Scheduler.NODE_TABLE)
                        .set(Scheduler.NODE_TABLE.MEMSLICES, Scheduler.NODE_TABLE.MEMSLICES.sub(req.memslices))
                        .set(Scheduler.NODE_TABLE.CORES, Scheduler.NODE_TABLE.CORES.sub(req.cores))
                        .where(Scheduler.NODE_TABLE.ID.eq((int) req.nodeId))
                        .execute();
                    requestFulfilled = true;
                }

                LOG.info("Processed scheduler affinity request: {}, cores: {}, " +
                    "cores_used: {}, memslices: {}, memslices_used: {}, ret: {}", 
                    req, cores, usedCores, memslices, usedMemslices, requestFulfilled);
            } catch (final NumberFormatException e) {
                LOG.info("Failed to unpack memslices/cores correctly: {}", e.toString());
            }
        } else {
            LOG.warn("Failed to process scheduler affinity alloc request because memslice " +
            "data for node {} not found", req.nodeId);
        }

        hdr.msgLen = AffinityResponse.BYTE_LEN;
        return new RPCMessage(hdr, new AffinityResponse(requestFulfilled).toBytes());
    }
}
