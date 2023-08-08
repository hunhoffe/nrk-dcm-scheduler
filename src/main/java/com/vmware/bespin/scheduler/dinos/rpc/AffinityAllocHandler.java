/*
 * Copyright 2023 University of Colorado, VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos.rpc;

import com.vmware.bespin.rpc.RPCHandler;
import com.vmware.bespin.rpc.RPCHeader;
import com.vmware.bespin.rpc.RPCMessage;
import com.vmware.bespin.scheduler.Scheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AffinityAllocHandler extends RPCHandler<Scheduler> {
    private static final Logger LOG = LogManager.getLogger(AffinityAllocHandler.class);

    public AffinityAllocHandler() { }

    @Override
    public RPCMessage handleRPC(final RPCMessage msg, final Scheduler scheduler) {
        final RPCHeader hdr = msg.hdr();
        final AffinityRequest req = new AffinityRequest(msg.payload());
        boolean requestFulfilled = false;

        // Fetch capacity of the node
        final long coreCapacity = scheduler.coreCapacityForNode(req.nodeId);
        final long memsliceCapacity = scheduler.memsliceCapacityForNode(req.nodeId);
        final long usedCores = scheduler.usedCoresForNode(req.nodeId);
        final long usedMemslices = scheduler.usedMemslicesForNode(req.nodeId);

        LOG.warn("Processed scheduler affinity request: {}, cores: {}, " +
            "cores_used: {}, memslices: {}, memslices_used: {}", 
            req, coreCapacity, usedCores, memsliceCapacity, usedMemslices);

        // There is enough memory to fulfill the alloc request!
        if (memsliceCapacity >= usedMemslices + req.memslices && coreCapacity >= usedCores + req.cores) {
            // Subtract from the overall node resource
            scheduler.updateNode(req.nodeId, req.cores, req.memslices, false);
            requestFulfilled = true;
        }

        // Fetch capacity of the node
        final long coreCapacity2 = scheduler.coreCapacityForNode(req.nodeId);
        final long memsliceCapacity2 = scheduler.memsliceCapacityForNode(req.nodeId);
        final long usedCores2 = scheduler.usedCoresForNode(req.nodeId);
        final long usedMemslices2 = scheduler.usedMemslicesForNode(req.nodeId);

        LOG.warn("Processed scheduler affinity request: {}, cores: {}, " +
                "cores_used: {}, memslices: {}, memslices_used: {}, ret: {}", 
                req, coreCapacity2, usedCores2, memsliceCapacity2, usedMemslices2, requestFulfilled);
        scheduler.printStats();

        hdr.msgLen = AffinityResponse.BYTE_LEN;
        return new RPCMessage(hdr, new AffinityResponse(requestFulfilled).toBytes());
    }
}
