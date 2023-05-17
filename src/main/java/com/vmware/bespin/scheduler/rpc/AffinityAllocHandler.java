/*
 * Copyright 2023 University of Colorado, VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

import com.vmware.bespin.rpc.RPCHandler;
import com.vmware.bespin.rpc.RPCHeader;
import com.vmware.bespin.rpc.RPCMessage;
import com.vmware.bespin.scheduler.DCMRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AffinityAllocHandler extends RPCHandler<DCMRunner> {
    private static final Logger LOG = LogManager.getLogger(AffinityAllocHandler.class);

    public AffinityAllocHandler() { }

    @Override
    public RPCMessage handleRPC(final RPCMessage msg, final DCMRunner runner) {
        final RPCHeader hdr = msg.hdr();
        final AffinityRequest req = new AffinityRequest(msg.payload());
        boolean requestFulfilled = false;

        // Fetch capacity of the node
        final long coreCapacity = runner.coreCapacityForNode(req.nodeId);
        final long memsliceCapacity = runner.memsliceCapacityForNode(req.nodeId);
        final long usedCores = runner.usedCoresForNode(req.nodeId);
        final long usedMemslices = runner.usedMemslicesForNode(req.nodeId);

        // There is enough memory to fulfill the alloc request!
        if (memsliceCapacity >= usedMemslices + req.memslices && coreCapacity >= usedCores + req.cores) {
            // Subtract from the overall node resource
            runner.updateNode(req.nodeId, req.cores, req.memslices, false);
            requestFulfilled = true;
        }

        LOG.info("Processed scheduler affinity request: {}, cores: {}, " +
                "cores_used: {}, memslices: {}, memslices_used: {}, ret: {}", 
                req, coreCapacity, usedCores, memsliceCapacity, usedMemslices, requestFulfilled);
        runner.printStats();

        hdr.msgLen = AffinityResponse.BYTE_LEN;
        return new RPCMessage(hdr, new AffinityResponse(requestFulfilled).toBytes());
    }
}
