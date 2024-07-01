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

        // TODO: add to executor pool
        scheduler.updateNode(req.nodeId, req.cores, req.memslices, false);

        hdr.msgLen = AffinityResponse.BYTE_LEN;
        return new RPCMessage(hdr, new AffinityResponse(true).toBytes());
    }
}
