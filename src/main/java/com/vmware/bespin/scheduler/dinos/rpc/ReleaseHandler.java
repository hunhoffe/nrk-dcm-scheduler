/*
 * Copyright 2022 University of Colorado. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos.rpc;

import com.vmware.bespin.rpc.RPCHandler;
import com.vmware.bespin.rpc.RPCHeader;
import com.vmware.bespin.rpc.RPCMessage;
import com.vmware.bespin.scheduler.Scheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReleaseHandler extends RPCHandler<Scheduler> {
    private static final Logger LOG = LogManager.getLogger(ReleaseHandler.class);

    public ReleaseHandler() { }

    @Override
    public RPCMessage handleRPC(final RPCMessage msg, final Scheduler scheduler) {
        final RPCHeader hdr = msg.hdr();
        final ReleaseRequest req = new ReleaseRequest(msg.payload());

        scheduler.releaseAllocation(req.nodeId, req.application, req.cores, req.memslices);
        LOG.info("Processed release request: {}", req);

        hdr.msgLen = ReleaseResponse.BYTE_LEN;
        return new RPCMessage(hdr, new ReleaseResponse(0).toBytes());
    }
}