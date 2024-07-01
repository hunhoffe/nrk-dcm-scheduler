/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos.rpc;

import com.vmware.bespin.rpc.RPCHandler;
import com.vmware.bespin.rpc.RPCHeader;
import com.vmware.bespin.rpc.RPCMessage;
import com.vmware.bespin.scheduler.dinos.DiNOSScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AllocHandler extends RPCHandler<DiNOSScheduler> {
    private static final Logger LOG = LogManager.getLogger(AllocHandler.class);
    private static long requestId = 0;

    @Override
    public RPCMessage handleRPC(final RPCMessage msg, final DiNOSScheduler scheduler) {
        final RPCHeader hdr = msg.hdr();
        final AllocRequest req = new AllocRequest(msg.payload());

        // Each core and memslice becomes a new request, so reserve a unique block
        final long requestIdStart = this.requestId;
        this.requestId += req.cores + req.memslices;

        // TODO: how to validate?
        final Runnable createRequests = () -> {
            // Add request to pending table
            scheduler.generateRequests(requestIdStart, req.cores, req.memslices, req.application);
        };

        scheduler.workerPool.execute(
            createRequests
        );

        LOG.info("Processed scheduler request: {}", req);

        hdr.msgLen = AllocResponse.BYTE_LEN;
        return new RPCMessage(hdr, new AllocResponse(requestIdStart).toBytes());
    }
}
