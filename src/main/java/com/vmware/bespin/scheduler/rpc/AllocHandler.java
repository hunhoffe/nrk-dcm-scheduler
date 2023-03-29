/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

import com.vmware.bespin.rpc.RPCHandler;
import com.vmware.bespin.rpc.RPCHeader;
import com.vmware.bespin.rpc.RPCMessage;
import com.vmware.bespin.scheduler.DCMRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AllocHandler extends RPCHandler<DCMRunner> {
    private static final Logger LOG = LogManager.getLogger(AllocHandler.class);
    private long requestId = 0;

    @Override
    public RPCMessage handleRPC(final RPCMessage msg, final DCMRunner runner) {
        final RPCHeader hdr = msg.hdr();
        final AllocRequest req = new AllocRequest(msg.payload());

        // Add application to application table if new
        runner.addApplication(req.application);

        // Add request to pending table
        runner.generateRequests(requestId, req.cores, req.memslices, req.application);
        LOG.info("Processed scheduler request: {}", req);

        final long requestIdStart = requestId;
        requestId += req.cores + req.memslices;

        hdr.msgLen = AllocResponse.BYTE_LEN;
        return new RPCMessage(hdr, new AllocResponse(requestIdStart).toBytes());
    }
}
