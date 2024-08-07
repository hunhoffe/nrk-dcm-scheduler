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

public class RegisterNodeHandler extends RPCHandler<DiNOSScheduler> {
    private static final Logger LOG = LogManager.getLogger(RegisterNodeHandler.class);
    
    public RegisterNodeHandler() { }

    @Override
    public RPCMessage handleRPC(final RPCMessage msg, final DiNOSScheduler scheduler) {
        final RPCHeader hdr = msg.hdr();
        assert (hdr.msgLen == RegisterNodeRequest.BYTE_LEN);
        final RegisterNodeRequest req = new RegisterNodeRequest(msg.payload());

        scheduler.addNode(req.id, req.cores, req.memslices);
        LOG.info("Handled register node request: {}, assigned id {}", req, req.id);

        hdr.msgLen = RegisterNodeResponse.BYTE_LEN;
        return new RPCMessage(hdr, new RegisterNodeResponse(true).toBytes());
    }
}
