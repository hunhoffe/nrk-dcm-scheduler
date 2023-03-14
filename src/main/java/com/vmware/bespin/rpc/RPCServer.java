/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.rpc;

import com.vmware.bespin.scheduler.rpc.RPCID;

import java.io.IOException;

/// RPC server operations
public abstract class RPCServer<S> {
    /// Register an RPC func with an ID
    public abstract boolean register(RPCID id, RPCHandler<S> handler);

    ///  Accept an RPC client
    // TODO: should really have a handler as an argument but not needed yet
    public abstract boolean addClient();

    /// Run the RPC server
    public abstract boolean runServer(S serverContext) throws IOException;
}
