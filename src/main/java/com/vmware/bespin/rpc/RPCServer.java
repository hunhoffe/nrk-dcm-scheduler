/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.rpc;

import java.io.IOException;

import com.vmware.bespin.scheduler.dinos.rpc.RPCID;

/// RPC server operations
public abstract class RPCServer<S> {
    /// Register an RPC func with an ID
    public abstract boolean register(RPCID id, RPCHandler<S> handler);

    ///  Accept an RPC client
    // TODO: should really have a handler as an argument but not needed yet
    public abstract boolean addClient();

    /// Run the RPC server
    public abstract void runServer(S serverContext) throws IOException;

    /// Stop the RPC server
    public abstract void stopServer();
}
