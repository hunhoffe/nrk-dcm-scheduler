package com.vmware.bespin.rpc;

import java.io.IOException;

/// RPC server operations
public abstract class RPCServer {
    /// Register an RPC func with an ID
    // TODO: should probably alias type for rpcId
    public abstract boolean register(byte rpcId, RPCHandler handler);

    ///  Accept an RPC client
    // TODO: should really have a handler as an argument but oh well
    public abstract boolean addClient();

    /// Run the RPC server
    public abstract boolean runServer() throws IOException;
}
