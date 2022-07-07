package com.vmware.bespin.rpc;

public abstract class RPCHandler {
    public abstract RPCMessage handleRPC(RPCMessage msg);
}
