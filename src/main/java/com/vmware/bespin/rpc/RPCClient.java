/*
* Copyright 2023 VMware, Inc. All Rights Reserved.
* SPDX-License-Identifier: BSD-2 OR MIT
*/

package com.vmware.bespin.rpc;

import java.io.IOException;

import com.vmware.bespin.scheduler.dinos.rpc.RPCID;
 
/// RPC server operations
public abstract class RPCClient {
    /// Connect with the server
    public abstract boolean connect();
 
    /// Trigger an RPC
    public abstract byte[] call(RPCID id, byte[] dataIn) throws IOException;

    /// Teardown the client gracefully
    public abstract void cleanUp();
}