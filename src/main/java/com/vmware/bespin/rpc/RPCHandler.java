/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.rpc;

public abstract class RPCHandler<S> {
    public abstract RPCMessage handleRPC(RPCMessage msg, S serverContext);
}
