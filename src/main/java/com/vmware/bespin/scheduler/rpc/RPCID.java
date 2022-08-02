/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.rpc;

public enum RPCID {
    REGISTER_NODE((byte) 1),
    SCHEDULER((byte) 2);

    private final byte id;

    public byte id() {
        return this.id;
    }

    private RPCID(final byte handler_id) {
        this.id = handler_id;
    }
}