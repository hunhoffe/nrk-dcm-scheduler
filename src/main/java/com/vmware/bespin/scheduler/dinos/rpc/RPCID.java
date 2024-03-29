/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos.rpc;

public enum RPCID {
    REGISTER_CLIENT((byte) 0),
    REGISTER_NODE((byte) 1),
    ALLOC((byte) 2),
    RELEASE((byte) 3),
    AFFINITY_ALLOC((byte) 4),
    AFFINITY_RELEASE((byte) 5),
    ALLOC_ASSIGNMENT((byte) 6);

    private final byte id;

    public byte id() {
        return this.id;
    }

    private RPCID(final byte handler_id) {
        this.id = handler_id;
    }
}