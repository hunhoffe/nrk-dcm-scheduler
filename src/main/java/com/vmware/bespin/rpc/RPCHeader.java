/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.rpc;

public class RPCHeader {
    public static final int BYTE_LEN = 4;
    private final byte msgId;       // 1 byte
    private final byte msgType;     // 1 byte
    public short msgLen;            // 2 bytes

    public RPCHeader(final byte msgType, final short msgLen) {
        this.msgId = 0;
        this.msgType = msgType;
        this.msgLen = msgLen;
    }

    public RPCHeader(final byte[] data) {
        assert (data.length == RPCHeader.BYTE_LEN);
        this.msgId = data[0];
        this.msgType = data[1];
        this.msgLen = Utils.bytesToShort(data, 2);
    }

    public byte[] toBytes() {
        final byte[] buff = new byte[RPCHeader.BYTE_LEN];
        buff[0] = this.msgId;
        buff[1] = this.msgType;
        Utils.shortToBytes(this.msgLen, buff, 2);
        return buff;
    }

    public byte getType() {
        return (byte) this.msgType;
    }

    @Override
    public String toString() {
        return String.format("RPCHdr(id=%d, type=%d, len=%d)", this.msgId, this.msgType, this.msgLen);
    }
}
