/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.rpc;

public class RPCHeader {
        public static final int BYTE_LEN = Long.BYTES * 2;

        private final long msgType;     // 8 byte
        public long msgLen;             // 8 bytes

        public RPCHeader(final long msgType, final long msgLen) {
                this.msgType = msgType;
                this.msgLen = msgLen;
        }

        public RPCHeader(final byte[] data) {
                assert (data.length == RPCHeader.BYTE_LEN);
                this.msgType = Utils.bytesToLong(data, 0);
                this.msgLen = Utils.bytesToLong(data, Long.BYTES);
        }

        public byte[] toBytes() {
                final byte[] buff = new byte[RPCHeader.BYTE_LEN];
                Utils.longToBytes(this.msgType, buff, 0);
                Utils.longToBytes(this.msgLen, buff, Long.BYTES);
                return buff;
        }

        public byte getType() {
                return (byte) this.msgType;
        }

        @Override
        public String toString() {
                return String.format("RPCHdr(type=%d, len=%d)", this.msgType, this.msgLen);
        }
}
