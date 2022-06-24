package com.vmware.bespin.rpc;

public class RPCHeader {
        public static final int BYTE_LEN = Long.BYTES * 4 + 1;

        public final long clientId;    // 8 bytes
        public final long pid;         // 8 bytes
        public final long reqId;       // 8 bytes
        public final byte msgType;     // 1 byte
        public long msgLen;            // 8 bytes

        public RPCHeader(long clientId, long pid, long reqId, byte msgType, long msgLen) {
                this.clientId = clientId;
                this.pid = pid;
                this.reqId = reqId;
                this.msgType = msgType;
                this.msgLen = msgLen;
        }

        public RPCHeader(byte[] data) {
                assert(data.length == RPCHeader.BYTE_LEN);
                this.clientId = Utils.bytesToLong(data, 0);
                this.pid = Utils.bytesToLong(data, Long.BYTES);
                this.reqId = Utils.bytesToLong(data, Long.BYTES * 2);
                this.msgType = data[Long.BYTES * 3];
                this.msgLen = Utils.bytesToLong(data, Long.BYTES * 3 + 1);
        }

        public byte[] toBytes() {
                byte[] buff = new byte[RPCHeader.BYTE_LEN];
                Utils.longToBytes(this.clientId, buff, 0);
                Utils.longToBytes(this.pid, buff, Long.BYTES);
                Utils.longToBytes(this.reqId, buff, Long.BYTES * 2);
                buff[Long.BYTES * 3] = this.msgType;
                Utils.longToBytes(this.msgLen, buff, Long.BYTES * 3 + 1);
                return buff;
        }
}
