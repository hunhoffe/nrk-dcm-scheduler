package com.vmware.bespin.scheduler;

public class RPCHeader {
        public static final int BYTE_LEN = Long.BYTES * 4 + 1;

        final long clientId;    // 8 bytes
        final long pid;         // 8 bytes
        final long reqId;       // 8 bytes
        final byte msgType;     // 1 byte
        final long msgLen;      // 8 bytes

        public RPCHeader(long clientId, long pid, long reqId, byte msgType, long msgLen) {
                this.clientId = clientId;
                this.pid = pid;
                this.reqId = reqId;
                this.msgType = msgType;
                this.msgLen = msgLen;
        }

        public RPCHeader(byte[] data) {
                assert(data.length == RPCHeader.BYTE_LEN);
                this.clientId = bytesToLong(data, 0);
                this.pid = bytesToLong(data, Long.BYTES);
                this.reqId = bytesToLong(data, Long.BYTES * 2);
                this.msgType = data[Long.BYTES * 3];
                this.msgLen = bytesToLong(data, Long.BYTES * 3 + 1);
        }

        public byte[] toBytes() {
                byte[] buff = new byte[RPCHeader.BYTE_LEN];
                longToBytes(this.clientId, buff, 0);
                longToBytes(this.pid, buff, Long.BYTES);
                longToBytes(this.reqId, buff, Long.BYTES * 2);
                buff[Long.BYTES * 3] = this.msgType;
                longToBytes(this.msgLen, buff, Long.BYTES * 3 + 1);
                return buff;
        }

        // Below modified from:
        // https://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java/29132118#29132118
        public static void longToBytes(long l, byte[] b, int offset) {
                for (int i = Long.BYTES - 1; i >= 0; i--) {
                        b[offset + i] = (byte)(l & 0xFF);
                        l >>= Byte.SIZE;
                }
        }

        // Below modified from:
        // https://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java/29132118#29132118
        public static long bytesToLong(final byte[] b, int offset) {
                long result = 0;
                for (int i = 0; i < Long.BYTES; i++) {
                        result <<= Byte.SIZE;
                        result |= (b[offset + i] & 0xFF);
                }
                return result;
        }
}
