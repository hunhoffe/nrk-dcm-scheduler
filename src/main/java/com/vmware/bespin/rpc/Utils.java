package com.vmware.bespin.rpc;

public class Utils {
    // Below modified from:
    // https://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java/29132118#29132118
    public static void longToBytes(long l, byte[] b, int offset) {
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            b[offset + Long.BYTES - 1 - i] = (byte)(l & 0xFF);
            l >>= Byte.SIZE;
        }
    }

    // Below modified from:
    // https://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java/29132118#29132118
    public static long bytesToLong(final byte[] b, int offset) {
        long result = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (b[offset + Long.BYTES - 1 - i] & 0xFF);
        }
        return result;
    }
}
