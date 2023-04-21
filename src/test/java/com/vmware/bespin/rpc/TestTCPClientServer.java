/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.net.InetAddress;

import org.junit.jupiter.api.Test;

import com.vmware.bespin.scheduler.rpc.RPCID;
 
public class TestTCPClientServer {

    class EchoHandler extends RPCHandler<Integer> {
        public EchoHandler() { }
    
        @Override
        public RPCMessage handleRPC(final RPCMessage msg, final Integer someState) {
            return msg;
        }
    }
    
 
    @Test
    public void testTCPClientServer() throws IOException {
        final RPCServer<Integer> rpcServer = new TCPServer<Integer>("LOCALHOST", 10110);
        final RPCClient rpcClient = new TCPClient(InetAddress.getByName("LOCALHOST"), 10110);

        final Runnable serverRunner =
        () -> {
            rpcServer.register(RPCID.AFFINITY_ALLOC, new EchoHandler());
            rpcServer.addClient();
            try {
                rpcServer.runServer(1);
            } catch (final IOException e) {
                throw new RuntimeException("Failed to run server!");
            }
        };
        final Thread serverThread = new Thread(serverRunner);
        serverThread.start();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        rpcClient.connect();

        final int buffLength = 64;
        byte[] buff = new byte[buffLength];
        for (int i = 0; i < buff.length; i++) {
            buff[i] = (byte) i;
        }
        final byte[] retBuff = rpcClient.call(RPCID.AFFINITY_ALLOC, buff);
        assertEquals(retBuff.length, buff.length, "Received payload is: " + Integer.toString(retBuff.length) + ", " + Arrays.toString(retBuff));
        for (int i = 0; i < buff.length; i++) {
            assertEquals(retBuff[i], i, "Expecting " + Integer.toString(i) + " but got " + Integer.toString(retBuff[i]));
        }

        serverThread.stop();
    }
}