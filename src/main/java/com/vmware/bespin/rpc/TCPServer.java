/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.rpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.bespin.scheduler.dinos.rpc.RPCID;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;


public class TCPServer<S> extends RPCServer<S> {
    private ServerSocket socket = null;
    private Socket clientSocket = null;
    private OutputStream clientOut = null;
    private InputStream clientIn = null;
    private HashMap<Byte, RPCHandler> handlers = new HashMap<>();
    private byte[] hdrBuff = new byte[RPCHeader.BYTE_LEN];
    private static final Logger LOG = LogManager.getLogger(TCPServer.class);
    private boolean shutdown;

    public TCPServer(final String ip, final int port) throws IOException {
        // Retry until successful just in cast tap interfaces aren't up
        boolean done = false;
        this.shutdown = false;
        while (!done && !this.shutdown) {
            try {
                this.socket = new ServerSocket(port, 1, InetAddress.getByName(ip));
                LOG.info("Server socket address: " + this.socket.getLocalSocketAddress());
                done = true;
            } catch (final IOException e) { 
                e.printStackTrace();
                try {
                    Thread.sleep(10000);
                } catch (final InterruptedException ignored) { }
            }
        }
    }

    @Override
    public boolean register(final RPCID rpcId, final RPCHandler<S> handler) {
        // Cannot add if key already exists
        if (this.handlers.containsKey(rpcId.id())) {
            return false;
        }

        this.handlers.put(rpcId.id(), handler);
        return true;
    }

    @Override
    public boolean addClient() {
        // Fail is client already accepted. Can only have one client, currently
        if (this.clientSocket != null) {
            return false;
        }

        // Try to accept a client
        try {
            this.clientSocket = this.socket.accept();
            this.clientOut = clientSocket.getOutputStream();
            this.clientIn = clientSocket.getInputStream();

            this.respond(this.receive());
            return true;
        } catch (final IOException e) {
            e.printStackTrace();
            this.cleanUp();
            return false;
        }
    }

    @Override
    public void runServer(final S serverContext) throws IOException {
        try {
            while (!this.shutdown) {
                final RPCMessage msg = this.receive();
                if (!this.shutdown) {
                    if (this.handlers.containsKey(msg.hdr().getType())) {
                        this.respond(this.handlers.get(msg.hdr().getType()).handleRPC(msg, serverContext));
                    } else {
                        LOG.error("Invalid msgType: {}", msg.hdr().getType());
                        break;
                    }
                } else {
                    break;
                }
            }
            LOG.error("Shutting down TCPServer");
            this.cleanUp();
        } catch (final IOException e) {
            LOG.error("Server failed: {}", Arrays.toString(e.getStackTrace()));
            e.printStackTrace();
            this.cleanUp();
            throw e;
        }
    }

    @Override
    public void stopServer() {
        this.shutdown = true;
    }

    private void cleanUp() {
        try {
            if (null != this.clientOut) {
                this.clientOut.close();
            }
        } catch (final IOException ignored) { }
        try {
            if (null != this.clientIn) {
                this.clientIn.close();
            }
        } catch (final IOException ignored) { }
        try {
            if (null != this.clientSocket) {
                this.clientSocket.close();
            }
        } catch (final IOException ignored) { }
    }

    private RPCMessage receive() throws IOException {
        if (null == this.clientIn) {
            throw new IOException("No clients connected");
        }
        int bytesRead = 0;
        int ret;

        // Read in entire header
        while (bytesRead < RPCHeader.BYTE_LEN && !this.shutdown) {
            ret = this.clientIn.read(hdrBuff, bytesRead, RPCHeader.BYTE_LEN - bytesRead);
            if (ret > 0) {
                bytesRead += ret;
            }
        }
        if (this.shutdown) {
            return null;
        }
        final RPCHeader hdr = new RPCHeader(this.hdrBuff);
        LOG.info("Received header: {}", hdr.toString());

        // Read in entire payload
        final byte[] payload = new byte[(int) hdr.msgLen];
        bytesRead = 0;
        while (bytesRead < hdr.msgLen) {
            ret = this.clientIn.read(payload, bytesRead, (int) hdr.msgLen - bytesRead);
            if (ret > 0) {
                bytesRead += ret;
            }
        }
        return new RPCMessage(hdr, payload);
    }

    private void respond(final RPCMessage msg) throws IOException {
        if (null == this.clientOut) {
            throw new IOException("No clients connected");
        }

        // Write out header
        final byte[] bytes = msg.hdr().toBytes();
        this.clientOut.write(bytes);
        this.clientOut.flush();

        // Write out payload
        if (msg.payload().length > 0) {
            this.clientOut.write(msg.payload());
            this.clientOut.flush();
        }
    }
}
