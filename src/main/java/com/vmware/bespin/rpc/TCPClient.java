/*
* Copyright 2023 VMware, Inc. All Rights Reserved.
* SPDX-License-Identifier: BSD-2 OR MIT
*/

package com.vmware.bespin.rpc;

import com.vmware.bespin.scheduler.rpc.RPCID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
 
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
 
public class TCPClient extends RPCClient {
    final InetAddress ip;
    final int port;
    Socket socket = null;
    OutputStream clientOut = null;
    InputStream clientIn = null;
    byte[] hdrBuff = new byte[RPCHeader.BYTE_LEN];
    private static final Logger LOG = LogManager.getLogger(TCPClient.class);
 
    public TCPClient(final InetAddress ip, final int port) throws IOException {
        // Retry until successful just in cast tap interfaces aren't up
        this.ip = ip;
        this.port = port;
    }
 
    @Override
    public boolean connect() {
        try {
            this.socket = new Socket(this.ip, this.port);
            this.clientOut = socket.getOutputStream();
            this.clientIn = socket.getInputStream();
            LOG.info("Local client socket address: " + this.socket.getLocalSocketAddress());
            this.call(RPCID.REGISTER_CLIENT, new byte[0]);
            LOG.info("Sent client registration rpc");
            return true;
        } catch (final IOException e) {
            LOG.info("Failed to start client socket: " + e.toString());
            cleanUp();
            return false;
        }
    }
 
    @Override
    public byte[] call(final RPCID id, final byte[] dataIn) throws IOException {
        final RPCHeader hdr = new RPCHeader(id.id(), (long) dataIn.length);
        RPCMessage msg = new RPCMessage(hdr, dataIn);
        LOG.info("TCPClient Sending msg: " + msg.toString());
        this.respond(msg);
        msg = this.receive();
        LOG.info("TCPClient Received msg: " + msg.toString());
        LOG.info("payload: " + Integer.toString(msg.payload().length) + " payload: " + Arrays.toString(msg.payload()));
        return msg.payload();
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
            if (null != this.socket) {
                this.socket.close();
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
        while (bytesRead < RPCHeader.BYTE_LEN) {
            ret = this.clientIn.read(hdrBuff, bytesRead, RPCHeader.BYTE_LEN - bytesRead);
            if (ret > 0) {
                bytesRead += ret;
            }
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
            throw new IOException("Client not connected");
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
 