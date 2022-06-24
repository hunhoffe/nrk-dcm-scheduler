package com.vmware.bespin.rpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.HashMap;


public class TCPServer extends RPCServer {
    ServerSocket socket = null;
    Socket clientSocket = null;
    OutputStream clientOut = null;
    InputStream clientIn = null;
    HashMap<Byte, RPCHandler> handlers = new HashMap<>();
    byte[] hdrBuff = new byte[RPCHeader.BYTE_LEN];
    private static final Logger LOG = LogManager.getLogger(TCPServer.class);

    public TCPServer(String ip, int port) throws IOException {
        InetAddress ipAddr = InetAddress.getByName(ip);
        SocketAddress sAddr = new InetSocketAddress(ipAddr, port);

        // Retry until successful just in cast tap interfaces aren't up
        boolean done = false;
        while (!done) {
            try {
                this.socket = new ServerSocket();
                this.socket.bind(sAddr);
                LOG.info("Server socket address: " + this.socket.getLocalSocketAddress());
                done = true;
            } catch (Exception e) {
                LOG.info("Failed to connect socket");
                LOG.debug(e.toString());
            }
        }
    }

    @Override
    public boolean register(byte rpcId, RPCHandler handler) {
        // Cannot add if key already exists
        if (this.handlers.containsKey(rpcId)) {
            return false;
        }

        this.handlers.put(rpcId, handler);
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
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            this.cleanUp();
            return false;
        }
    }

    @Override
    public boolean runServer() throws IOException {
        try {
            while (true) {
                RPCMsg msg = this.receive();
                if (this.handlers.containsKey(msg.hdr().msgType)) {
                    this.respond(this.handlers.get(msg.hdr().msgType).handleRPC(msg));
                } else {
                    LOG.error("Invalid msgType: {}", msg.hdr().msgType);
                }
            }
        } catch (Exception e) {
            LOG.error("Server failed: {}", Arrays.toString(e.getStackTrace()));
            this.cleanUp();
            throw e;
        }
    }

    private void cleanUp() {
        try {
            if (null != this.clientOut) {
                this.clientOut.close();
            }
        } catch (Exception ignored) {}
        try {
            if (null != this.clientIn) {
                this.clientIn.close();
            }
        } catch (Exception ignored) {}
        try {
            if (null != this.clientSocket) {
                this.clientSocket.close();
            }
        } catch (Exception ignored) {}
    }

    private RPCMsg receive() throws IOException {
        if (null == this.clientIn) {
            throw new IOException("No clients connected");
        }
        int bytesRead = 0;
        int ret;

        // Read in entire header
        while (bytesRead < RPCHeader.BYTE_LEN) {
            ret = this.clientIn.read(hdrBuff, bytesRead, RPCHeader.BYTE_LEN);
            if (ret > 0) {
                bytesRead += ret;
            }
        }
        RPCHeader hdr = new RPCHeader(this.hdrBuff);

        // Read in entire payload
        byte[] payload = new byte[(int) hdr.msgLen];
        bytesRead = 0;
        while (bytesRead < hdr.msgLen) {
            ret = this.clientIn.read(payload, bytesRead, RPCHeader.BYTE_LEN);
            if (ret > 0) {
                bytesRead += ret;
            }
        }
        return new RPCMsg(hdr, payload);
    }

    private void respond(RPCMsg msg) throws IOException {
        if (null == this.clientOut) {
            throw new IOException("No clients connected");
        }

        // Write out header
        this.clientOut.write(msg.hdr().toBytes());

        // Write out payload
        this.clientOut.write(msg.payload());

        // Make sure output is flushed
        this.clientOut.flush();
    }
}
