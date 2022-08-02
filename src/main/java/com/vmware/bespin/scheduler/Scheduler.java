/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler;

import com.vmware.bespin.rpc.RPCServer;
import com.vmware.bespin.rpc.TCPServer;

import com.vmware.bespin.scheduler.rpc.RPCID;
import com.vmware.bespin.scheduler.rpc.RegisterNodeHandler;
import com.vmware.bespin.scheduler.rpc.SchedulerHandler;
import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;

import com.vmware.bespin.scheduler.generated.tables.Applications;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.Placed;
import com.vmware.bespin.scheduler.generated.tables.Nodes;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class Scheduler {
    private final Model model;
    private final DSLContext conn;
    private final int maxReqsPerSolve;
    private final long maxTimePerSolve;
    private final long pollInterval;
    private final DatagramSocket udpSocket;
    private final InetAddress ip;
    private final int port;
    private static final Logger LOG = LogManager.getLogger(Scheduler.class);

    public static final Applications APPLICATION_TABLE = Applications.APPLICATIONS;
    public static final Pending PENDING_TABLE = Pending.PENDING;
    public static final Placed PLACED_TABLE = Placed.PLACED;
    public static final Nodes NODE_TABLE = Nodes.NODES;

    Scheduler(final Model model, final DSLContext conn, final int maxReqsPerSolve, final long maxTimePerSolve,
              final long pollInterval, final InetAddress ip, final int port) throws SocketException {
        this.model = model;
        this.conn = conn;
        this.maxReqsPerSolve = maxReqsPerSolve;
        this.maxTimePerSolve = maxTimePerSolve;
        this.pollInterval = pollInterval;
        this.udpSocket = new DatagramSocket();
        this.ip = ip;
        this.port = port;
    }

    public boolean runModelAndUpdateDB() throws IOException, NullPointerException {
        final Result<? extends Record> results;
        try {
            results = model.solve("PENDING");
        } catch (ModelException | SolverException e) {
            LOG.error(e);
            return false;
        }

        // TODO: should find a way to batch these, both to/from database but also in communication with controller
        // Add new assignments to placed, remove new assignments from pending, notify listener of changes
        for (final Record record : results) {
            // Extract fields from the record
            final Long recordId = (Long) record.get("ID");
            final Integer controllableNode = (Integer) record.get("CONTROLLABLE__NODE");
            final Integer cores = (Integer) record.get("CORES");
            final Integer memslices = (Integer) record.get("MEMSLICES");
            if (controllableNode == null) {
                throw new NullPointerException();
            }

            // Add to placed table
            conn.insertInto(PLACED_TABLE, PLACED_TABLE.APPLICATION, PLACED_TABLE.NODE, PLACED_TABLE.CORES,
                            PLACED_TABLE.MEMSLICES)
                    .values((Integer) record.get("APPLICATION"), controllableNode, cores, memslices)
                    .onDuplicateKeyUpdate()
                    .set(PLACED_TABLE.CORES,  PLACED_TABLE.CORES.plus(cores))
                    .set(PLACED_TABLE.MEMSLICES, PLACED_TABLE.MEMSLICES.plus(memslices))
                    .execute();
            // Delete from pending table
            conn.deleteFrom(PENDING_TABLE)
                    .where(PENDING_TABLE.ID.eq(recordId))
                    .execute();

            // TODO: move object creation out of loop
            final SchedulerAssignment assignment = new SchedulerAssignment(recordId, controllableNode.longValue());
            final DatagramPacket packet = new DatagramPacket(assignment.toBytes(), SchedulerAssignment.BYTE_LEN);
            packet.setAddress(this.ip);
            packet.setPort(this.port);
            this.udpSocket.send(packet);
            LOG.info("Sent allocation for {}", (Long) record.get("ID"));
            this.udpSocket.receive(packet);
        }
        return true;
    }

    private int getNumPendingRequests() {
        final String numRequests = "select count(1) from pending";
        return ((Long) this.conn.fetch(numRequests).get(0).getValue(0)).intValue();
    }

    public void run() throws InterruptedException, IOException {

        final Runnable rpcRunner =
                () -> {
                    try {
                        LOG.info("RPCServer thread started");
                        final RPCServer rpcServer = new TCPServer("172.31.0.20", 6970);
                        LOG.info("Created server");
                        rpcServer.register(RPCID.REGISTER_NODE, new RegisterNodeHandler(conn));
                        rpcServer.register(RPCID.SCHEDULER, new SchedulerHandler(conn));
                        LOG.info("Registered handlers");
                        rpcServer.addClient();
                        LOG.info("Added Client");
                        rpcServer.runServer();
                    } catch (final IOException e) {
                        LOG.error("RPCServer thread failed");
                        LOG.error(e);
                        throw new RuntimeException();
                    }
                };
        final Thread thread = new Thread(rpcRunner);
        thread.start();

        // Enter loop solve loop
        long lastSolve = System.currentTimeMillis();
        while (true) {
            // Sleep for poll interval
            Thread.sleep(this.pollInterval);

            // Get time elapsed since last solve
            final long timeElapsed = System.currentTimeMillis() - lastSolve;

            // Get number of rows
            final int numRequests = this.getNumPendingRequests();

            // If time since last solve is too long, solve
            if (timeElapsed >= this.maxTimePerSolve || numRequests >= this.maxReqsPerSolve) {
                if (numRequests > 0) {
                    if (timeElapsed >= this.maxTimePerSolve) {
                        LOG.info(String.format("solver thread solving due to timeout: numRequests = %d", numRequests));
                    } else {
                        LOG.info(String.format("solver thread solving due to numRequests = %d", numRequests));
                    }
                    // Only actually solve if work to do, exit if solver error
                    if (!this.runModelAndUpdateDB()) {
                        break;
                    }
                }
                lastSolve = System.currentTimeMillis();
            }
        } // while (true)
    }
}
