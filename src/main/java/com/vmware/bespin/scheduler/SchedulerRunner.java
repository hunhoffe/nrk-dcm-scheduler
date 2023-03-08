/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler;

import com.vmware.bespin.rpc.RPCServer;
import com.vmware.bespin.rpc.TCPServer;

import com.vmware.bespin.scheduler.rpc.RPCID;
import com.vmware.bespin.scheduler.rpc.RegisterNodeHandler;
import com.vmware.bespin.scheduler.rpc.AllocHandler;
import com.vmware.bespin.scheduler.rpc.ReleaseHandler;
import com.vmware.bespin.scheduler.rpc.AffinityAllocHandler;
import com.vmware.bespin.scheduler.rpc.AffinityReleaseHandler;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

import java.io.IOException;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class SchedulerRunner extends DCMRunner {
    private final int maxReqsPerSolve;
    private final long maxTimePerSolve;
    private final long pollInterval;
    private final DatagramSocket udpSocket;
    private final InetAddress ip;
    private final int port;

    SchedulerRunner(final DSLContext conn, final int numNodes, final int coresPerNode, 
            final int memslicesPerNode, final int numApps, final Integer randomSeed, 
            final boolean useCapFunc, final boolean usePrintDiagnostics, final int maxReqsPerSolve, 
            final long maxTimePerSolve, final long pollInterval, final InetAddress ip, final int port) 
    throws SocketException {

        super(conn, numNodes, coresPerNode, memslicesPerNode, numApps, randomSeed, useCapFunc, false);

        this.maxReqsPerSolve = maxReqsPerSolve;
        this.maxTimePerSolve = maxTimePerSolve;
        this.pollInterval = pollInterval;
        this.udpSocket = new DatagramSocket();
        this.ip = ip;
        this.port = port;

        LOG.info("Running scheduler with parameters: useCapFunction={}, maxReqsPerSolve={}, " +
            "maxTimePerSolve={}, pollInterval={}",
            useCapFunc, maxReqsPerSolve, maxTimePerSolve, pollInterval);
    }

    @Override
    public boolean runModelAndUpdateDB(final boolean printTimingData) throws IOException {
        final Result<? extends Record> results;
        final long start = System.currentTimeMillis();
        final long solveFinish;
         try {
             results = model.solve("PENDING");
             solveFinish = System.currentTimeMillis();
         } catch (ModelException | SolverException e) {
             LOG.error(e);
             return false;
         }
 
         // TODO: should find a way to batch these
         // Add new assignments to placed, remove new assignments from pending
         for (final Record r : results) {
            // Extract fields from the record
            final Long recordId = (Long) r.get("ID");
            final Integer controllableNode = (Integer) r.get("CONTROLLABLE__NODE");
            final Integer cores = (Integer) r.get("CORES");
            final Integer memslices = (Integer) r.get("MEMSLICES");
            final Integer application = (Integer) r.get("APPLICATION");

             updateAllocation(controllableNode, application, cores, memslices);
             conn.deleteFrom(PENDING_TABLE)
                     .where(PENDING_TABLE.ID.eq(recordId))
                     .execute();

            // TODO: move object creation out of loop
            final SchedulerAssignment assignment = new SchedulerAssignment(recordId, controllableNode.longValue());
            final DatagramPacket packet = new DatagramPacket(assignment.toBytes(), SchedulerAssignment.BYTE_LEN);
            packet.setAddress(this.ip);
            packet.setPort(this.port);
            this.udpSocket.send(packet);
            LOG.info("Sent allocation for {}", recordId);
            this.udpSocket.receive(packet);
        }
        
        final long updateFinish = System.currentTimeMillis();
        if (printTimingData) {
             LOG.info("SOLVE_RESULTS: solve={}ms, solve_update={}ms", solveFinish - start, updateFinish - start);
        }
        return true;
    }

    public void run(final boolean printTimingData) throws InterruptedException, IOException {

        final Runnable rpcRunner =
                () -> {
                    try {
                        LOG.info("RPCServer thread started");
                        final RPCServer rpcServer = new TCPServer("172.31.0.20", 6970);
                        LOG.info("Created server");
                        rpcServer.register(RPCID.REGISTER_NODE, new RegisterNodeHandler(conn));
                        rpcServer.register(RPCID.ALLOC, new AllocHandler(conn));
                        rpcServer.register(RPCID.RELEASE, new ReleaseHandler(conn));
                        rpcServer.register(RPCID.AFFINITY_ALLOC, new AffinityAllocHandler(conn));
                        rpcServer.register(RPCID.AFFINITY_RELEASE, new AffinityReleaseHandler(conn));
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
            final int numRequests = getNumPendingRequests();

            // If time since last solve is too long, solve
            if (timeElapsed >= this.maxTimePerSolve || numRequests >= this.maxReqsPerSolve) {
                if (numRequests > 0) {
                    if (timeElapsed >= this.maxTimePerSolve) {
                        LOG.info(String.format("solver thread solving due to timeout: numRequests = %d", numRequests));
                    } else {
                        LOG.info(String.format("solver thread solving due to numRequests = %d", numRequests));
                    }
                    // Only actually solve if work to do, exit if solver error
                    if (!runModelAndUpdateDB(printTimingData)) {
                        break;
                    }
                }
                lastSolve = System.currentTimeMillis();
            }
        } // while (true)
    }
}
