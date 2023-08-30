package com.vmware.bespin.scheduler.dinos;

/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

import com.vmware.bespin.rpc.RPCClient;
import com.vmware.bespin.rpc.RPCServer;
import com.vmware.bespin.rpc.TCPClient;
import com.vmware.bespin.rpc.TCPServer;
import com.vmware.bespin.scheduler.Scheduler;
import com.vmware.bespin.scheduler.SolverException;
import com.vmware.bespin.scheduler.dinos.rpc.AffinityAllocHandler;
import com.vmware.bespin.scheduler.dinos.rpc.AffinityReleaseHandler;
import com.vmware.bespin.scheduler.dinos.rpc.AllocHandler;
import com.vmware.bespin.scheduler.dinos.rpc.RPCID;
import com.vmware.bespin.scheduler.dinos.rpc.RegisterNodeHandler;
import com.vmware.bespin.scheduler.dinos.rpc.ReleaseHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;

public class DiNOSScheduler extends Scheduler {
    private final int maxReqsPerSolve;
    private final long maxTimePerSolve;
    private final long pollInterval;
    private final InetAddress ip;
    private final int serverPort;
    private final int clientPort;
    private RPCClient rpcClient;

    protected Logger LOG = LogManager.getLogger(DiNOSScheduler.class);

    DiNOSScheduler(final DSLContext conn, final int maxReqsPerSolve, final long maxTimePerSolve, 
            final long pollInterval, final InetAddress ip, final int serverPort, final int clientPort,
            final DiNOSSolver solver) throws SocketException {

        super(conn, solver);

        this.maxReqsPerSolve = maxReqsPerSolve;
        this.maxTimePerSolve = maxTimePerSolve;
        this.pollInterval = pollInterval;
        this.ip = ip;
        this.serverPort = serverPort;
        this.clientPort = clientPort;

        LOG.info("Running scheduler with parameters: maxReqsPerSolve={}, maxTimePerSolve={}, " +
                "pollInterval={}", maxReqsPerSolve, maxTimePerSolve, pollInterval);
    }

    @Override
    public boolean runSolverAndUpdateDB(final boolean printTimingData) throws IOException {
        final Result<? extends Record> results;
        final long start = System.currentTimeMillis();
        final long solveFinish;
        try {
            results = solver.solve(conn);
            solveFinish = System.currentTimeMillis();
        } catch (final SolverException e) {
            LOG.error(e);
            //if (this.getNumPendingRequests() > 0) {
                // TOOD: Notify requests they were not feasible. Drop them from the pending table.
            //}
            return false;
        }

        // Solver did no work
        if (results.isEmpty()) {
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

            final SchedulerAssignment assignment = new SchedulerAssignment(recordId, controllableNode.longValue());
            LOG.warn("Assigning alloc_id {} cores={} memslices={} to node {}", recordId, cores, memslices, 
                    controllableNode.longValue());
            this.rpcClient.call(RPCID.ALLOC_ASSIGNMENT, assignment.toBytes());
        }
        
        final long updateFinish = System.currentTimeMillis();
        if (printTimingData) {
            System.out.println(String.format("SOLVE_RESULTS: solve=%dms, solve_update=%dms", solveFinish - start, 
                    updateFinish - start));
        }
        return true;
    }

    public void run(final boolean printTimingData) throws InterruptedException, IOException {
        final Runnable rpcRunner = () -> {
            try {
                LOG.info("RPCServer thread started");
                final RPCServer<Scheduler> rpcServer = new TCPServer<Scheduler>("172.31.0.20", this.serverPort);
                LOG.info("Created server");
                rpcServer.register(RPCID.REGISTER_NODE, new RegisterNodeHandler());
                rpcServer.register(RPCID.ALLOC, new AllocHandler());
                rpcServer.register(RPCID.RELEASE, new ReleaseHandler());
                rpcServer.register(RPCID.AFFINITY_ALLOC, new AffinityAllocHandler());
                rpcServer.register(RPCID.AFFINITY_RELEASE, new AffinityReleaseHandler());
                LOG.info("Registered handlers");
                rpcServer.addClient();
                LOG.info("Server added client");
                this.rpcClient = new TCPClient(this.ip, this.clientPort);
                LOG.info("Added RPC client");
                this.rpcClient.connect();
                LOG.info("Connected RPC client");
                rpcServer.runServer(this);
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
            final long numRequests = getNumPendingRequests();

            // If time since last solve is too long, solve
            if (timeElapsed >= this.maxTimePerSolve || numRequests >= this.maxReqsPerSolve) {
                if (numRequests > 0) {
                    if (timeElapsed >= this.maxTimePerSolve) {
                        LOG.info(String.format("solver thread solving due to timeout: numRequests = %d", numRequests));
                    } else {
                        LOG.info(String.format("solver thread solving due to numRequests = %d", numRequests));
                    }
                    // Only actually solve if work to do, exit if solver error
                    if (!runSolverAndUpdateDB(printTimingData)) {
                        break;
                    }
                }
                lastSolve = System.currentTimeMillis();
            }
        } // while (true)
    }
}
