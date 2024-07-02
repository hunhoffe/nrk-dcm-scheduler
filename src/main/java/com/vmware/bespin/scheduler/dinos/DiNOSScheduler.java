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
import com.vmware.bespin.scheduler.Solver;
import com.vmware.bespin.scheduler.dinos.rpc.AffinityAllocHandler;
import com.vmware.bespin.scheduler.dinos.rpc.AffinityReleaseHandler;
import com.vmware.bespin.scheduler.dinos.rpc.AllocHandler;
import com.vmware.bespin.scheduler.dinos.rpc.RPCID;
import com.vmware.bespin.scheduler.dinos.rpc.RegisterNodeHandler;
import com.vmware.bespin.scheduler.dinos.rpc.ReleaseHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class DiNOSScheduler extends Scheduler {
    private final int maxReqsPerSolve;
    private final long maxTimePerSolve;
    private final long pollInterval;
    private final InetAddress ip;
    private final int serverPort;
    private final int clientPort;
    private RPCClient rpcClient;
    public final ExecutorService workerPool;
    private boolean calledShutdown;

    protected Logger LOG = LogManager.getLogger(DiNOSScheduler.class);

    DiNOSScheduler(final DSLContext conn, final int maxReqsPerSolve, final long maxTimePerSolve, 
            final long pollInterval, final InetAddress ip, final int serverPort, final int clientPort,
            final Solver solver, final boolean verbose) throws SocketException {

        super(conn, solver, verbose);

        this.maxReqsPerSolve = maxReqsPerSolve;
        this.maxTimePerSolve = maxTimePerSolve;
        this.pollInterval = pollInterval;
        this.ip = ip;
        this.serverPort = serverPort;
        this.clientPort = clientPort;
        this.workerPool = Executors.newFixedThreadPool(4);
        this.calledShutdown = false;

        // this is a hack so we don't have to register new applications (for now)
        for (long i = 0; i < 3; i++) {
            addApplication(i);
        }

        LOG.warn("Running scheduler with parameters: maxReqsPerSolve={}, maxTimePerSolve={}, " +
                "pollInterval={} solver={} verbose={}", 
                maxReqsPerSolve, maxTimePerSolve, pollInterval, solver.getClass().toString(), verbose);
    }

    @Override
    public boolean runSolverAndUpdateDB() throws IOException {
        final Result<? extends Record> results;
        final long start = System.currentTimeMillis();
        final long solveFinish;
        try {
            results = solver.solve(conn, this);
            solveFinish = System.currentTimeMillis();
        } catch (final com.vmware.bespin.scheduler.SolverException e) {
            LOG.error(e);
            final Long errReturn = new Long(-1);
            if (this.getNumPendingRequests() > 0) {
                final long[] pendingRequestIds = getPendingRequestIDs();
                for (final long requestId : pendingRequestIds) {
                    final SchedulerAssignment assignment = new SchedulerAssignment(requestId, errReturn);
                    LOG.warn("Assigning error ({}) for alloc_id {}", errReturn, requestId);
                    this.rpcClient.call(RPCID.ALLOC_ASSIGNMENT, assignment.toBytes());
                }
            }
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
        if (this.verbose) {
            System.out.println(String.format("SOLVE_RESULTS: solve=%dms, solve_update=%dms", solveFinish - start, 
                    updateFinish - start));
            System.out.println(conn.fetch("select * from placed"));
        }
        return true;
    }

    public void run() throws InterruptedException, IOException {
        final RPCServer<DiNOSScheduler> rpcServer = new TCPServer<DiNOSScheduler>("172.31.0.20", this.serverPort);
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
        final Runnable rpcRunner = () -> {
            try {
                LOG.info("RPCServer thread started");
                rpcServer.runServer(this);
            } catch (final IOException e) {
                LOG.error("RPCServer thread failed");
                LOG.error(e);
                throw new RuntimeException();
            }
        };
        final Thread rpcThread = new Thread(rpcRunner);
        rpcThread.start();

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if (workerPool != null) {
                    LOG.warn("Waiting for worker pool to shut down");
                    workerPool.shutdown(); // Disable new tasks from being submitted
                    try {
                        // Wait a while for existing tasks to terminate
                        if (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                            workerPool.shutdownNow();
                            if (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                                LOG.error("Pool did not terminate");
                            }
                        }
                    } catch (final InterruptedException ignored) {
                        // (Re-)Cancel if current thread also interrupted
                        workerPool.shutdownNow();
                        // Preserve interrupt status
                        Thread.currentThread().interrupt();
                    }
                }
                if (rpcThread.isAlive()) {
                    LOG.warn("Waiting for RPC server to shutdown...");
                    rpcServer.stopServer();
                    try {
                        rpcThread.join();
                    } catch (final InterruptedException e) {
                        LOG.error("Failed to wait for RPC server to stop");
                        LOG.error(e.toString());
                    }
                    if (!calledShutdown) {
                        try {
                            LOG.warn("Shutting down main thread...");
                            mainThread.join();
                        } catch (final InterruptedException e) {
                            LOG.error("Failed to wait for main thread to stop");
                            LOG.error(e.toString());
                        }
                    }
                }
                rpcClient.cleanUp();
                LOG.warn("Exiting.");
            }
        });

        // Enter loop solve loop
        long lastSolve = System.currentTimeMillis();
        while (rpcThread.isAlive()) {
            // Sleep for poll interval
            Thread.sleep(this.pollInterval);

            // Get time elapsed since last solve
            final long timeElapsed = System.currentTimeMillis() - lastSolve;

            // Get number of rows
            try {
                final long numRequests = getNumPendingRequests();

                // If time since last solve is too long, solve
                if (timeElapsed >= this.maxTimePerSolve || numRequests >= this.maxReqsPerSolve) {
                    if (numRequests > 0) {
                        if (timeElapsed >= this.maxTimePerSolve) {
                            LOG.info(String.format(
                                "solver thread solving due to timeout: numRequests = %d", numRequests));
                        } else {
                            LOG.info(String.format("solver thread solving due to numRequests = %d", numRequests));
                        }
                        // Only actually solve if work to do, exit if solver error
                        if (!runSolverAndUpdateDB()) {
                            LOG.error("Solver failed unexpectedly.");
                            this.calledShutdown = true;
                            System.exit(-1);
                        }
                    }
                    lastSolve = System.currentTimeMillis();
                }
            } catch (final DataAccessException e) {
                LOG.error("Database closed unexpectedly.");
                LOG.error(e.toString());
            }
        }
    }
}
