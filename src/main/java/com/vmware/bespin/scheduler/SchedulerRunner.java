/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler;

import com.vmware.bespin.rpc.RPCHandler;
import com.vmware.bespin.rpc.RPCHeader;
import com.vmware.bespin.rpc.RPCMessage;
import com.vmware.bespin.rpc.RPCServer;
import com.vmware.bespin.rpc.TCPServer;

import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;

import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.Placed;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

class Scheduler {
    private final Model model;
    private final DSLContext conn;
    private final int maxReqsPerSolve;
    private final long maxTimePerSolve;
    private final long pollInterval;
    private final DatagramSocket udpSocket;
    private final InetAddress ip;
    private final int port;
    int requestsReceived = 0;
    int allocationsSent = 0;
    private static final Logger LOG = LogManager.getLogger(Scheduler.class);

    static final Pending PENDING_TABLE = Pending.PENDING;
    static final Placed PLACED_TABLE = Placed.PLACED;

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

    public boolean runModelAndUpdateDB() throws IOException {
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
            conn.insertInto(PLACED_TABLE, PLACED_TABLE.APPLICATION, PLACED_TABLE.NODE, PLACED_TABLE.CORES,
                            PLACED_TABLE.MEMSLICES)
                    .values((Integer) record.get("APPLICATION"), (Integer) record.get("CONTROLLABLE__NODE"), 
                            (Integer) record.get("CORES"), (Integer) record.get("MEMSLICES"))
                    .onDuplicateKeyUpdate()
                    .set(PLACED_TABLE.CORES,  PLACED_TABLE.CORES.plus((Integer) record.get("CORES")))
                    .set(PLACED_TABLE.MEMSLICES, PLACED_TABLE.MEMSLICES.plus((Integer) record.get("MEMSLICES")))
                    .execute();
            conn.deleteFrom(PENDING_TABLE)
                    .where(PENDING_TABLE.ID.eq((Long) record.get("ID")))
                    .execute();

                // TODO: move allocation out of loop
                final SchedulerAssignment assignment = new SchedulerAssignment((Long) record.get("ID"), ((Integer) 
                record.get("CONTROLLABLE__NODE")).longValue());
                final DatagramPacket packet = new DatagramPacket(assignment.toBytes(), SchedulerAssignment.BYTE_LEN);
                packet.setAddress(this.ip);
                packet.setPort(this.port);
                this.udpSocket.send(packet);
                this.allocationsSent += 1;
                LOG.info("Sent allocation for {}", (Long) record.get("ID"));
                this.udpSocket.receive(packet);
        }

        LOG.info("Requests Recevied: {}, Allocations Sent: {}", this.requestsReceived, this.allocationsSent);
        return true;
    }

    private int getNumPendingRequests() {
        final String numRequests = "select count(1) from pending";
        return ((Long) this.conn.fetch(numRequests).get(0).getValue(0)).intValue();
    }

    public void run() throws InterruptedException, IOException {

        class RequestHandler extends RPCHandler {
            private long requestId = 0;
            private Scheduler scheduler;

            public RequestHandler(final Scheduler scheduler) {
                this.scheduler = scheduler;
            }

            @Override
            public RPCMessage handleRPC(final RPCMessage msg) {
                final RPCHeader hdr = msg.hdr();
                assert (hdr.msgLen == SchedulerRequest.BYTE_LEN);
                final SchedulerRequest req = new SchedulerRequest(msg.payload());

                conn.insertInto(PENDING_TABLE)
                        .set(PENDING_TABLE.ID, requestId)
                        .set(PENDING_TABLE.APPLICATION, (int) req.application)
                        .set(PENDING_TABLE.CORES, (int) req.cores)
                        .set(PENDING_TABLE.MEMSLICES, (int) req.memslices)
                        .set(PENDING_TABLE.STATUS, "PENDING")
                        .set(PENDING_TABLE.CURRENT_NODE, -1)
                        .set(PENDING_TABLE.CONTROLLABLE__NODE, (Field<Integer>) null)
                        .execute();

                hdr.msgLen = SchedulerResponse.BYTE_LEN;
                requestId++;
                scheduler.requestsReceived++;
                return new RPCMessage(hdr, new SchedulerResponse(requestId).toBytes());
            }
        }

        final Runnable rpcRunner =
                () -> {
                    try {
                        LOG.info("RPCServer thread started");
                        final RPCServer rpcServer = new TCPServer("172.31.0.20", 6970);
                        LOG.info("Created server");
                        rpcServer.register((byte) 1, new RequestHandler(this));
                        LOG.info("Registered handler");
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
                if (timeElapsed >= this.maxTimePerSolve) {
                    LOG.info(String.format("solver thread solving due to timeout: numRequests = %d", numRequests));
                } else {
                    LOG.info(String.format("solver thread solving due to numRequests = %d", numRequests));
                }
                // Only actually solve if work to do
                if (numRequests > 0) {
                    // Exit if solver error
                    if (!this.runModelAndUpdateDB()) {
                        break;
                    }
                }
                lastSolve = System.currentTimeMillis();
            }
        } // while (true)
    }
}


public class SchedulerRunner extends DCMRunner {
    private static final Logger LOG = LogManager.getLogger(SchedulerRunner.class);
    private static final String NUM_NODES_OPTION = "numNodes";
    private static final int NUM_NODES_DEFAULT = 64;
    private static final String CORES_PER_NODE_OPTION = "coresPerNode";
    private static final int CORES_PER_NODE_DEFAULT = 128;
    private static final String MEMSLICES_PER_NODE_OPTION = "memslicesPerNode";
    private static final int MEMSLICES_PER_NODE_DEFAULT = 256;
    private static final String NUM_APPS_OPTION = "numApps";
    private static final int NUM_APPS_DEFAULT = 20;
    private static final String USE_CAP_FUNCTION_OPTION = "useCapFunction";
    private static final boolean USE_CAP_FUNCTION_DEFAULT = true;
    private static final String MAX_REQUESTS_PER_SOLVE_OPTION = "maxReqsPerSolve";
    private static final int MAX_REQUESTS_PER_SOLVE_DEFAULT = 15;
    private static final String MAX_TIME_PER_SOLVE_OPTION = "maxTimePerSolve";
    private static final int MAX_TIME_PER_SOLVE_DEFAULT = 10; // in milliseconds
    private static final String POLL_INTERVAL_OPTION = "pollInterval";
    private static final int POLL_INTERVAL_DEFAULT = 500; // 1/2 second in milliseconds

    public static void main(final String[] args) throws ClassNotFoundException, InterruptedException, 
            SocketException, UnknownHostException, IOException {
        // These are the defaults for these parameters.
        // They should be overridden by commandline arguments.
        int numNodes = NUM_NODES_DEFAULT;
        int coresPerNode = CORES_PER_NODE_DEFAULT;
        int memslicesPerNode = MEMSLICES_PER_NODE_DEFAULT;
        int numApps = NUM_APPS_DEFAULT;
        boolean useCapFunction = USE_CAP_FUNCTION_DEFAULT;
        int maxReqsPerSolve = MAX_REQUESTS_PER_SOLVE_DEFAULT;
        long maxTimePerSolve = MAX_TIME_PER_SOLVE_DEFAULT;
        long pollInterval = POLL_INTERVAL_DEFAULT;

        // create Options object
        final Options options = new Options();

        final Option helpOption = Option.builder("h")
                .longOpt("help").argName("h")
                .hasArg(false)
                .desc("print help message")
                .build();
        final Option numNodesOption = Option.builder("n")
                .longOpt(NUM_NODES_OPTION).argName(NUM_NODES_OPTION)
                .hasArg()
                .desc(String.format("number of nodes.%nDefault: %d", NUM_NODES_DEFAULT))
                .type(Integer.class)
                .build();
        final Option coresPerNodeOption = Option.builder("c")
                .longOpt(CORES_PER_NODE_OPTION).argName(CORES_PER_NODE_OPTION)
                .hasArg()
                .desc(String.format("cores per node.%nDefault: %d", CORES_PER_NODE_DEFAULT))
                .type(Integer.class)
                .build();
        final Option memslicesPerNodeOption = Option.builder("m")
                .longOpt(MEMSLICES_PER_NODE_OPTION).argName(MEMSLICES_PER_NODE_OPTION)
                .hasArg()
                .desc(String.format("number of 2 MB memory slices per node.%nDefault: %d", MEMSLICES_PER_NODE_DEFAULT))
                .type(Integer.class)
                .build();
        final Option numAppsOption = Option.builder("p")
                .longOpt(NUM_APPS_OPTION).argName(NUM_APPS_OPTION)
                .hasArg()
                .desc(String.format("number of applications running on the cluster.%nDefault: %d", NUM_APPS_DEFAULT))
                .type(Integer.class)
                .build();
        final Option useCapFunctionOption = Option.builder("f")
                .longOpt(USE_CAP_FUNCTION_OPTION).argName(USE_CAP_FUNCTION_OPTION)
                .hasArg()
                .desc(String.format("use capability function vs hand-written constraints.%nDefault: %b",
                        USE_CAP_FUNCTION_DEFAULT))
                .type(Boolean.class)
                .build();
        final Option maxReqsPerSolveOption = Option.builder("r")
                .longOpt(MAX_REQUESTS_PER_SOLVE_OPTION).argName(MAX_REQUESTS_PER_SOLVE_OPTION)
                .hasArg()
                .desc(String.format("max number of new allocations request per solver iteration.%nDefault: %d",
                        MAX_REQUESTS_PER_SOLVE_DEFAULT))
                .type(Integer.class)
                .build();
        final Option maxTimePerSolveOption = Option.builder("t")
                .longOpt(MAX_TIME_PER_SOLVE_OPTION).argName(MAX_TIME_PER_SOLVE_OPTION)
                .hasArg()
                .desc(String.format("max number of second between each solver iteration.%nDefault: %d",
                        MAX_REQUESTS_PER_SOLVE_DEFAULT))
                .type(Long.class)
                .build();
        final Option pollIntervalOption = Option.builder("p")
                .longOpt(POLL_INTERVAL_OPTION).argName(POLL_INTERVAL_OPTION)
                .hasArg()
                .desc(String.format("interval to check if the solver should run in milliseconds.%nDefault: %d",
                        POLL_INTERVAL_DEFAULT))
                .type(Long.class)
                .build();

        options.addOption(helpOption);
        options.addOption(numNodesOption);
        options.addOption(coresPerNodeOption);
        options.addOption(memslicesPerNodeOption);
        options.addOption(numAppsOption);
        options.addOption(useCapFunctionOption);
        options.addOption(maxReqsPerSolveOption);
        options.addOption(maxTimePerSolveOption);
        options.addOption(maxTimePerSolveOption);
        options.addOption(pollIntervalOption);

        final CommandLineParser parser = new DefaultParser();
        try {
            final CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                // automatically generate the help statement
                final HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("java -jar -Dlog4j.configurationFile=src/main/resources/log4j2.xml " +
                                "target/scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar [options]",
                        options);
                return;
            }
            if (cmd.hasOption(NUM_NODES_OPTION)) {
                numNodes = Integer.parseInt(cmd.getOptionValue(NUM_NODES_OPTION));
            }
            if (cmd.hasOption(CORES_PER_NODE_OPTION)) {
                coresPerNode = Integer.parseInt(cmd.getOptionValue(CORES_PER_NODE_OPTION));
            }
            if (cmd.hasOption(MEMSLICES_PER_NODE_OPTION)) {
                memslicesPerNode = Integer.parseInt(cmd.getOptionValue(MEMSLICES_PER_NODE_OPTION));
            }
            if (cmd.hasOption(NUM_APPS_OPTION)) {
                numApps = Integer.parseInt(cmd.getOptionValue(NUM_APPS_OPTION));
            }
            if (cmd.hasOption(USE_CAP_FUNCTION_OPTION)) {
                useCapFunction = Boolean.parseBoolean(cmd.getOptionValue(USE_CAP_FUNCTION_OPTION));
            }
            if (cmd.hasOption(MAX_REQUESTS_PER_SOLVE_OPTION)) {
                maxReqsPerSolve = Integer.parseInt(cmd.getOptionValue(MAX_REQUESTS_PER_SOLVE_OPTION));
            }
            if (cmd.hasOption(MAX_TIME_PER_SOLVE_OPTION)) {
                maxTimePerSolve = Integer.parseInt(cmd.getOptionValue(MAX_TIME_PER_SOLVE_OPTION));
            }
            if (cmd.hasOption(POLL_INTERVAL_OPTION)) {
                pollInterval = Long.parseLong(cmd.getOptionValue(POLL_INTERVAL_OPTION));
            }
        } catch (final ParseException ignored) {
            LOG.error("Failed to parse command line");
            return;
        }

        // Create an in-memory database and get a JOOQ connection to it
        Class.forName("org.h2.Driver");
        final DSLContext conn = DSL.using("jdbc:h2:mem:");
        initDB(conn, numNodes, coresPerNode, memslicesPerNode, numApps, 0, false);

        LOG.info("Running solver with parameters: nodes={}, coresPerNode={}, memSlicesPerNode={}, " +
                        "numApps={}, useCapFunction={}, maxReqsPerSolve={}, maxTimePerSolve={}, pollInterval={}",
                numNodes, coresPerNode, memslicesPerNode, numApps, useCapFunction, maxReqsPerSolve, maxTimePerSolve,
                pollInterval);
        final Model model = createModel(conn, useCapFunction);
        final Scheduler scheduler = new Scheduler(model, conn, maxReqsPerSolve, maxTimePerSolve, pollInterval, 
            InetAddress.getByName("172.31.0.11"), 6971);
        scheduler.run();
    }
}