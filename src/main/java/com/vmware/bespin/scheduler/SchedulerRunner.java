package com.vmware.bespin.scheduler;

import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;

import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.Placed;
import com.vmware.bespin.scheduler.generated.tables.Nodes;

import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.cli.*;


class Scheduler
{
    final Model model;
    final DSLContext conn;

    final int maxReqsPerSolve;
    final int maxTimePerSolve;
    final int pollInterval;

    private static final Logger LOG = LogManager.getLogger(Scheduler.class);

    static final Pending pendingTable = Pending.PENDING;
    static final Placed placedTable = Placed.PLACED;
    static final Nodes nodeTable = Nodes.NODES;

    Scheduler(Model model, DSLContext conn, int maxReqsPerSolve, int maxTimePerSolve, int pollInterval) {
        this.model = model;
        this.conn = conn;
        this.maxReqsPerSolve = maxReqsPerSolve;
        this.maxTimePerSolve = maxTimePerSolve;
        this.pollInterval = pollInterval;
    }

    public boolean runModelAndUpdateDB() {
        Result<? extends Record> results;
        try {
            results = model.solve("PENDING");
        } catch (ModelException | SolverException e) {
            LOG.error(e);
            return false;
        }

        // TODO: should find a way to batch these
        // Add new assignments to placed, remove new assignments from pending
        results.forEach(r -> {
                    conn.insertInto(placedTable, placedTable.APPLICATION, placedTable.NODE, placedTable.CORES, placedTable.MEMSLICES)
                            .values((int) r.get("APPLICATION"), (int) r.get("CONTROLLABLE__NODE"), (int) r.get("CORES"), (int) r.get("MEMSLICES"))
                            .onDuplicateKeyUpdate()
                            .set(placedTable.CORES,  placedTable.CORES.plus((int) r.get("CORES")))
                            .set(placedTable.MEMSLICES, placedTable.MEMSLICES.plus((int) r.get("MEMSLICES")))
                            .execute();
                    conn.deleteFrom(pendingTable)
                            .where(pendingTable.ID.eq((int) r.get("ID")))
                            .execute();
                }
        );
        return true;
    }

    public void run() {
        // TODO: implement this
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
    private static final int MAX_TIME_PER_SOLVE_DEFAULT = 10; // in seconds
    private static final String POLL_INTERVAL_MILLIS_OPTION = "pollInterval";
    private static final int POLL_INTERVAL_MILLIS_DEFAULT = 100; // in milliseconds

    public static void main(String[] args) throws ClassNotFoundException {
        // These are the defaults for these parameters.
        // They should be overridden by commandline arguments.
        int numNodes = NUM_NODES_DEFAULT;
        int coresPerNode = CORES_PER_NODE_DEFAULT;
        int memslicesPerNode = MEMSLICES_PER_NODE_DEFAULT;
        int numApps = NUM_APPS_DEFAULT;
        boolean useCapFunction = USE_CAP_FUNCTION_DEFAULT;
        int maxReqsPerSolve = MAX_REQUESTS_PER_SOLVE_DEFAULT;
        int maxTimePerSolve = MAX_TIME_PER_SOLVE_DEFAULT;
        int pollInterval = POLL_INTERVAL_MILLIS_DEFAULT;

        // create Options object
        Options options = new Options();

        Option helpOption = Option.builder("h")
                .longOpt("help").argName("h")
                .hasArg(false)
                .desc("print help message")
                .build();
        Option numNodesOption = Option.builder("n")
                .longOpt(NUM_NODES_OPTION).argName(NUM_NODES_OPTION)
                .hasArg()
                .desc(String.format("number of nodes.\nDefault: %d", NUM_NODES_DEFAULT))
                .type(Integer.class)
                .build();
        Option coresPerNodeOption = Option.builder("c")
                .longOpt(CORES_PER_NODE_OPTION).argName(CORES_PER_NODE_OPTION)
                .hasArg()
                .desc(String.format("cores per node.\nDefault: %d", CORES_PER_NODE_DEFAULT))
                .type(Integer.class)
                .build();
        Option memslicesPerNodeOption = Option.builder("m")
                .longOpt(MEMSLICES_PER_NODE_OPTION).argName(MEMSLICES_PER_NODE_OPTION)
                .hasArg()
                .desc(String.format("number of 2 MB memory slices per node.\nDefault: %d", MEMSLICES_PER_NODE_DEFAULT))
                .type(Integer.class)
                .build();
        Option numAppsOption = Option.builder("p")
                .longOpt(NUM_APPS_OPTION).argName(NUM_APPS_OPTION)
                .hasArg()
                .desc(String.format("number of applications running on the cluster.\nDefault: %d", NUM_APPS_DEFAULT))
                .type(Integer.class)
                .build();
        Option useCapFunctionOption = Option.builder("f")
                .longOpt(USE_CAP_FUNCTION_OPTION).argName(USE_CAP_FUNCTION_OPTION)
                .hasArg()
                .desc(String.format("use capability function vs hand-written constraints.\nDefault: %b",
                        USE_CAP_FUNCTION_DEFAULT))
                .type(Boolean.class)
                .build();
        Option maxReqsPerSolveOption = Option.builder("r")
                .longOpt(MAX_REQUESTS_PER_SOLVE_OPTION).argName(MAX_REQUESTS_PER_SOLVE_OPTION)
                .hasArg()
                .desc(String.format("max number of new allocations request per solver iteration.\nDefault: %d",
                        MAX_REQUESTS_PER_SOLVE_DEFAULT))
                .type(Integer.class)
                .build();
        Option maxTimePerSolveOption = Option.builder("t")
                .longOpt(MAX_TIME_PER_SOLVE_OPTION).argName(MAX_TIME_PER_SOLVE_OPTION)
                .hasArg()
                .desc(String.format("max number of second between each solver iteration.\nDefault: %d",
                        MAX_REQUESTS_PER_SOLVE_DEFAULT))
                .type(Integer.class)
                .build();
        Option pollIntervalOption = Option.builder("p")
                .longOpt(POLL_INTERVAL_MILLIS_OPTION).argName(POLL_INTERVAL_MILLIS_OPTION)
                .hasArg()
                .desc(String.format("interval to check if the solver should run in milliseconds.\nDefault: %d",
                        POLL_INTERVAL_MILLIS_DEFAULT))
                .type(Integer.class)
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

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            if(cmd.hasOption("h")) {
                // automatically generate the help statement
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "java -jar -Dlog4j.configurationFile=src/main/resources/log4j2.xml " +
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
            if (cmd.hasOption(POLL_INTERVAL_MILLIS_OPTION)) {
                pollInterval = Integer.parseInt(cmd.getOptionValue(POLL_INTERVAL_MILLIS_OPTION));
            }
        } catch (ParseException e) {
            LOG.error("Failed to parse command line");
            return;
        }

        // Create an in-memory database and get a JOOQ connection to it
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        initDB(conn, numNodes, coresPerNode, memslicesPerNode, numApps, 0, false);

        LOG.info("Running solver with parameters: nodes={}, coresPerNode={}, memSlicesPerNode={}, " +
                        "numApps={}, useCapFunction={}, maxReqsPerSolve={}, maxTimePerSolve={}, pollInterval={}",
                numNodes, coresPerNode, memslicesPerNode, numApps, useCapFunction, maxReqsPerSolve, maxTimePerSolve,
                pollInterval);
        Model model = createModel(conn, useCapFunction);
        Scheduler scheduler = new Scheduler(model, conn, maxReqsPerSolve, maxTimePerSolve, pollInterval);
        scheduler.run();
    }
}