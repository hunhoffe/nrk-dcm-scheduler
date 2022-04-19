package com.vmware.bespin.scheduler;

import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;

import com.vmware.bespin.scheduler.generated.tables.Nodes;
import com.vmware.bespin.scheduler.generated.tables.Applications;
import com.vmware.bespin.scheduler.generated.tables.Allocations;

import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

class Simulation
{
    final Model model;
    final DSLContext conn;
    private int counter;

    private static final Logger LOG = LogManager.getLogger(Simulation.class);

    Simulation(Model model, DSLContext conn) {
        this.model = model;
        this.conn = conn;
        this.counter = 1;
    }

    public void createTurnover() {

        // TODO: actually calculate turnover

        final Allocations allocTable = Allocations.ALLOCATIONS;
        for (int i = 1; i <= 5; i++) {
            // TODO: fix application & id value for core
            conn.insertInto(allocTable)
                    .set(allocTable.ID, 256*64 + this.counter*5 + i)
                    .set(allocTable.APPLICATION, 1)
                    .set(allocTable.CORES, 1)
                    .set(allocTable.STATUS, "PENDING")
                    .set(allocTable.CURRENT_NODE, -1)
                    .set(allocTable.CONTROLLABLE__NODE, (Field<Integer>) null)
                    .execute();
        }
        this.counter++;
    }

    public boolean runModelAndUpdateDB() {
        Result<? extends Record> results = null;
        try {
            results = model.solve("ALLOCATIONS");
        } catch (ModelException e) {
            LOG.info("Got a model exception when solving: {}", e);
            return false;
        }

        if (results == null) {
            return false;
        }

        final List<Update<?>> updates = new ArrayList<>();
        LOG.info("new allocations placed on:");
        results.forEach(r -> {
                    final Integer coreId = (Integer) r.get("ID");
                    final Integer currentNode = (Integer) r.get("CONTROLLABLE__NODE");
                    updates.add(
                            conn.update(DSL.table("ALLOCATIONS"))
                                    .set(DSL.field("CURRENT_NODE"), currentNode)
                                    .set(DSL.field("CONTROLLABLE__NODE"), currentNode)
                                    .set(DSL.field("STATUS"), "PLACED")
                                    .where(DSL.field("ID").eq(coreId)
                                            .and(DSL.field("STATUS").eq("PENDING")))
            );
                }
        );
        conn.batch(updates).execute();

        // TODO: remove this
        System.out.println(conn.fetch("select * from allocations"));
        System.out.println(conn.fetch("select nodes.id, nodes.cores - sum(allocations.cores) as core_spare " +
                "from allocations " +
                "join nodes " +
                "  on nodes.id = allocations.controllable__node " +
                "group by nodes.id, nodes.cores"));
        return true;
    }

    public boolean checkForCapacityViolation() {
        // TODO: check per-node capacity
        return false;
    }
}

public class SimulationRunner {
    private static final Logger LOG = LogManager.getLogger(SimulationRunner.class);
    private static final String NUM_STEPS_OPTION = "steps";
    private static final String NUM_NODES_OPTION = "numNodes";
    private static final String CORES_PER_NODE_OPTION = "coresPerNode";
    private static final String MEM_SLICES_PER_NODE_OPTION = "memSlicesPerNode";

    private static void setupDb(final DSLContext conn) {
        final InputStream resourceAsStream = SimulationRunner.class.getResourceAsStream("/bespin_tables.sql");
        try (final BufferedReader tables = new BufferedReader(new InputStreamReader(resourceAsStream,
                StandardCharsets.UTF_8))) {
            // Create a fresh database
            final String schemaAsString = tables.lines()
                    .filter(line -> !line.startsWith("--")) // remove SQL comments
                    .collect(Collectors.joining("\n"));
            final String[] createStatements = schemaAsString.split(";");
            for (final String createStatement: createStatements) {
                conn.execute(createStatement);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void initDB(final DSLContext conn, int numNodes, int coresPerNode, int memSlicesPerNode) {
        Random rand = new Random();
        setupDb(conn);

        // Add nodes with specified cores
        final Nodes t = Nodes.NODES;
        for (int i = 1; i <= numNodes; i++) {
            conn.insertInto(t)
                    .set(t.ID, i)
                    .set(t.CORES, coresPerNode)
                    .execute();
        }

        // Randomly generate applications between 10 and 25
        int numApplications = 10 + rand.nextInt(15);
        final Applications appTable = Applications.APPLICATIONS;
        for (int i = 1; i <= numApplications; i++) {
            // Add initial applications
            conn.insertInto(appTable)
                    .set(appTable.ID, i)
                    .execute();
        }

        // Randomly add core requests for 70% capacity of cluster
        int coreRequests = numNodes * coresPerNode;
        coreRequests = (int) Math.ceil((float) coreRequests * 0.10);
        final Allocations allocTable = Allocations.ALLOCATIONS;
        int node = 0;
        for (int i = 1; i <= coreRequests; i++) {
            node = rand.nextInt(numNodes) + 1;
            conn.insertInto(allocTable)
                    .set(allocTable.ID, i)
                    .set(allocTable.APPLICATION, rand.nextInt(numApplications) + 1)
                    .set(allocTable.CORES, 1)
                    .set(allocTable.STATUS, "PLACED")
                    .set(allocTable.CURRENT_NODE, node)
                    .set(allocTable.CONTROLLABLE__NODE, node)
                    .execute();
        }
    }

    private static Model createModel(final DSLContext conn) {
        // Only PENDING core requests are placed
        // All DCM solvers need to have this constraint
        final String placed_constraint = "create constraint placed_constraint as " +
                " select * from allocations where status = 'PLACED' check current_node = controllable__node";

        // Add capacity view
        final String capacity_core_view = "create constraint spare_cores as " +
                "select nodes.id, nodes.cores - sum(allocations.cores) as core_spare " +
                "from allocations " +
                "join nodes " +
                "  on nodes.id = allocations.controllable__node " +
                "group by nodes.id, nodes.cores";

        // Create capacity constraint
        final String capacity_core_constraint = "create constraint capacity_core_constraint as " +
                " select * from spare_cores check core_spare >= 0";

        OrToolsSolver.Builder b = new OrToolsSolver.Builder();
        return Model.build(conn, b.build(), List.of(placed_constraint, capacity_core_view, capacity_core_constraint));
    }

    private static void printStats(final DSLContext conn) {
        // TODO: write this method
        //LOG.info("Total capacity provisioned: {}", conn.fetch("select SUM(max_capacity) from components"));
    }

    public static void main(String[] args) throws ClassNotFoundException {

        int i = 0;
        // These are the defaults for these parameters.
        // They should be overridden by commandline arguments.
        int numSteps = 10;
        int numNodes = 5;
        int coresPerNode = 10;
        int memSlicesPerNode = 256;

        // create Options object
        Options options = new Options();

        // TODO: handle defaults better
        Option numStepsOption = Option.builder(NUM_STEPS_OPTION)
                .hasArg(true)
                .desc("maximum number of steps to run the simulations.\nDefault: 10")
                .type(Integer.class)
                .build();
        Option numNodesOption = Option.builder(NUM_NODES_OPTION)
                .hasArg()
                .desc("number of nodes.\nDefault: 64")
                .type(Integer.class)
                .build();
        Option coresPerNodeOption = Option.builder(CORES_PER_NODE_OPTION)
                .hasArg()
                .desc("cores per node.\nDefault: 128")
                .type(Integer.class)
                .build();
        Option memSlicesPerNodeOption = Option.builder(MEM_SLICES_PER_NODE_OPTION)
                .hasArg()
                .desc("number of 2 MB memory slices per node.\nDefault: 256")
                .type(Integer.class)
                .build();

        options.addOption("h", false, "print help message");
        options.addOption(numStepsOption);
        options.addOption(numNodesOption);
        options.addOption(coresPerNodeOption);
        options.addOption(memSlicesPerNodeOption);

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse( options, args);
            if(cmd.hasOption("h")) {
                // automatically generate the help statement
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "java -jar -Dlog4j.configurationFile=src/main/resources/log4j2.xml " +
                                "target/scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar [options]",
                        options);
                return;
            }
            if (cmd.hasOption(NUM_STEPS_OPTION)) {
                numSteps = Integer.parseInt(cmd.getOptionValue(NUM_STEPS_OPTION));
            }
            if (cmd.hasOption(NUM_NODES_OPTION)) {
                numNodes = Integer.parseInt(cmd.getOptionValue(NUM_NODES_OPTION));
            }
            if (cmd.hasOption(CORES_PER_NODE_OPTION)) {
                coresPerNode = Integer.parseInt(cmd.getOptionValue(CORES_PER_NODE_OPTION));
            }
            if (cmd.hasOption(MEM_SLICES_PER_NODE_OPTION)) {
                memSlicesPerNode = Integer.parseInt(cmd.getOptionValue(CORES_PER_NODE_OPTION));
            }
        } catch (ParseException e) {
            LOG.error("Failed to parse command line");
            return;
        }

        // Create an in-memory database and get a JOOQ connection to it
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        initDB(conn, numNodes, coresPerNode, memSlicesPerNode);

        LOG.debug("Creating a simulation with parameters:\n nodes : {}, coresPerNode : {}, memSlicesPerNode : {} ",
                numNodes, coresPerNode, memSlicesPerNode);
        Model model = createModel(conn);
        Simulation sim = new Simulation(model, conn);

        for (; i < numSteps; i++) {
            LOG.info("Simulation step: {}", i);

            // Add/delete memory and/or core requests
            sim.createTurnover();

            // Solve and update accordingly
            if (!sim.runModelAndUpdateDB()) {
                LOG.warn("No updates from running model???");
                break;
            }

            // Check for violations
            if (sim.checkForCapacityViolation())
                break;

            printStats(conn);
        }

        // Print final stats
        printStats(conn);
        LOG.info("Simulation survived {} steps\n", i + 1);
    }
}