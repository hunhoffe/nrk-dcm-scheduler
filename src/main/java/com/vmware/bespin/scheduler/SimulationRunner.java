package com.vmware.bespin.scheduler;

import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;

import com.vmware.bespin.scheduler.generated.tables.Nodes;
import com.vmware.bespin.scheduler.generated.tables.Applications;
import com.vmware.bespin.scheduler.generated.tables.Cores;

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

        final Cores t = Cores.CORES;

        // TODO: actually calculate turnover
        for (int i = 1; i <= 10; i++) {
            conn.insertInto(t)
                    .set(t.ID, this.counter * i)
                    .set(t.APPLICATION, 0)
                    .set(t.CURRENT_NODE, -1)
                    .set(t.CONTROLLABLE__NODE, (Field<Integer>) null)
                    .execute();
        }
        this.counter++;
    }

    public boolean runModelAndUpdateDB() {
        Result<? extends Record> results = null;
        try {
            results = model.solve("cores");
        } catch (ModelException e) {
            LOG.info("Got a model exception when solving: {}", e);
            return false;
        }

        if (results == null)
            return false;

        final Cores t = Cores.CORES;
        final List<Update<?>> updates = new ArrayList<>();
        // Each record is a row in the COMPONENTS table
        LOG.debug("new cores placed on:");
        results.forEach(r -> {
                    final Integer coreId = (Integer) r.get("ID");
                    final Integer application = (Integer) r.get("APPLICATION");
                    final Integer currentNode = (Integer) r.get("CURRENT_NODE");
                    final Integer newNode = (Integer) r.get("CONTROLLABLE__NODE");
                    if (currentNode == -1) {
                        LOG.debug("{} ", newNode);
                        updates.add(
                                conn.update(t)
                                        .set(t.CURRENT_NODE, newNode)
                                        .where(t.ID.eq(coreId).and(t.APPLICATION.eq(application)))
                        );
                    }
                }
        );
        conn.batch(updates).execute();
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
        setupDb(conn);

        // Add nodes with specified cores/memory
        final Nodes t = Nodes.NODES;
        for (int i = 1; i <= numNodes; i++) {
            conn.insertInto(t)
                    .set(t.ID, i)
                    .set(t.CORES, coresPerNode)
                    .set(t.MEMSLICES, memSlicesPerNode)
                    .newRecord()
                    .set(t.ID, 100+i)
                    .set(t.CORES, coresPerNode)
                    .set(t.MEMSLICES, memSlicesPerNode)
                    .execute();
        }

        // Add initial applications
        final Applications t2 = Applications.APPLICATIONS;
        conn.insertInto(t2)
                .set(t2.ID, 0)
                .execute();

        final Cores t3 = Cores.CORES;
        conn.insertInto(t3)
                .set(t3.ID, 100000)
                .set(t3.APPLICATION, 0)
                .set(t3.CONTROLLABLE__NODE, 1)
                .set(t3.CURRENT_NODE, 1)
                .execute();
        conn.insertInto(t3)
                .set(t3.ID, 100001)
                .set(t3.APPLICATION, 0)
                .set(t3.CONTROLLABLE__NODE, 1)
                .set(t3.CURRENT_NODE, 1)
                .execute();
        conn.insertInto(t3)
                .set(t3.ID, 100002)
                .set(t3.APPLICATION, 0)
                .set(t3.CONTROLLABLE__NODE, 1)
                .set(t3.CURRENT_NODE, 1)
                .execute();
        conn.insertInto(t3)
                .set(t3.ID, 100003)
                .set(t3.APPLICATION, 0)
                .set(t3.CONTROLLABLE__NODE, 2)
                .set(t3.CURRENT_NODE, 2)
                .execute();
        conn.insertInto(t3)
                .set(t3.ID, 100004)
                .set(t3.APPLICATION, 0)
                .set(t3.CONTROLLABLE__NODE, 2)
                .set(t3.CURRENT_NODE, 2)
                .execute();
        conn.insertInto(t3)
                .set(t3.ID, 100005)
                .set(t3.APPLICATION, 0)
                .set(t3.CONTROLLABLE__NODE, 3)
                .set(t3.CURRENT_NODE, 3)
                .execute();
    }

    private static Model createModel(final DSLContext conn) {
        /*
        // Capacity constraint is actually just a priority constraint
        final String spareDiskCapacity = "create view spare_disk as " +
                "select disks.max_capacity - sum(components.current_util) as disk_spare " +
                "from components " +
                "join disks " +
                "  on disks.id = components.controllable__disk " +
                "group by disks.id, disks.max_capacity";

        // Queries presented as objectives, will have their values maximized.
        final String priorityDisk = "create view objective_load_disk as select min(disk_spare)-max(disk_spare) from spare_disk";

        // Make sure each replica of a component is on a different disk
        final String replica_constraint = "create view replica_constraint as " +
                "select * from components group by id check all_different(controllable__disk) = true";

        // Only new components are placed
        final String placed_constraint = "create view placed_constraint as " +
                " select * from components where status = 'PLACED' check current_disk = controllable__disk";

        // Components belonging to the same object must be on the same disk
        final String object_constraint = "create view object_constraint as " +
                " select * from components group by object_id,replica_id check all_equal(controllable__disk) = true";
         */

        final String node_capacity_view = "CREATE CONSTRAINT node_capacity_view AS " +
                " SELECT nodes.id, nodes.cores, COUNT(*) AS usedCores " +
                " FROM nodes JOIN cores ON (nodes.id = cores.controllable__node) " +
                " GROUP BY nodes.id, nodes.cores";

        final String node_capacity_constraint = "CREATE CONSTRAINT node_capacity_constraint AS " +
                " SELECT * FROM node_capacity_view CHECK usedCores < cores";

        return Model.build(conn, List.of(node_capacity_view, node_capacity_constraint));
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
        int numNodes = 64;
        int coresPerNode = 128;
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
        Class.forName ("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        initDB(conn, numNodes, coresPerNode, memSlicesPerNode);

        LOG.debug("Creating a simulation with parameters:\n nodes : {}, coresPerNode : {}, memSlicesPerNode : {} ",
                numNodes, coresPerNode, memSlicesPerNode);
        Simulation sim = new Simulation(createModel(conn), conn);

        for (; i < numSteps; i++) {
            LOG.info("Simulation step: {}", i);

            // Add/delete memory and/or core requests
            sim.createTurnover();
            System.out.println("Nodes:");
            System.out.println(conn.fetch("select * from nodes"));
            System.out.println("Cores:");
            System.out.println(conn.fetch("select * from cores"));
            System.out.println("BYE");

            // Solve and update accordingly
            sim.runModelAndUpdateDB();

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