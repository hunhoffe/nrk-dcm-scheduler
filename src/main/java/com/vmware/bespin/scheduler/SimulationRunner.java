package com.vmware.bespin.scheduler;

import com.vmware.dcm.backend.ortools.OrToolsSolver;
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


        //final Cores t = Cores.CORES;

        // TODO: actually calculate turnover
        for (int i = 1; i <= 10; i++) {
            conn.execute(String.format("insert into processes values(%d, 'PENDING', -1, null)", this.counter*10 + i));
            /*
            conn.insertInto(t)
                    .set(t.ID, this.counter*10 + i)
                    .set(t.APPLICATION, 0)
                    .set(t.STATUS, "PENDING")
                    .set(t.CURRENT_NODE, -1)
                    .set(t.CONTROLLABLE__NODE, (Field<Integer>) null)
                    .execute();
            */
        }
        this.counter++;
    }

    public boolean runModelAndUpdateDB() {
        Result<? extends Record> results = null;
        try {
            results = model.solve("PROCESSES");
        } catch (ModelException e) {
            LOG.info("Got a model exception when solving: {}", e);
            return false;
        }

        if (results == null) {
            return false;
        }

        final List<Update<?>> updates = new ArrayList<>();
        LOG.info("new cores placed on:");
        results.forEach(r -> {
                    final Integer componentId = (Integer) r.get("ID");
                    final Integer currentCore = (Integer) r.get("CONTROLLABLE__CORE");
                    LOG.info("{} ", r);
                    updates.add(
                            conn.update(DSL.table("PROCESSES"))
                                    .set(DSL.field("CURRENT_CORE"), currentCore)
                                    .set(DSL.field("STATUS"), "PLACED")
                                    .where(DSL.field("ID").eq(componentId)
                                            .and(DSL.field("STATUS").eq("PENDING")))
            );
                }
        );
        conn.batch(updates).execute();
        System.out.println(conn.fetch("select * from processes"));

        /*

        final Cores t = Cores.CORES;
        final List<Update<?>> updates = new ArrayList<>();
        // Each record is a row in the COMPONENTS table
        results.forEach(r -> {
                    final Integer coreId = (Integer) r.get("ID");
                    final Integer application = (Integer) r.get("APPLICATION");
                    final String status = (String) r.get("STATUS");
                    final Integer newNode = (Integer) r.get("CONTROLLABLE__NODE");
                    if (status.equals("PENDING")) {
                        LOG.info("{} ", r);
                        updates.add(
                                conn.update(t)
                                        .set(t.CURRENT_NODE, newNode)
                                        .set(t.STATUS, "PLACED")
                                        .where(t.ID.eq(coreId).and(t.APPLICATION.eq(application))
                                        .and(t.STATUS.eq("PENDING")))
                        );
                    }
                }
        );
        conn.batch(updates).execute();
         */
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
        // node == physical machine / server
        conn.execute("create table nodes(" +
                "id integer, primary key (id))");

        // core == CPU in a socket on a node
        conn.execute("create table cores(" +
                "id integer," +
                "node_id integer," +
                "max_threads integer,\n" +
                "    foreign key (node_id) references nodes(id), primary key (id))");

        // physical memory resources available
        conn.execute("create table memory(" +
                "id integer, " +
                "node_id integer," +
                "size integer,\n" +
                "    foreign key (node_id) references nodes(id), primary key (id))");

        // All processes in the system
        // controllable__* columns are columns that need to be solved by DCM; resource that needs to be allocated
        // use `status` column to indicate to DCM whether to solve for controllable__* in this row (set status == PENDING)
        // After DCM finishes solving, copy controllable__core into current_core, and set status to PLACED
        conn.execute("create table processes(" +
                "id integer,\n" +
                "status varchar(36),\n" + // status = DCM needs this to know what processes to solve for. {PENDING, PLACED}
                "current_core integer,\n" +
                "controllable__core integer,\n" +
                "foreign key (controllable__core) references cores(id), primary key (id))");

        // Memory regions per application. Commented out bc some SQL parsing error in this SQL
      /*  conn.execute("create table memory_regions(" +
                "id integer,\n" +
                "application_id integer,\n" +
                "\tstatus varchar(36),\n" +
                "\tsize integer,\n" +
                "\tcurrent_memory integer,\n" +
                "\tcontrollable__memory integer,\n" +
                "\tforeign key (application_id) references applications(id)" +
                "\tforeign key (controllable__memory) references memory(id))");*/

        // Applications with their corresponding processes in the system
        conn.execute("\n" +
                "create table applications(\n" +
                "    id integer,\n" +
                "    process_id integer,\n" +
                "    foreign key (process_id) references processes(id),\n" +
                "    primary key (id, process_id));");

        // What this group by query does is produce all application process running on a particular core
        // possible output of this view:
        // app_id = 1, core = 2
        // app_id = 1, core = 2
        conn.execute("create view blep as \n" +
                "                        select applications.id as id, processes.current_core as core " +
                "                        from applications join processes on applications.process_id = processes.id" +
                "                        group by applications.id, processes.current_core");

        // output of this view: (how many different cores an application is running on)
        // app_id = 1, count = 1
        conn.execute("create view blep2 as \n" +
                "                        select count(core) as c " +
                "                        from blep group by id");

        /*
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
        */
    }

    private static void initDB(final DSLContext conn, int numNodes, int coresPerNode, int memSlicesPerNode) {
        setupDb(conn);

        /*
        // Add nodes with specified cores/memory
        final Nodes t = Nodes.NODES;
        for (int i = 1; i <= numNodes; i++) {
            conn.insertInto(t)
                    .set(t.ID, i)
                    .set(t.CORES, coresPerNode)
                    .set(t.MEMSLICES, memSlicesPerNode)
                    .execute();
        }

        // Add initial applications
        final Applications t2 = Applications.APPLICATIONS;
        conn.insertInto(t2)
                .set(t2.ID, 0)
                .execute();
         */

        /* Now just insert so test data */
        // Add nodes
        conn.execute("insert into nodes values(1)");
        conn.execute("insert into nodes values(2)");
        conn.execute("insert into nodes values(3)");
        conn.execute("insert into nodes values(4)");

        // Add cores to each node
        // Node 1
        conn.execute("insert into cores values(1, 1, 2)");
        conn.execute("insert into cores values(2, 1, 2)");
        conn.execute("insert into cores values(3, 1, 2)");
        conn.execute("insert into cores values(4, 1, 2)");

        // Node 2
        conn.execute("insert into cores values(5, 2, 2)");
        conn.execute("insert into cores values(6, 2, 2)");

        // Node 3
        conn.execute("insert into cores values(7, 3, 2)");
        conn.execute("insert into cores values(8, 3, 2)");

        // Node 4
        conn.execute("insert into cores values(9, 4, 2)");
        conn.execute("insert into cores values(10, 4, 2)");
        conn.execute("insert into cores values(11, 4, 2)");
        conn.execute("insert into cores values(12, 4, 2)");
        conn.execute("insert into cores values(13, 4, 2)");
        conn.execute("insert into cores values(14, 4, 2)");
        conn.execute("insert into cores values(15, 4, 2)");
        conn.execute("insert into cores values(16, 4, 2)");

        // Add processes
        // for example, null is the controllable__core column
        conn.execute("insert into processes values(1, 'PENDING', -1, null)");
        conn.execute("insert into processes values(2, 'PENDING', -1, null)");
        conn.execute("insert into processes values(3, 'PENDING', -1, null)");
        conn.execute("insert into processes values(4, 'PENDING', -1, null)");
        conn.execute("insert into processes values(5, 'PENDING', -1, null)");
        conn.execute("insert into processes values(6, 'PENDING', -1, null)");
        conn.execute("insert into processes values(7, 'PENDING', -1, null)");
        conn.execute("insert into processes values(8, 'PENDING', -1, null)");
        conn.execute("insert into processes values(9, 'PENDING', -1, null)");
        conn.execute("insert into processes values(10, 'PENDING', -1, null)");


        //conn.execute("insert into applications values(1,1)");
        conn.execute("insert into applications values(1,5)");
        conn.execute("insert into applications values(1,6)");
    }

    private static Model createModel(final DSLContext conn) {
        /*
        // Create view that calculates how many cores are used on each node
        final String node_capacity_view = "CREATE VIEW node_capacity_view AS " +
                " SELECT nodes.id, (nodes.cores - COUNT(*)) AS spareCores " +
                " FROM nodes JOIN cores ON (nodes.id = cores.controllable__node) " +
                " GROUP BY nodes.id";
        conn.execute(node_capacity_view);

        // Only PENDING processes are placed
        // All DCM solvers need to have this constraint
        final String placed_constraint = "CREATE CONSTRAINT placed_constraint AS " +
                " SELECT * FROM cores WHERE status = 'PLACED' CHECK current_node = controllable__node";

        // Hard constraint: Respect core capacity of each node
        final String node_capacity_constraint = "CREATE CONSTRAINT node_capacity_constraint AS " +
                " SELECT * FROM node_capacity_view CHECK spareCores >= 0";

        // Soft constraint: Load balance cores across nodes
        final String balance_nodes = "CREATE CONSTRAINT balance_nodes AS " +
                " SELECT * FROM node_capacity_view " +
                " MAXIMIZE min(spareCores)-max(spareCores)";
         */

        /* Now write constraints that will be fed into DCM */
        // Capacity constraint: how many processes (count) are running on a core (cores.id)
        // In this constraint, we don't need cores.max_threads explicitly, but we extract the column so future constraints
        // can compute on it (constraint2 below)
        final String constraint = "create constraint capacity_view as " +
                "select cores.id as core, cores.max_threads as m, count(processes.id) as num_procs from processes join cores on cores.id = processes.controllable__core " +
                "group by cores.id, cores.max_threads ";

        // Make sure each core is allocated at most max_threads processes.
        final String constraint2 = "create constraint cap_constraint as " +
                "select * from capacity_view " +
                "check num_procs <= m";

        // Only PENDING processes are placed
        // All DCM solvers need to have this constraint
        final String placed_constraint = "create constraint placed_constraint as " +
                " select * from processes where status = 'PLACED' check current_core = controllable__core";

        // Calculate how much spare resource is available for each resource table
        final String spare_view = "create constraint spare_threads_per_core as \n" +
                "select cores.max_threads - count(processes.id) as spare_threads " +
                "from processes join cores " +
                "on cores.id = processes.controllable__core " +
                "group by cores.id, cores.max_threads";

        // Balanced constraint
        // Make sure spread between core with smallest # of threads and core with largest # of threads is minimized
        // If you have 2 cores, each can handle 5 threads, and you want to place 4 threads, the optimal solution is 2 and 2,
        // barring no other constraints are unsatisfied.

        // Soft vs hard constraints: hard constraints MUST be satisfied by DCM, soft constraints are just maximized.
        // check is hard constraints.
        final String balance_constraint =
                "create constraint balance_cores as select * from spare_threads_per_core " +
                        "maximize min(spare_threads)-max(spare_threads)";

        // These next 2 views are same as spare_threads_per_core and balance_cores above, just for memory
        // Balanced memory
        // Example: memory table:
        // node_id = 1, memory_id = 1, size = 16
        // node_id = 1, memory_id = 2, size = 16
        // node_id = 2, memory_id = 3, size = 16
        // memory region table:
        // app_id = 1, memory_id = 1, size = 8
        // app_id = 1, memory_id = 2, size = 8
        // app_id = 3, memory_id = 3, size = 16

        // spare memory will calculate:
        // memory_id = 1, spare = 8
        // memory_id = 2, spare = 8
        // memory_id = 3, spare = 0
        final String spare_memory = "create constraint unallocated_space_per_memory as \n" +
                "select memory.size - sum(memory_regions.size) as spare " +
                "from memory_regions join memory " +
                "on memory.id = memory_regions.controllable__memory " +
                "group by memory.size";

        final String balance_mem_constraint =
                "create constraint balance_mem as select * from unallocated_space_per_memory " +
                        "maximize min(spare)-max(spare)";

        // Application colocation: for the same application, its processes need to be on same (COUNT(DISTINCT c) = 1)
        // or different (COUNT(DISTINCT c) > 1) cores.
        // Calculate all cores that an application is on
        final String app_colocation_view =
                "create constraint app_colocation as \n" +
                        "select applications.id as app, processes.controllable__core as core " +
                        " from applications join processes on applications.process_id = processes.id " +
                        "group by applications.id, processes.controllable__core";

        // Count how many distinct cores an application is on
        final String interm_view =
                "create constraint app_colocation_interm_view as select count(core) as c from app_colocation " +
                        "group by app ";

        // Check the number of distinct cores satisifes some constraint, depending on semantic requirement (see comment
        // on app_colocation_view above).
        final String app_colocation_constraint =
                "create constraint app_colocation_constraint as select c from app_colocation_interm_view " +
                        "check COUNT(DISTINCT c) > 1 ";
        // " maximize c "; // another alternate semantic requirement: put application on as many cores as possible

        OrToolsSolver.Builder b = new OrToolsSolver.Builder();
        return Model.build(conn, b.build(), List.of(constraint, constraint2,
                placed_constraint));
                // spare_view, balance_constraint,
                //spare_memory, balance_mem_constraint,));
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
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        initDB(conn, numNodes, coresPerNode, memSlicesPerNode);

        LOG.debug("Creating a simulation with parameters:\n nodes : {}, coresPerNode : {}, memSlicesPerNode : {} ",
                numNodes, coresPerNode, memSlicesPerNode);
        Simulation sim = new Simulation(createModel(conn), conn);

        for (; i < numSteps; i++) {
            LOG.info("Simulation step: {}", i);

            // Add/delete memory and/or core requests
            sim.createTurnover();
            System.out.println("Processes:");
            System.out.println(conn.fetch("select * from processes"));
            System.out.println("BYE");

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