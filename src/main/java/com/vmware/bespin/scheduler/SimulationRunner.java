package com.vmware.bespin.scheduler;

import com.vmware.dcm.SolverException;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;

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

    private static final Logger LOG = LogManager.getLogger(Simulation.class);

    // below are percentages
    static final int MIN_CAPACITY = 65;
    static final int MAX_CAPACITY = 95;
    static final int MIN_TURNOVER = 1;
    static final int MAX_TURNOVER = 5;

    private static final Random rand = new Random();

    Simulation(Model model, DSLContext conn) {
        this.model = model;
        this.conn = conn;
    }

    public void createTurnover() {
        final Allocations allocTable = Allocations.ALLOCATIONS;

        int totalCores = this.totalCores();
        int coreTurnover = MIN_TURNOVER + rand.nextInt(MAX_TURNOVER - MIN_TURNOVER);
        int coreAllocationsToMake = (int) Math.ceil((((double) coreTurnover) / 100) * totalCores);
        int maxCoreAllocations = (int) Math.ceil((((double) MAX_CAPACITY) / 100) * totalCores);
        int minCoreAllocations = (int) Math.ceil((((double) MIN_CAPACITY) / 100) * totalCores);
        System.out.printf("Core Turnover: %d, allocationsToMake: %d, maxAllocations: %d, minAllocation: %d%n",
                coreTurnover, coreAllocationsToMake, maxCoreAllocations, minCoreAllocations);

        int turnoverCounter = 0;
        while (turnoverCounter < coreAllocationsToMake) {
            int usedCores = this.usedCores();
            if (usedCores > minCoreAllocations) {
                if (usedCores >= maxCoreAllocations || rand.nextBoolean()) {
                    // delete core allocation
                    final String allocations = "select * from allocations where allocations.cores > 0";
                    Result<Record> results = this.conn.fetch(allocations);
                    int rowToDelete = rand.nextInt(results.size());
                    rowToDelete = (int) results.get(rowToDelete).getValue(0);
                    conn.delete(allocTable).where(allocTable.ID.eq(rowToDelete)).execute();
                }
            }
            // Add an allocation, count as turnover
            if (usedCores < maxCoreAllocations) {
                conn.insertInto(allocTable)
                        .set(allocTable.ID, this.getNextAllocationId())
                        .set(allocTable.APPLICATION, this.chooseRandomApplication())
                        .set(allocTable.CORES, 1)
                        .set(allocTable.MEMSLICES, 0)
                        .set(allocTable.STATUS, "PENDING")
                        .set(allocTable.CURRENT_NODE, -1)
                        .set(allocTable.CONTROLLABLE__NODE, (Field<Integer>) null)
                        .execute();
                turnoverCounter++;
            }
        }

        int totalMemslices = this.totalMemslices();
        int memsliceTurnover = MIN_TURNOVER + rand.nextInt(MAX_TURNOVER - MIN_TURNOVER);
        int memsliceAllocationsToMake = (int) Math.ceil((((double) memsliceTurnover) / 100) * totalMemslices);
        int maxMemsliceAllocations = (int) Math.ceil((((double) MAX_CAPACITY) / 100) * totalMemslices);
        int minMemsliceAllocations = (int) Math.ceil((((double) MIN_CAPACITY) / 100) * totalMemslices);
        System.out.printf("Memslice Turnover: %d, allocationsToMake: %d, maxAllocations: %d, minAllocation: %d%n",
                memsliceTurnover, memsliceAllocationsToMake, maxMemsliceAllocations, minMemsliceAllocations);

        turnoverCounter = 0;
        while (turnoverCounter < memsliceAllocationsToMake) {
            int usedMemslices = this.usedMemslices();
            if (usedMemslices > minMemsliceAllocations) {
                if (usedMemslices >= maxMemsliceAllocations || rand.nextBoolean()) {
                    // delete memslice allocation
                    final String allocations = "select * from allocations where allocations.memslices > 0";
                    Result<Record> results = this.conn.fetch(allocations);
                    int rowToDelete = rand.nextInt(results.size());
                    rowToDelete = (int) results.get(rowToDelete).getValue(0);
                    conn.delete(allocTable).where(allocTable.ID.eq(rowToDelete)).execute();
                }
            }
            // Add an allocation, count as turnover
            if (usedMemslices < maxMemsliceAllocations) {
                conn.insertInto(allocTable)
                        .set(allocTable.ID, this.getNextAllocationId())
                        .set(allocTable.APPLICATION, this.chooseRandomApplication())
                        .set(allocTable.CORES, 0)
                        .set(allocTable.MEMSLICES, 1)
                        .set(allocTable.STATUS, "PENDING")
                        .set(allocTable.CURRENT_NODE, -1)
                        .set(allocTable.CONTROLLABLE__NODE, (Field<Integer>) null)
                        .execute();
                turnoverCounter++;
            }
        }

        if (this.checkForCapacityViolation()) {
            System.out.print("CAPACITY GENERATION ERROR AFTER POPULATING");
            System.exit(-1);
        }
    }

    public boolean runModelAndUpdateDB() {
        Result<? extends Record> results;
        try {
            System.out.println(conn.fetch("select nodes.id, nodes.cores - sum(allocations.cores) as core_spare " +
                    "from allocations " +
                    "join nodes " +
                    "  on nodes.id = allocations.controllable__node " +
                    "group by nodes.id, nodes.cores"));
            System.out.println(conn.fetch("select nodes.id, nodes.memslices - sum(allocations.memslices) as mem_spare " +
                    "from allocations " +
                    "join nodes " +
                    "  on nodes.id = allocations.controllable__node " +
                    "group by nodes.id, nodes.memslices"));
            System.out.println(conn.fetch("select applications.id, count(distinct allocations.controllable__node) as num_nodes " +
                    "from allocations " +
                    "join applications " +
                    "  on applications.id = allocations.application " +
                    "group by applications.id"));
            results = model.solve("ALLOCATIONS");
        } catch (ModelException e) {
            System.out.println(String.format("Got a model exception when solving: %s", e.toString()));
            return false;
        } catch (SolverException e) {
            System.out.println(String.format("Got a model exception when solving: {}", e.toString()));
            System.out.println(this.checkForCapacityViolation());
            System.out.print("Free cores per node:");
            System.out.println(conn.fetch("select nodes.id, nodes.cores - sum(allocations.cores) as core_spare " +
                    "from allocations " +
                    "join nodes " +
                    "  on nodes.id = allocations.controllable__node " +
                    "group by nodes.id, nodes.cores"));
            System.out.print("Free memslices per node:");
            System.out.println(conn.fetch("select nodes.id, nodes.memslices - sum(allocations.memslices) as mem_spare " +
                    "from allocations " +
                    "join nodes " +
                    "  on nodes.id = allocations.controllable__node " +
                    "group by nodes.id, nodes.memslices"));
            System.out.print("Nodes per application:");
            System.out.println(conn.fetch("select applications.id, count(distinct allocations.controllable__node) as num_nodes " +
                    "from allocations " +
                    "join applications " +
                    "  on applications.id = allocations.application " +
                    "group by applications.id"));
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
        return true;
    }

    public boolean checkForCapacityViolation() {
        // Check total capacity
        boolean totalCoreCheck = this.totalCores() - this.usedCores() >= 0;
        boolean totalMemsliceCheck = this.totalCores() - this.usedCores() >= 0;

        // Check per-node capacity for cores
        var nodeWrapper = new Object(){ boolean nodesPerCoreCheck = true; };
        Result<Record> freeCores = conn.fetch("select nodes.id, nodes.cores - sum(allocations.cores) as core_spare " +
                "from allocations " +
                "join nodes " +
                "  on nodes.id = allocations.controllable__node " +
                "group by nodes.id, nodes.cores");
        freeCores.forEach(spareCores -> {
                    final Integer spareCoresPerNode = (Integer) spareCores.get(0);
                    nodeWrapper.nodesPerCoreCheck = nodeWrapper.nodesPerCoreCheck && spareCoresPerNode >= 0;
                }
        );

        // Check per-node capacity for memslices
        var memsliceWrapper = new Object(){ boolean memslicesPerCoreCheck = true; };
        Result<Record> freeMemslices = conn.fetch("select nodes.id, nodes.cores - sum(allocations.cores) as core_spare " +
                "from allocations " +
                "join nodes " +
                "  on nodes.id = allocations.controllable__node " +
                "group by nodes.id, nodes.cores");
        freeMemslices.forEach(spareMemslices -> {
                    final Integer spareMemslicesPerNode = (Integer) spareMemslices.get(0);
                    memsliceWrapper.memslicesPerCoreCheck = memsliceWrapper.memslicesPerCoreCheck && spareMemslicesPerNode >= 0;
                }
        );

        return !(totalCoreCheck && totalMemsliceCheck && nodeWrapper.nodesPerCoreCheck && memsliceWrapper.memslicesPerCoreCheck);
    }

    private int usedCores() {
        final String usedCores = "select sum(allocations.cores) as usedCores from allocations";
        return ((Long) this.conn.fetch(usedCores).get(0).getValue(0)).intValue();
    }

    private int totalCores() {
        final String totalCores = "select sum(nodes.cores) from nodes";
        return ((Long) this.conn.fetch(totalCores).get(0).getValue(0)).intValue();
    }

    private int usedMemslices() {
        final String usedMemslices = "select sum(allocations.memslices) as usedMemslices from allocations";
        return ((Long) this.conn.fetch(usedMemslices).get(0).getValue(0)).intValue();
    }

    private int totalMemslices() {
        final String totalMemslices = "select sum(nodes.memslices) from nodes";
        return ((Long) this.conn.fetch(totalMemslices).get(0).getValue(0)).intValue();
    }

    private int getNextAllocationId() {
        final String highestAllocationId = "select max(allocations.id) from allocations";
        return (int) this.conn.fetch(highestAllocationId).get(0).getValue(0) + 1;
    }

    private int chooseRandomApplication() {
        final String applicationIds = "select id from applications";
        Result<Record> results = this.conn.fetch(applicationIds);
        int rowNum = rand.nextInt(results.size());
        return (int) results.get(rowNum).getValue(0);
    }
}

public class SimulationRunner {
    private static final Logger LOG = LogManager.getLogger(SimulationRunner.class);
    private static final String NUM_STEPS_OPTION = "steps";
    private static final String NUM_NODES_OPTION = "numNodes";
    private static final String CORES_PER_NODE_OPTION = "coresPerNode";
    private static final String MEMSLICES_PER_NODE_OPTION = "memslicesPerNode";

    private static void setupDb(final DSLContext conn) {
        final InputStream resourceAsStream = SimulationRunner.class.getResourceAsStream("/bespin_tables.sql");
        try {
            assert resourceAsStream != null;
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
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void initDB(final DSLContext conn, int numNodes, int coresPerNode, int memslicesPerNode) {
        Random rand = new Random();
        setupDb(conn);

        // Add nodes with specified cores
        final Nodes nodeTable = Nodes.NODES;
        for (int i = 1; i <= numNodes; i++) {
            conn.insertInto(nodeTable)
                    .set(nodeTable.ID, i)
                    .set(nodeTable.CORES, coresPerNode)
                    .set(nodeTable.MEMSLICES, memslicesPerNode)
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
        coreRequests = (int) Math.ceil((float) coreRequests * 0.70);
        final Allocations allocTable = Allocations.ALLOCATIONS;
        int node = 0;
        int nodeCapacity = 0;
        int nodesUsed = 1;
        for (int i = 1; i <= coreRequests; i++) {
            while (nodeCapacity <= nodesUsed ) {
                node = rand.nextInt(numNodes) + 1;
                nodeCapacity = (int) conn.fetch(String.format("select cores from nodes where id = %d", node)).getValues(0).get(0);
                nodesUsed = conn.fetch(String.format("select * from allocations where cores >= 1 and current_node = %d", node)).size();
                //System.out.println(String.format("Capacity for node %d is %d, used cores is %d", node, nodeCapacity, nodesUsed));
            }
            conn.insertInto(allocTable)
                    .set(allocTable.ID, i)
                    .set(allocTable.APPLICATION, rand.nextInt(numApplications) + 1)
                    .set(allocTable.CORES, 1)
                    .set(allocTable.MEMSLICES, 0)
                    .set(allocTable.STATUS, "PLACED")
                    .set(allocTable.CURRENT_NODE, node)
                    .set(allocTable.CONTROLLABLE__NODE, node)
                    .execute();
            nodeCapacity = 0;
            nodesUsed = 1;
        }

        // Randomly add memslice requests for 70% capacity of cluster
        int memRequests = numNodes * memslicesPerNode;
        memRequests = (int) Math.ceil((float) memRequests * 0.70);
        int memCapacity = 0;
        int memUsed = 1;
        for (int i = 1; i <= memRequests; i++) {
            node = rand.nextInt(numNodes) + 1;
            while (memCapacity <= memUsed ) {
                node = rand.nextInt(numNodes) + 1;
                memCapacity = (int) conn.fetch(String.format("select memslices from nodes where id = %d", node)).getValues(0).get(0);
                memUsed = conn.fetch(String.format("select * from allocations where memslices >= 1 and current_node = %d", node)).size();
                //System.out.println(String.format("Capacity for node %d is %d, used memslices is %d", node, memCapacity, memUsed));
            }
            conn.insertInto(allocTable)
                    .set(allocTable.ID, coreRequests + i)
                    .set(allocTable.APPLICATION, rand.nextInt(numApplications) + 1)
                    .set(allocTable.CORES, 0)
                    .set(allocTable.MEMSLICES, 1)
                    .set(allocTable.STATUS, "PLACED")
                    .set(allocTable.CURRENT_NODE, node)
                    .set(allocTable.CONTROLLABLE__NODE, node)
                    .execute();
            memCapacity = 0;
            memUsed = 1;
        }
    }

    private static Model createModel(final DSLContext conn) {
        // Only PENDING core requests are placed
        // All DCM solvers need to have this constraint
        final String placed_constraint = "create constraint placed_constraint as " +
                " select * from allocations where status = 'PLACED' check current_node = controllable__node";

        // Capacity core view
        final String capacity_core_view = "create constraint spare_cores as " +
                "select nodes.id, nodes.cores - sum(allocations.cores) as cores_spare " +
                "from allocations " +
                "join nodes " +
                "  on nodes.id = allocations.controllable__node " +
                "group by nodes.id, nodes.cores";

        // Capacity core constraint (e.g., can only use what is available on each node)
        final String capacity_core_constraint = "create constraint capacity_core_constraint as " +
                " select * from spare_cores check cores_spare >= 0";

        // Capacity memslice view
        final String capacity_memslice_view = "create constraint spare_memslices as " +
                "select nodes.id, nodes.memslices - sum(allocations.memslices) as memslices_spare " +
                "from allocations " +
                "join nodes " +
                "  on nodes.id = allocations.controllable__node " +
                "group by nodes.id, nodes.memslices";

        // Capacity memslice constraint (e.g., can only use what is available on each node)
        final String capacity_memslice_constraint = "create constraint capacity_memslice_constraint as " +
                " select * from spare_memslices check memslices_spare >= 0";

        // Create load balancing constraint across nodes for cores and memslices
        final String node_load_cores = "create constraint node_load_cores as " +
                "select * from spare_cores " +
                "maximize min(cores_spare)";
        final String node_load_memslices = "create constraint node_load_memslices as " +
                "select * from spare_memslices " +
                "maximize min(memslices_spare)";

        // Minimize number of nodes per application (e.g., maximize locality)
        final String application_num_nodes_view = "create constraint application_num_nodes as " +
                "select applications.id, count(distinct allocations.controllable__node) as num_nodes " +
                "from allocations " +
                "join applications " +
                "  on applications.id = allocations.application " +
                "group by applications.id";
        final String application_locality_constraint = "create constraint application_locality_constraint as " +
                "select * from application_num_nodes " +
                "maximize -1*sum(num_nodes)";

        OrToolsSolver.Builder b = new OrToolsSolver.Builder();
        return Model.build(conn, b.build(), List.of(
                placed_constraint,
                capacity_core_view,
                capacity_core_constraint,
                capacity_memslice_view,
                capacity_memslice_constraint,
                node_load_cores,
                node_load_memslices,
                application_num_nodes_view,
                application_locality_constraint
        ));
    }

    private static void printStats(final DSLContext conn) {
        System.out.print("Free cores per node:");
        System.out.println(conn.fetch("select nodes.id, nodes.cores - sum(allocations.cores) as core_spare " +
                "from allocations " +
                "join nodes " +
                "  on nodes.id = allocations.controllable__node " +
                "group by nodes.id, nodes.cores"));
        System.out.print("Free memslices per node:");
        System.out.println(conn.fetch("select nodes.id, nodes.memslices - sum(allocations.memslices) as mem_spare " +
                "from allocations " +
                "join nodes " +
                "  on nodes.id = allocations.controllable__node " +
                "group by nodes.id, nodes.memslices"));
        System.out.print("Nodes per application:");
        System.out.println(conn.fetch("select applications.id, count(distinct allocations.controllable__node) as num_nodes " +
                "from allocations " +
                "join applications " +
                "  on applications.id = allocations.application " +
                "group by applications.id"));
    }

    public static void main(String[] args) throws ClassNotFoundException {

        int i = 0;
        // These are the defaults for these parameters.
        // They should be overridden by commandline arguments.
        int numSteps = 10;
        int numNodes = 25;
        int coresPerNode = 15;
        int memslicesPerNode = 15;

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
        Option memslicesPerNodeOption = Option.builder(MEMSLICES_PER_NODE_OPTION)
                .hasArg()
                .desc("number of 2 MB memory slices per node.\nDefault: 128")
                .type(Integer.class)
                .build();

        options.addOption("h", false, "print help message");
        options.addOption(numStepsOption);
        options.addOption(numNodesOption);
        options.addOption(coresPerNodeOption);
        options.addOption(memslicesPerNodeOption);

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
            if (cmd.hasOption(MEMSLICES_PER_NODE_OPTION)) {
                memslicesPerNode = Integer.parseInt(cmd.getOptionValue(MEMSLICES_PER_NODE_OPTION));
            }
        } catch (ParseException e) {
            LOG.error("Failed to parse command line");
            return;
        }

        // Create an in-memory database and get a JOOQ connection to it
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        initDB(conn, numNodes, coresPerNode, memslicesPerNode);

        LOG.debug("Creating a simulation with parameters:\n nodes : {}, coresPerNode : {}, memSlicesPerNode : {} ",
                numNodes, coresPerNode, memslicesPerNode);
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