package com.vmware.bespin.scheduler;

import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;

import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.Placed;
import com.vmware.bespin.scheduler.generated.tables.Applications;
import com.vmware.bespin.scheduler.generated.tables.Nodes;

import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import static org.jooq.impl.DSL.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.math.BigDecimal;

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

    static final Pending pendingTable = Pending.PENDING;
    static final Placed placedTable = Placed.PLACED;
    static final Nodes nodeTable = Nodes.NODES;

    private static final Random rand = new Random();

    Simulation(Model model, DSLContext conn) {
        this.model = model;
        this.conn = conn;
    }

    public void createTurnover() {

        // TODO: add application creation/removal to turnover

        int totalCores = this.coreCapacity();
        int coreTurnover = MIN_TURNOVER + rand.nextInt(MAX_TURNOVER - MIN_TURNOVER);
        int coreAllocationsToMake = (int) Math.ceil((((double) coreTurnover) / 100) * totalCores);
        int maxCoreAllocations = (int) Math.ceil((((double) MAX_CAPACITY) / 100) * totalCores);
        int minCoreAllocations = (int) Math.ceil((((double) MIN_CAPACITY) / 100) * totalCores);
        LOG.info("Core Turnover: {}, allocationsToMake: {}, maxAllocations: {}, minAllocation: {}\n",
                coreTurnover, coreAllocationsToMake, maxCoreAllocations, minCoreAllocations);

        int turnoverCounter = 0;
        while (turnoverCounter < coreAllocationsToMake) {
            int usedCores = this.usedCores();
            if (usedCores > minCoreAllocations) {
                if (usedCores >= maxCoreAllocations || rand.nextBoolean()) {
                    // delete core allocation, do not count as turnover
                    Result<Record> results = conn.select().from(placedTable).where(
                            placedTable.CORES.greaterThan(0)).fetch();
                    Record rowToDelete = results.get(rand.nextInt(results.size()));
                    conn.update(placedTable)
                            .set(placedTable.CORES, placedTable.CORES.minus(1))
                            .where(and(
                                    placedTable.NODE.eq(rowToDelete.getValue(placedTable.NODE)),
                                    placedTable.APPLICATION.eq(rowToDelete.get(placedTable.APPLICATION))))
                            .execute();
                }
            }
            // Add a pending allocation, count as turnover
            if (usedCores < maxCoreAllocations) {
                conn.insertInto(pendingTable)
                        .set(pendingTable.APPLICATION, this.chooseRandomApplication())
                        .set(pendingTable.CORES, 1)
                        .set(pendingTable.MEMSLICES, 0)
                        .set(pendingTable.STATUS, "PENDING")
                        .set(pendingTable.CURRENT_NODE, -1)
                        .set(pendingTable.CONTROLLABLE__NODE, (Field<Integer>) null)
                        .execute();
                turnoverCounter++;
            }
        }

        int totalMemslices = this.memsliceCapacity();
        int memsliceTurnover = MIN_TURNOVER + rand.nextInt(MAX_TURNOVER - MIN_TURNOVER);
        int memsliceAllocationsToMake = (int) Math.ceil((((double) memsliceTurnover) / 100) * totalMemslices);
        int maxMemsliceAllocations = (int) Math.ceil((((double) MAX_CAPACITY) / 100) * totalMemslices);
        int minMemsliceAllocations = (int) Math.ceil((((double) MIN_CAPACITY) / 100) * totalMemslices);
        LOG.info("Memslice Turnover: {}, allocationsToMake: {}, maxAllocations: {}, minAllocation: {}\n",
                memsliceTurnover, memsliceAllocationsToMake, maxMemsliceAllocations, minMemsliceAllocations);

        turnoverCounter = 0;
        while (turnoverCounter < memsliceAllocationsToMake) {
            int usedMemslices = this.usedMemslices();
            if (usedMemslices > minMemsliceAllocations) {
                if (usedMemslices >= maxMemsliceAllocations || rand.nextBoolean()) {
                    // delete memslice allocation, do not count as turnover
                    Result<Record> results = conn.select().from(placedTable).where(
                            placedTable.MEMSLICES.greaterThan(0)).fetch();
                    Record rowToDelete = results.get(rand.nextInt(results.size()));
                    conn.update(placedTable)
                            .set(placedTable.MEMSLICES, placedTable.MEMSLICES.minus(1))
                            .where(and(
                                    placedTable.NODE.eq(rowToDelete.getValue(placedTable.NODE)),
                                    placedTable.APPLICATION.eq(rowToDelete.get(placedTable.APPLICATION))))
                            .execute();
                }
            }
            // Add an allocation, count as turnover
            if (usedMemslices < maxMemsliceAllocations) {
                conn.insertInto(pendingTable)
                        .set(pendingTable.APPLICATION, this.chooseRandomApplication())
                        .set(pendingTable.CORES, 0)
                        .set(pendingTable.MEMSLICES, 1)
                        .set(pendingTable.STATUS, "PENDING")
                        .set(pendingTable.CURRENT_NODE, -1)
                        .set(pendingTable.CONTROLLABLE__NODE, (Field<Integer>) null)
                        .execute();
                turnoverCounter++;
            }
        }

        // Remove any empty rows that may be produced
        String deleteEmpty = "delete from placed " +
                "where cores = 0 and memslices = 0";
        conn.execute(deleteEmpty);
    }

    public boolean runModelAndUpdateDB() {
        Result<? extends Record> results;
        try {
            results = model.solve("PENDING");
        } catch (ModelException e) {
            throw e;
            //return false;
        } catch (SolverException e) {
            throw e;
            //return false;
        }

        // Update database to finalized assignment
        final List<Update<?>> updates = new ArrayList<>();
        results.forEach(r -> {
                    updates.add(conn.update(pendingTable)
                            .set(pendingTable.CURRENT_NODE, (Integer) r.get("CONTROLLABLE__NODE"))
                            .set(pendingTable.STATUS, "PLACED")
                            .where(and(pendingTable.ID.eq((Integer) r.get("ID")), pendingTable.STATUS.eq("PENDING")))
                    );
            }
        );
        conn.batch(updates).execute();

        int usedCores = this.usedCores();
        int usedMemslices = this.usedMemslices();

        // Merge rows when possible
        // h2db disallows merge-match-delete commands, so do this in two commands :(
        String updateMerged = "update placed a\n" +
                "set a.cores = (select m.cores from merged_resources m where m.id = a.id), " +
                "    a.memslices = (select m.memslices from merged_resources m where m.id = a.id)\n " +
                "where a.status = 'PLACED' and " +
                "    exists\n" +
                "(select * from merged_resources m where m.id = a.id)";
        conn.execute(updateMerged);
        String deleteMerged = "delete from placed " +
                "where id not in (select m.id from merged_resources m) and status = 'PLACED'";
        conn.execute(deleteMerged);
        String deleteEmpty = "delete from placed " +
                "where cores = 0 and memslices = 0";
        conn.execute(deleteEmpty);

        // Triple check above command didn't change anything.
        assert(usedCores == this.usedCores());
        assert(usedMemslices == this.usedMemslices());
        return true;
    }

    public boolean checkForCapacityViolation() {
        // Check total capacity
        boolean totalCoreCheck = this.coreCapacity() >= this.usedCores();
        boolean totalMemsliceCheck = this.memsliceCapacity() >= this.usedCores();

        boolean nodesPerCoreCheck = true;
        boolean memslicesPerCoreCheck = true;

        // Iterate over each node, check current memslice allocation state and pending allocations
        Result<Record> results = conn.fetch("select id from nodes");
        for (Record r: results) {
            int node = r.getValue(nodeTable.ID);
            final String allocatedCoresSQL = String.format("select sum(allocations.cores) from allocations where current_node = %d", node);
            final String coresAvailableSQL = String.format("select cores from nodes where id = %d", node);
            Long usedCores = 0L;
            int coreCapacity = 0;
            try {
                usedCores += (Long) this.conn.fetch(allocatedCoresSQL).get(0).getValue(0);
            } catch (NullPointerException e) {}
            try {
                coreCapacity = (int) this.conn.fetch(coresAvailableSQL).get(0).getValue(0);
            } catch (NullPointerException e) {}

            nodesPerCoreCheck = nodesPerCoreCheck && (coreCapacity >= usedCores.intValue());

            final String allocatedMemslicesSQL = String.format("select sum(allocations.memslices) from allocations where current_node = %d", node);
            final String memslicesAvailableSQL = String.format("select memslices from nodes where id = %d", node);
            Long usedMemslices = 0L;
            int memsliceCapacity = 0;
            try {
                usedMemslices += (Long) this.conn.fetch(allocatedMemslicesSQL).get(0).getValue(0);
            } catch (NullPointerException e) {}
            try {
                memsliceCapacity = (int) this.conn.fetch(memslicesAvailableSQL).get(0).getValue(0);
            } catch (NullPointerException e) {}

            memslicesPerCoreCheck = memslicesPerCoreCheck && (memsliceCapacity >= usedMemslices.intValue());
        }

        return !(totalCoreCheck && totalMemsliceCheck && nodesPerCoreCheck && memslicesPerCoreCheck);
    }

    private int usedCores() {
        final String allocatedCoresSQL = "select sum(allocations.cores) from allocations";
        Long usedCores = 0L;
        try {
            usedCores += (Long) this.conn.fetch(allocatedCoresSQL).get(0).getValue(0);
        } catch (NullPointerException e) {}
        return usedCores.intValue();
    }

    private int coreCapacity() {
        final String totalCores = "select sum(nodes.cores) from nodes";
        return ((Long) this.conn.fetch(totalCores).get(0).getValue(0)).intValue();
    }

    private int usedMemslices() {
        final String allocatedMemslicesSQL = "select sum(allocations.memslices) from allocations";
        Long usedMemslices = 0L;
        try {
            usedMemslices += (Long) this.conn.fetch(allocatedMemslicesSQL).get(0).getValue(0);
        } catch (NullPointerException e) {}
        return usedMemslices.intValue();
    }

    private int memsliceCapacity() {
        final String totalMemslices = "select sum(nodes.memslices) from nodes";
        return ((Long) this.conn.fetch(totalMemslices).get(0).getValue(0)).intValue();
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

        // View to see totals of placed resources at each node
        conn.execute("create view allocated as " +
                "select node, sum(cores) as cores, sum(memslices) as memslices " +
                "from placed " +
                "group by node");

        conn.execute("create view unallocated as " +
                "select nodes.id, nodes.cores - sum(placed.cores) as cores, " +
                "    nodes.memslices - sum(placed.memslices) as memslices " +
                "from placed " +
                "join nodes " +
                "  on nodes.id = placed.node " +
                "group by nodes.id");

        // View to see the nodes each application is placed on
        conn.execute("create view app_nodes as " +
                "select application, node " +
                "from placed " +
                "group by application, node");
    }

    private static void initDB(final DSLContext conn, int numNodes, int coresPerNode, int memslicesPerNode) {
        final Nodes nodeTable = Nodes.NODES;
        final Applications appTable = Applications.APPLICATIONS;
        final Placed placedTable = Placed.PLACED;

        Random rand = new Random();
        setupDb(conn);

        // Add nodes with specified cores
        for (int i = 1; i <= numNodes; i++) {
            conn.insertInto(nodeTable)
                    .set(nodeTable.ID, i)
                    .set(nodeTable.CORES, coresPerNode)
                    .set(nodeTable.MEMSLICES, memslicesPerNode)
                    .execute();
        }

        // Randomly generate applications between 10 and 25
        int numApplications = 10 + rand.nextInt(15);
        for (int i = 1; i <= numApplications; i++) {
            // Add initial applications
            conn.insertInto(appTable)
                    .set(appTable.ID, i)
                    .execute();
        }


        // allocations for 70% capacity of cluster cores and cluster memslices
        int coreAllocs = numNodes * coresPerNode;
        coreAllocs = (int) Math.ceil((float) coreAllocs * 0.70);
        int memsliceAllocs = numNodes * memslicesPerNode;
        memsliceAllocs = (int) Math.ceil((float) memsliceAllocs * 0.70);

        HashMap<Integer, List<Integer>> appAllocMap = new HashMap();

        // Assign cores the applications
        for (int i = 0; i < coreAllocs; i++) {
            int application = rand.nextInt(numApplications) + 1;
            List<Integer> key = appAllocMap.getOrDefault(application, List.of(0, 0));
            appAllocMap.put(application, List.of(key.get(0) + 1, key.get(1)));
        }

        // Assign memslices to applications
        for (int i = 0; i < memsliceAllocs; i++) {
            int application = rand.nextInt(numApplications) + 1;
            List<Integer> key = appAllocMap.getOrDefault(application, List.of(0, 0));
            appAllocMap.put(application, List.of(key.get(0), key.get(1) + 1));
        }

        // Assign application allocs to nodes
        for (Map.Entry<Integer, List<Integer>> entry : appAllocMap.entrySet()) {
            int application = entry.getKey();
            int cores = entry.getValue().get(0);
            int memslices = entry.getValue().get(1);

            // Only have to do something if application was assigned a resource
            while (cores > 0 || memslices > 0) {

                // Choose a random node
                int node = rand.nextInt(numNodes) + 1;

                // figure out how many cores/memslices we can alloc on that node
                String sql = String.format("select cores from allocated where node = %d", node);
                int coresUsed = 0;
                try {
                    coresUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
                } catch (NullPointerException | IndexOutOfBoundsException e) {}
                int coresToAlloc = Math.min(coresPerNode - coresUsed, cores);

                sql = String.format("select memslices from allocated where node = %d", node);
                int memslicesUsed = 0;
                try {
                    memslicesUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
                } catch (NullPointerException | IndexOutOfBoundsException e) {}
                int memslicesToAlloc = Math.min(memslicesPerNode - memslicesUsed, memslices);

                // If we can alloc anything, do so
                if (coresToAlloc > 0 || memslicesToAlloc > 0) {
                    conn.insertInto(placedTable,
                                    placedTable.APPLICATION, placedTable.NODE, placedTable.CORES, placedTable.MEMSLICES)
                            .values(application, node, coresToAlloc, memslicesToAlloc)
                            .onDuplicateKeyUpdate()
                            .set(placedTable.CORES,  placedTable.CORES.plus(coresToAlloc))
                            .set(placedTable.MEMSLICES, placedTable.MEMSLICES.plus(memslicesToAlloc))
                            .execute();

                    // Mark resources as allocated
                    cores -= coresToAlloc;
                    memslices -= memslicesToAlloc;
                }
            }
        }

        // Double check correctness
        String sql = "select sum(cores) from allocated";
        int totalCoresUsed = ((BigDecimal) conn.fetch(sql).get(0).getValue(0)).intValue();
        sql = "select sum(memslices) from allocated";
        int totalMemslicesUsed = ((BigDecimal) conn.fetch(sql).get(0).getValue(0)).intValue();
        sql = "select max(cores) from allocated";
        int maxCoresUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
        sql = "select max(memslices) from allocated";
        int maxMemslicesUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
        assert(totalCoresUsed == coreAllocs);
        assert(maxCoresUsed <= coresPerNode);
        assert(totalMemslicesUsed == memsliceAllocs);
        assert(maxMemslicesUsed <= memslicesPerNode);

        LOG.info(String.format("Total cores used: %d/%d, Max cores per node: %d/%d",
                totalCoresUsed, coreAllocs, maxCoresUsed, coresPerNode));
        LOG.info(String.format("Total memslices used: %d/%d, Max memslices per node: %d/%d",
                totalMemslicesUsed, memsliceAllocs, maxMemslicesUsed, memslicesPerNode));
        printStats(conn);
    }

    private static Model createModel(final DSLContext conn) {
        // Only PENDING core requests are placed
        // All DCM solvers need to have this constraint
        final String placed_constraint = "create constraint placed_constraint as " +
                " select * from allocations where status = 'PLACED' check current_node = controllable__node";

        // View of spare resources per node for both memslices and cores
        final String spare_view = "create constraint spare_view as " +
                "select nodes.id, nodes.cores - sum(allocations.cores) as cores, nodes.memslices - sum(allocations.memslices) as memslices " +
                "from allocations " +
                "join nodes " +
                "  on nodes.id = allocations.controllable__node " +
                "group by nodes.id, nodes.cores, nodes.memslices";

        // Capacity core constraint (e.g., can only use what is available on each node)
        final String capacity_constraint = "create constraint capacity_constraint as " +
                " select * from spare_view check cores >= 0 and memslices >= 0";

        // TODO: should scale because we're maximizing the sum cores_per_node ratio compared to memslices_per_node
        // Create load balancing constraint across nodes for cores and memslices
        final String node_balance_cores_constraint = "create constraint node_balance_cores_constraint as " +
                "select cores from spare_view " +
                "maximize min(cores)";
        // Create load balancing constraint across nodes for cores and memslices
        final String node_balance_memslices_constraint = "create constraint node_balance_memslices_constraint as " +
                "select memslices from spare_view " +
                "maximize min(memslices)";

        // Minimize number of nodes per application (e.g., maximize locality)
        // TODO: write this.
        // could do two constraints, when you place a pending application two cases:
        // there are no existing placements (above is good)
        // allocations on controllable node should be in set (of existing placements) if set exists
        // https://github.com/vmware/declarative-cluster-management/blob/master/k8s-scheduler/src/main/java/com/vmware/dcm/Policies.java#L102
        // Like 98 -> change check to maximize to make it soft.

        OrToolsSolver.Builder b = new OrToolsSolver.Builder()
                .setPrintDiagnostics(true)
                .setMaxTimeInSeconds(120);
        return Model.build(conn, b.build(), List.of(
                placed_constraint,
                spare_view,
                capacity_constraint,
                node_balance_cores_constraint,
                node_balance_memslices_constraint
                //locality_hard_constraint
                //app_locality_view,
                //app_locality_constraint
        ));
    }

    private static void printStats(final DSLContext conn) {

        // print resource usage statistics by node
        System.out.println("Allocated resources per node:");
        final String allocated_resources = "select * from allocated";
        System.out.println(conn.fetch(allocated_resources));
        System.out.println("Unallocated resources per node:");
        final String unallocated_resources = "select * from unallocated";
        System.out.println(conn.fetch(unallocated_resources));

        // print application statistics
        System.out.println("Nodes per application: ");
        final String nodes_per_app_view = "select application, sum(cores) as cores, sum(memslices) as memslices, " +
                "    count(distinct placed.node) as num_nodes " +
                "from placed " +
                "group by application";
        System.out.println(conn.fetch(nodes_per_app_view));
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
            if (sim.checkForCapacityViolation()) {
                LOG.warn("Failed due to capacity violation??");
                break;
            }

            printStats(conn);
        }

        // Print final stats
        printStats(conn);
        LOG.info("Simulation survived {} steps\n", i + 1);
    }
}