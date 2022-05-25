package com.vmware.bespin.scheduler;

import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;

import com.vmware.bespin.scheduler.generated.tables.AllocationState;
import com.vmware.bespin.scheduler.generated.tables.Applications;
import com.vmware.bespin.scheduler.generated.tables.Nodes;
import com.vmware.bespin.scheduler.generated.tables.PendingAllocations;

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

        // TODO: add application creation/removal to turnover

        final PendingAllocations allocTable = PendingAllocations.PENDING_ALLOCATIONS;
        final AllocationState allocStateTable = AllocationState.ALLOCATION_STATE;

        int allocationId = 0;
        int totalCores = this.totalCores();
        int coreTurnover = MIN_TURNOVER + rand.nextInt(MAX_TURNOVER - MIN_TURNOVER);
        int coreAllocationsToMake = (int) Math.ceil((((double) coreTurnover) / 100) * totalCores);
        int maxCoreAllocations = (int) Math.ceil((((double) MAX_CAPACITY) / 100) * totalCores);
        int minCoreAllocations = (int) Math.ceil((((double) MIN_CAPACITY) / 100) * totalCores);
        System.out.printf("Core Turnover: %d, allocationsToMake: %d, maxAllocations: %d, minAllocation: %d\n",
                coreTurnover, coreAllocationsToMake, maxCoreAllocations, minCoreAllocations);

        int turnoverCounter = 0;
        while (turnoverCounter < coreAllocationsToMake) {
            int usedCores = this.usedCores();
            if (usedCores > minCoreAllocations) {
                if (usedCores >= maxCoreAllocations || rand.nextBoolean()) {
                    // delete core allocation, do not count as turnover
                    Result<Record> results = conn.select().from(allocStateTable).where(allocStateTable.CORES.greaterThan(0)).fetch();
                    Record rowToDelete = results.get(rand.nextInt(results.size()));
                    conn.update(allocStateTable)
                            .set(allocStateTable.CORES, allocStateTable.CORES.minus(1))
                            .where(and(
                                    allocStateTable.NODE.eq(rowToDelete.getValue(allocStateTable.NODE)),
                                    allocStateTable.APPLICATION.eq(rowToDelete.get(allocStateTable.APPLICATION))))
                            .execute();
                }
            }
            // Add a pending allocation, count as turnover
            if (usedCores < maxCoreAllocations) {
                conn.insertInto(allocTable)
                        .set(allocTable.ID, allocationId)
                        .set(allocTable.APPLICATION, this.chooseRandomApplication())
                        .set(allocTable.CORES, 1)
                        .set(allocTable.MEMSLICES, 0)
                        .set(allocTable.STATUS, "PENDING")
                        .set(allocTable.CURRENT_NODE, -1)
                        .set(allocTable.CONTROLLABLE__NODE, (Field<Integer>) null)
                        .execute();
                turnoverCounter++;
                allocationId++;
            }
        }

        int totalMemslices = this.totalMemslices();
        int memsliceTurnover = MIN_TURNOVER + rand.nextInt(MAX_TURNOVER - MIN_TURNOVER);
        int memsliceAllocationsToMake = (int) Math.ceil((((double) memsliceTurnover) / 100) * totalMemslices);
        int maxMemsliceAllocations = (int) Math.ceil((((double) MAX_CAPACITY) / 100) * totalMemslices);
        int minMemsliceAllocations = (int) Math.ceil((((double) MIN_CAPACITY) / 100) * totalMemslices);
        System.out.printf("Memslice Turnover: %d, allocationsToMake: %d, maxAllocations: %d, minAllocation: %d\n",
                memsliceTurnover, memsliceAllocationsToMake, maxMemsliceAllocations, minMemsliceAllocations);

        turnoverCounter = 0;
        while (turnoverCounter < memsliceAllocationsToMake) {
            int usedMemslices = this.usedMemslices();
            if (usedMemslices > minMemsliceAllocations) {
                if (usedMemslices >= maxMemsliceAllocations || rand.nextBoolean()) {
                    // delete core allocation, do not count as turnover
                    Result<Record> results = conn.select().from(allocStateTable).where(allocStateTable.MEMSLICES.greaterThan(0)).fetch();
                    Record rowToDelete = results.get(rand.nextInt(results.size()));
                    conn.update(allocStateTable)
                            .set(allocStateTable.MEMSLICES, allocStateTable.MEMSLICES.minus(1))
                            .where(and(
                                    allocStateTable.NODE.eq(rowToDelete.getValue(allocStateTable.NODE)),
                                    allocStateTable.APPLICATION.eq(rowToDelete.get(allocStateTable.APPLICATION))))
                            .execute();
                }
            }
            // Add an allocation, count as turnover
            if (usedMemslices < maxMemsliceAllocations) {
                conn.insertInto(allocTable)
                        .set(allocTable.ID, allocationId)
                        .set(allocTable.APPLICATION, this.chooseRandomApplication())
                        .set(allocTable.CORES, 0)
                        .set(allocTable.MEMSLICES, 1)
                        .set(allocTable.STATUS, "PENDING")
                        .set(allocTable.CURRENT_NODE, -1)
                        .set(allocTable.CONTROLLABLE__NODE, (Field<Integer>) null)
                        .execute();
                turnoverCounter++;
                allocationId++;
            }
        }
    }

    public boolean runModelAndUpdateDB() {
        Result<? extends Record> results;
        try {
            results = model.solve("PENDING_ALLOCATIONS");
        } catch (ModelException e) {
            throw e;
            //return false;
        } catch (SolverException e) {
            throw e;
            //return false;
        }

        final PendingAllocations allocTable = PendingAllocations.PENDING_ALLOCATIONS;
        final AllocationState allocStateTable = AllocationState.ALLOCATION_STATE;
        results.forEach(r -> {
                    final Integer allocId = (Integer) r.get("ID");
                    final Integer node = (Integer) r.get("CONTROLLABLE__NODE");
                    final Integer application = (Integer) r.get("APPLICATION");

                    // Check if row in allocation_state table. If not, create. If so, update.
                    if (!conn.fetchExists(conn.selectFrom(allocStateTable).
                            where(and(allocStateTable.NODE.eq(node), allocStateTable.APPLICATION.eq(application))))) {
                        conn.insertInto(allocStateTable)
                                .set(allocStateTable.APPLICATION, application)
                                .set(allocStateTable.NODE, node)
                                .set(allocStateTable.MEMSLICES, (Integer) r.get("MEMSLICES"))
                                .set(allocStateTable.CORES, (Integer) r.get("CORES"))
                                .execute();
                    } else {
                        conn.update(allocStateTable)
                                .set(allocStateTable.CORES, allocStateTable.CORES.plus((Integer) r.get("CORES")))
                                .set(allocStateTable.MEMSLICES, allocStateTable.MEMSLICES.plus((Integer) r.get("MEMSLICES")))
                                .where(and(allocStateTable.NODE.eq(node), allocStateTable.APPLICATION.eq(application)))
                                .execute();
                    }

                    // Delete row from pending_allocation table now that it's been assigned
                    conn.delete(allocTable).where(allocTable.ID.eq(allocId)).execute();
                }
        );
        return true;
    }

    public boolean checkForCapacityViolation() {
        // Check total capacity
        boolean totalCoreCheck = this.totalCores() - this.usedCores() >= 0;
        boolean totalMemsliceCheck = this.totalCores() - this.usedCores() >= 0;

        // Check per-node capacity for cores
        var nodeWrapper = new Object(){ boolean nodesPerCoreCheck = true; };
        Result<Record> freeCores = conn.fetch("select nodes.id, nodes.cores - sum(allocation_state.cores) as core_spare " +
                "from allocation_state " +
                "join nodes " +
                "  on nodes.id = allocation_state.node " +
                "group by nodes.id, nodes.cores");
        freeCores.forEach(spareCores -> {
                    final Integer spareCoresPerNode = (Integer) spareCores.get(0);
                    nodeWrapper.nodesPerCoreCheck = nodeWrapper.nodesPerCoreCheck && spareCoresPerNode >= 0;
                }
        );

        // Check per-node capacity for memslices
        var memsliceWrapper = new Object(){ boolean memslicesPerCoreCheck = true; };
        Result<Record> freeMemslices = conn.fetch("select nodes.id, nodes.cores - sum(allocation_state.cores) as core_spare " +
                "from allocation_state " +
                "join nodes " +
                "  on nodes.id = allocation_state.node " +
                "group by nodes.id, nodes.cores");
        freeMemslices.forEach(spareMemslices -> {
                    final Integer spareMemslicesPerNode = (Integer) spareMemslices.get(0);
                    memsliceWrapper.memslicesPerCoreCheck = memsliceWrapper.memslicesPerCoreCheck && spareMemslicesPerNode >= 0;
                }
        );

        return !(totalCoreCheck && totalMemsliceCheck && nodeWrapper.nodesPerCoreCheck && memsliceWrapper.memslicesPerCoreCheck);
    }

    private int usedCores() {
        final String allocatedCoresSQL = "select sum(allocation_state.cores) from allocation_state";
        final String pendingCoresSQL = "select sum(pending_allocations.cores) from pending_allocations";
        Long usedCores = 0L;
        try {
            usedCores += (Long) this.conn.fetch(allocatedCoresSQL).get(0).getValue(0);
        } catch (NullPointerException e) {}
        try {
            usedCores += (Long) this.conn.fetch(pendingCoresSQL).get(0).getValue(0);
        } catch (NullPointerException e) {}
        return usedCores.intValue();
    }

    private int totalCores() {
        final String totalCores = "select sum(nodes.cores) from nodes";
        return ((Long) this.conn.fetch(totalCores).get(0).getValue(0)).intValue();
    }

    private int usedMemslices() {
        final String allocatedMemslicesSQL = "select sum(allocation_state.memslices) from allocation_state";
        final String pendingMemslicesSQL = "select sum(pending_allocations.memslices) from pending_allocations";
        Long usedMemslices = 0L;
        try {
            usedMemslices += (Long) this.conn.fetch(allocatedMemslicesSQL).get(0).getValue(0);
        } catch (NullPointerException e) {}
        try {
            usedMemslices += (Long) this.conn.fetch(pendingMemslicesSQL).get(0).getValue(0);
        } catch (NullPointerException e) {}
        return usedMemslices.intValue();
    }

    private int totalMemslices() {
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
    }

    private static void initDB(final DSLContext conn, int numNodes, int coresPerNode, int memslicesPerNode) {
        final Nodes nodeTable = Nodes.NODES;
        final Applications appTable = Applications.APPLICATIONS;
        final AllocationState allocStateTable = AllocationState.ALLOCATION_STATE;

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
                String sql = String.format("select sum(allocation_state.cores) from allocation_state where node = %d", node);
                int coresUsed = 0;
                try {
                    coresUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
                } catch (NullPointerException e) {}
                int coresToAlloc = Math.min(coresPerNode - coresUsed, cores);

                sql = String.format("select sum(allocation_state.memslices) from allocation_state where node = %d", node);
                int memslicesUsed = 0;
                try {
                    memslicesUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
                } catch (NullPointerException e) {}
                int memslicesToAlloc = Math.min(memslicesPerNode - memslicesUsed, memslices);

                // If we can alloc anything, do so
                if (coresToAlloc > 0 || memslicesToAlloc > 0) {
                    // If row for (node, application) doesn't exist in allocState table, create it. Otherwise, update it.
                    if (!conn.fetchExists(conn.selectFrom(allocStateTable).
                            where(and(allocStateTable.NODE.eq(node), allocStateTable.APPLICATION.eq(application))))) {
                        conn.insertInto(allocStateTable)
                                .set(allocStateTable.APPLICATION, application)
                                .set(allocStateTable.NODE, node)
                                .set(allocStateTable.MEMSLICES, memslicesToAlloc)
                                .set(allocStateTable.CORES, coresToAlloc)
                                .execute();
                    } else {
                        conn.update(allocStateTable)
                                .set(allocStateTable.CORES, allocStateTable.CORES.plus(coresToAlloc))
                                .set(allocStateTable.MEMSLICES, allocStateTable.MEMSLICES.plus(memslicesToAlloc))
                                .where(and(allocStateTable.NODE.eq(node), allocStateTable.APPLICATION.eq(application)))
                                .execute();
                    }

                    // Mark resources as allocated
                    cores -= coresToAlloc;
                    memslices -= memslicesToAlloc;
                }
            }
        }

        // Double check correctness
        String sql = "select sum(allocation_state.cores) from allocation_state";
        int totalCoresUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
        sql = "select sum(allocation_state.memslices) from allocation_state";
        int totalMemslicesUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
        sql = "create view nodeSums as select nodes.id, sum(allocation_state.cores) as cores, sum(allocation_state.memslices) as memslices " +
                "from allocation_state " +
                "join nodes " +
                "  on nodes.id = allocation_state.node " +
                "group by nodes.id, nodes.cores, nodes.memslices";
        conn.execute(sql);
        sql = "select max(nodeSums.cores) from nodeSums";
        int maxCoresUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
        sql = "select max(nodeSums.memslices) from nodeSums";
        int maxMemslicesUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
        assert(totalCoresUsed == coreAllocs);
        assert(maxCoresUsed <= coresPerNode);
        assert(totalMemslicesUsed == memsliceAllocs);
        assert(maxMemslicesUsed <= memslicesPerNode);

        LOG.info(String.format("Total cores used: %d/%d, Max cores per node: %d/%d",
                totalCoresUsed, coreAllocs, maxCoresUsed, coresPerNode));
        LOG.info(String.format("Total memslices used: %d/%d, Max memslices per node: %d/%d",
                totalMemslicesUsed, memsliceAllocs, maxMemslicesUsed, memslicesPerNode));
    }

    private static Model createModel(final DSLContext conn) {
        // Only PENDING core requests are placed
        // All DCM solvers need to have this constraint
        // TODO: do I actually need that constraint now?? current_node??
        final String placed_constraint = "create constraint placed_constraint as " +
                " select * from pending_allocations where status = 'PLACED' check current_node = controllable__node";

        // View of pending resources on nodes for both memslices and cores
        final String pending_view = "create constraint pending_view as " +
                "select nodes.id, sum(pending_allocations.cores) as cores, sum(pending_allocations.memslices) as memslices " +
                "from pending_allocations " +
                "join nodes " +
                "  on nodes.id = pending_allocations.controllable__node " +
                "group by nodes.id, nodes.cores, nodes.memslices";

        // View of placed resources on nodes for both memslices and cores
        final String placed_view = "create constraint placed_view as " +
                "select nodes.id, sum(allocation_state.cores) as cores, sum(allocation_state.memslices) as memslices " +
                "from allocation_state " +
                "join nodes " +
                "  on nodes.id = allocation_state.node " +
                "group by nodes.id, nodes.cores, nodes.memslices";

        // Capacity core constraint (e.g., can only use what is available on each node)
        final String capacity_core_constraint = "create constraint capacity_core_constraint as " +
                " select * from spare_cores check cores_spare >= 0";

        // Create load balancing constraint across nodes for cores and memslices
        final String node_load_cores = "create constraint node_load_cores as " +
                "select * from spare_cores " +
                "maximize min(cores_spare)";
        final String node_load_memslices = "create constraint node_load_memslices as " +
                "select * from spare_memslices " +
                "maximize min(memslices_spare)";

        // Minimize number of nodes per application (e.g., maximize locality)
        final String application_num_nodes_view = "create constraint application_num_nodes as " +
                "select applications.id, count(distinct pending_allocations.controllable__node) as num_nodes " +
                "from pending_allocations " +
                "join applications " +
                "  on applications.id = pending_allocations.application " +
                "group by applications.id";
        final String application_locality_constraint = "create constraint application_locality_constraint as " +
                "select * from application_num_nodes " +
                "maximize -1*sum(num_nodes)";

        OrToolsSolver.Builder b = new OrToolsSolver.Builder();
        return Model.build(conn, b.build(), List.of(
                placed_constraint,
                pending_view,
                placed_view,
                // TODO: need to add capacity constraints
                node_load_cores,
                node_load_memslices,
                application_num_nodes_view,
                application_locality_constraint
        ));
    }

    private static void printStats(final DSLContext conn) {
        System.out.print("Free cores per node:");
        System.out.println(conn.fetch("select nodes.id, nodes.cores - sum(pending_allocations.cores) as core_spare " +
                "from pending_allocations " +
                "join nodes " +
                "  on nodes.id = pending_allocations.controllable__node " +
                "group by nodes.id, nodes.cores"));
        System.out.print("Free memslices per node:");
        System.out.println(conn.fetch("select nodes.id, nodes.memslices - sum(pending_allocations.memslices) as mem_spare " +
                "from pending_allocations " +
                "join nodes " +
                "  on nodes.id = pending_allocations.controllable__node " +
                "group by nodes.id, nodes.memslices"));
        System.out.print("Nodes per application:");
        System.out.println(conn.fetch("select applications.id, count(distinct pending_allocations.controllable__node) as num_nodes " +
                "from pending_allocations " +
                "join applications " +
                "  on applications.id = pending_allocations.application " +
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