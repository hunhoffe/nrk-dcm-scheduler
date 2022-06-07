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

class Simulation
{
    final Model model;
    final DSLContext conn;
    final Random rand;
    final int allocsPerStep;

    private static final Logger LOG = LogManager.getLogger(Simulation.class);

    // below are percentages
    static final int MIN_CAPACITY = 65;
    static final int MAX_CAPACITY = 95;

    static final Pending pendingTable = Pending.PENDING;
    static final Placed placedTable = Placed.PLACED;
    static final Nodes nodeTable = Nodes.NODES;

    Simulation(Model model, DSLContext conn, int allocsPerStep, int randomSeed) {
        this.model = model;
        this.conn = conn;
        this.allocsPerStep = allocsPerStep;
        this.rand = new Random(randomSeed);
    }

    public void createTurnover() {
        int totalCores = this.coreCapacity();
        int totalMemslices = this.memsliceCapacity();
        double coreMemsliceRatio = (double) totalCores / (double) (totalCores + totalMemslices);
        LOG.info("core:memslices = {}:{}, fraction = {}:{}", totalCores, totalMemslices,
                coreMemsliceRatio, 1 - coreMemsliceRatio);

        int coreAllocationsToMake = (int) Math.ceil((double) this.allocsPerStep * coreMemsliceRatio);
        int maxCoreAllocations = (int) Math.ceil((((double) MAX_CAPACITY) / 100) * totalCores);
        int minCoreAllocations = (int) Math.ceil((((double) MIN_CAPACITY) / 100) * totalCores);
        LOG.info("Core allocationsToMake: {}, maxAllocations: {}, minAllocation: {}\n",
                coreAllocationsToMake, maxCoreAllocations, minCoreAllocations);

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

        int memsliceAllocationsToMake = (int) Math.floor((double) this.allocsPerStep * (1 - coreMemsliceRatio));
        int maxMemsliceAllocations = (int) Math.ceil((((double) MAX_CAPACITY) / 100) * totalMemslices);
        int minMemsliceAllocations = (int) Math.ceil((((double) MIN_CAPACITY) / 100) * totalMemslices);
        LOG.info("Memslice allocationsToMake: {}, maxAllocations: {}, minAllocation: {}\n",
                memsliceAllocationsToMake, maxMemsliceAllocations, minMemsliceAllocations);

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

        // Remove any empty rows that may have been produced
        conn.execute("delete from placed where cores = 0 and memslices = 0");
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

    public boolean checkForCapacityViolation() {
        // Check total capacity
        // TODO: could make this more efficient by using unallocated view
        boolean totalCoreCheck = this.coreCapacity() >= this.usedCores();
        boolean totalMemsliceCheck = this.memsliceCapacity() >= this.usedCores();

        boolean nodesPerCoreCheck = true;
        boolean memslicesPerCoreCheck = true;

        // Iterate over each node, check current memslice allocation state and pending allocations
        Result<Record> results = conn.fetch("select id from nodes");
        for (Record r: results) {
            int node = r.getValue(nodeTable.ID);
            final String allocatedCoresSQL = String.format("select sum(placed.cores) from placed where node = %d", node);
            final String coresAvailableSQL = String.format("select cores from nodes where id = %d", node);
            Long usedCores = 0L;
            int coreCapacity = 0;
            try {
                usedCores += (Long) this.conn.fetch(allocatedCoresSQL).get(0).getValue(0);
            } catch (NullPointerException ignored) {}
            try {
                coreCapacity = (int) this.conn.fetch(coresAvailableSQL).get(0).getValue(0);
            } catch (NullPointerException ignored) {}

            nodesPerCoreCheck = nodesPerCoreCheck && (coreCapacity >= usedCores.intValue());

            final String allocatedMemslicesSQL = String.format("select sum(placed.memslices) from placed where node = %d", node);
            final String memslicesAvailableSQL = String.format("select memslices from nodes where id = %d", node);
            Long usedMemslices = 0L;
            int memsliceCapacity = 0;
            try {
                usedMemslices += (Long) this.conn.fetch(allocatedMemslicesSQL).get(0).getValue(0);
            } catch (NullPointerException ignored) {}
            try {
                memsliceCapacity = (int) this.conn.fetch(memslicesAvailableSQL).get(0).getValue(0);
            } catch (NullPointerException ignored) {}

            memslicesPerCoreCheck = memslicesPerCoreCheck && (memsliceCapacity >= usedMemslices.intValue());
        }

        return !(totalCoreCheck && totalMemsliceCheck && nodesPerCoreCheck && memslicesPerCoreCheck);
    }

    private int usedCores() {
        final String allocatedCoresSQL = "select sum(cores) from placed";
        Long usedCores = 0L;
        try {
            usedCores += (Long) this.conn.fetch(allocatedCoresSQL).get(0).getValue(0);
        } catch (NullPointerException ignored) {}
        return usedCores.intValue();
    }

    private int coreCapacity() {
        final String totalCores = "select sum(cores) from nodes";
        return ((Long) this.conn.fetch(totalCores).get(0).getValue(0)).intValue();
    }

    private int usedMemslices() {
        final String allocatedMemslicesSQL = "select sum(memslices) from placed";
        Long usedMemslices = 0L;
        try {
            usedMemslices += (Long) this.conn.fetch(allocatedMemslicesSQL).get(0).getValue(0);
        } catch (NullPointerException ignored) {}
        return usedMemslices.intValue();
    }

    private int memsliceCapacity() {
        final String totalMemslices = "select sum(memslices) from nodes";
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
    private static final int NUM_STEPS_DEFAULT = 10;
    private static final String NUM_NODES_OPTION = "numNodes";
    private static final int NUM_NODES_DEFAULT = 64;
    private static final String CORES_PER_NODE_OPTION = "coresPerNode";
    private static final int CORES_PER_NODE_DEFAULT = 128;
    private static final String MEMSLICES_PER_NODE_OPTION = "memslicesPerNode";
    private static final int MEMSLICES_PER_NODE_DEFAULT = 256;
    private static final String NUM_APPS_OPTION = "numApps";
    private static final int NUM_APPS_DEFAULT = 20;
    private static final String ALLOCS_PER_STEP_OPTION = "allocsPerStep";
    private static final int ALLOCS_PER_STEP_DEFAULT = 75;
    private static final String RANDOM_SEED_OPTION = "randomSeed";
    private static final int RANDOM_SEED_DEFAULT = 1;
    private static final String USE_CAP_FUNCTION_OPTION = "useCapFunction";
    private static final boolean USE_CAP_FUNCTION_DEFAULT = true;

    public static void setupDb(final DSLContext conn) {
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
        conn.execute("""
                create view allocated as
                select node, cast(sum(cores) as int) as cores, cast(sum(memslices) as int) as memslices
                from placed
                group by node
                """);

        // View to see total unallocated (unused) resources at each node
        conn.execute("""
                create view unallocated as
                select n.id as node, cast(n.cores - coalesce(sum(p.cores), 0) as int) as cores,
                    cast(n.memslices - coalesce(sum(p.memslices), 0) as int) as memslices
                from nodes n
                left join placed p
                    on n.id = p.node
                group by n.id
                """);

        // View to see the nodes each application is placed on
        conn.execute("""
                create view app_nodes as
                select application, node
                from placed
                group by application, node
                """);
    }

    private static void initDB(final DSLContext conn, int numNodes, int coresPerNode, int memslicesPerNode,
                               int numApps, int randomSeed) {
        final Nodes nodeTable = Nodes.NODES;
        final Applications appTable = Applications.APPLICATIONS;
        final Placed placedTable = Placed.PLACED;

        Random rand = new Random(randomSeed);
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
        for (int i = 1; i <= numApps; i++) {
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

        HashMap<Integer, List<Integer>> appAllocMap = new HashMap<>();

        // Assign cores the applications
        for (int i = 0; i < coreAllocs; i++) {
            int application = rand.nextInt(numApps) + 1;
            List<Integer> key = appAllocMap.getOrDefault(application, List.of(0, 0));
            appAllocMap.put(application, List.of(key.get(0) + 1, key.get(1)));
        }

        // Assign memslices to applications
        for (int i = 0; i < memsliceAllocs; i++) {
            int application = rand.nextInt(numApps) + 1;
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
                    coresUsed = (int) conn.fetch(sql).get(0).getValue(0);
                } catch (NullPointerException | IndexOutOfBoundsException ignored) {}
                int coresToAlloc = Math.min(coresPerNode - coresUsed, cores);

                sql = String.format("select memslices from allocated where node = %d", node);
                int memslicesUsed = 0;
                try {
                    memslicesUsed = (int) conn.fetch(sql).get(0).getValue(0);
                } catch (NullPointerException | IndexOutOfBoundsException ignored) {}
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
        int totalCoresUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
        sql = "select sum(memslices) from allocated";
        int totalMemslicesUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
        sql = "select max(cores) from allocated";
        int maxCoresUsed = (Integer) conn.fetch(sql).get(0).getValue(0);
        sql = "select max(memslices) from allocated";
        int maxMemslicesUsed = (Integer) conn.fetch(sql).get(0).getValue(0);
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

    private static Model createModel(final DSLContext conn, boolean useCapFunc) {
        List<String> constraints = new ArrayList<>();
        constraints.add(Constraints.getPlacedConstraint().sql);

        if (useCapFunc) {
            // this will replace two above, and balance constraint below
            constraints.add(Constraints.getCapacityFunctionCoreConstraint().sql);
            constraints.add(Constraints.getCapacityFunctionMemsliceConstraint().sql);
        } else {
            constraints.add(Constraints.getSpareView().sql);
            constraints.add(Constraints.getCapacityConstraint().sql);

            // TODO: should scale because we're maximizing the sum cores_per_node ratio compared to memslices_per_node
            constraints.add(Constraints.getLoadBalanceCoreConstraint().sql);
            constraints.add(Constraints.getLoadBalanceMemsliceConstraint().sql);
        }
        constraints.add(Constraints.getAppLocalityConstraint().sql);

        OrToolsSolver.Builder b = new OrToolsSolver.Builder()
                .setPrintDiagnostics(true)
                .setMaxTimeInSeconds(300);
        return Model.build(conn, b.build(), constraints);
    }

    private static void printStats(final DSLContext conn) {

        // print resource usage statistics by node
        System.out.println("Unallocated resources per node:");
        System.out.println(conn.fetch("select * from unallocated"));

        // print application statistics
        System.out.println("Application resources per node: ");
        final String nodesPerAppView = """
                select application, sum(cores) as cores, sum(memslices) as memslices,
                    count(distinct node) as num_nodes
                from placed
                group by application
        """;
        System.out.println(conn.fetch(nodesPerAppView));
    }

    public static void main(String[] args) throws ClassNotFoundException {

        int i = 0;
        // These are the defaults for these parameters.
        // They should be overridden by commandline arguments.
        int numSteps = NUM_STEPS_DEFAULT;
        int numNodes = NUM_NODES_DEFAULT;
        int coresPerNode = CORES_PER_NODE_DEFAULT;
        int memslicesPerNode = MEMSLICES_PER_NODE_DEFAULT;
        int numApps = NUM_APPS_DEFAULT;
        int allocsPerStep = ALLOCS_PER_STEP_DEFAULT;
        int randomSeed = RANDOM_SEED_DEFAULT;
        boolean useCapFunction = USE_CAP_FUNCTION_DEFAULT;

        // create Options object
        Options options = new Options();

        Option helpOption = Option.builder("h")
                .longOpt("help").argName("h")
                .hasArg(false)
                .desc("print help message")
                .build();
        Option numStepsOption = Option.builder("s")
                .longOpt(NUM_STEPS_OPTION).argName(NUM_STEPS_OPTION)
                .hasArg(true)
                .desc(String.format("maximum number of steps to run the simulations.\nDefault: %d", NUM_STEPS_DEFAULT))
                .type(Integer.class)
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
        Option allocsPerStepOption = Option.builder("a")
                .longOpt(ALLOCS_PER_STEP_OPTION).argName(ALLOCS_PER_STEP_OPTION)
                .hasArg()
                .desc(String.format("number of new allocations per step.\nDefault: %d", ALLOCS_PER_STEP_DEFAULT))
                .type(Integer.class)
                .build();
        Option randomSeedOption = Option.builder("r")
                .longOpt(RANDOM_SEED_OPTION).argName(RANDOM_SEED_OPTION)
                .hasArg()
                .desc(String.format("seed from random.\nDefault: %d", RANDOM_SEED_DEFAULT))
                .type(Integer.class)
                .build();
        Option useCapFunctionOption = Option.builder("f")
                .longOpt(USE_CAP_FUNCTION_OPTION).argName(USE_CAP_FUNCTION_OPTION)
                .hasArg(false)
                .desc(String.format("use capability function vs hand-written constraints.\nDefault: %b",
                        USE_CAP_FUNCTION_DEFAULT))
                .type(Boolean.class)
                .build();

        options.addOption(helpOption);
        options.addOption(numStepsOption);
        options.addOption(numNodesOption);
        options.addOption(coresPerNodeOption);
        options.addOption(memslicesPerNodeOption);
        options.addOption(numAppsOption);
        options.addOption(allocsPerStepOption);
        options.addOption(randomSeedOption);
        options.addOption(useCapFunctionOption);

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
            if (cmd.hasOption(NUM_APPS_OPTION)) {
                numApps = Integer.parseInt(cmd.getOptionValue(NUM_APPS_OPTION));
            }
            if (cmd.hasOption(ALLOCS_PER_STEP_OPTION)) {
                allocsPerStep = Integer.parseInt(cmd.getOptionValue(ALLOCS_PER_STEP_OPTION));
            }
            if (cmd.hasOption(RANDOM_SEED_OPTION)) {
                randomSeed = Integer.parseInt(cmd.getOptionValue(RANDOM_SEED_OPTION));
            }
            if (cmd.hasOption(USE_CAP_FUNCTION_OPTION)) {
                useCapFunction = Boolean.parseBoolean(cmd.getOptionValue(USE_CAP_FUNCTION_OPTION));
            }
        } catch (ParseException e) {
            LOG.error("Failed to parse command line");
            return;
        }

        // Create an in-memory database and get a JOOQ connection to it
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        initDB(conn, numNodes, coresPerNode, memslicesPerNode, numApps, randomSeed);

        LOG.info("Creating a simulation with parameters:\n nodes={}, coresPerNode={}, " +
                        "memSlicesPerNode={} numApps={}, allocsPerStep={}, randomSeed={}, useCapFunction={}",
                numNodes, coresPerNode, memslicesPerNode, numApps, allocsPerStep, randomSeed, useCapFunction);
        Model model = createModel(conn, useCapFunction);
        Simulation sim = new Simulation(model, conn, allocsPerStep, randomSeed);

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