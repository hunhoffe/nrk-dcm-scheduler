package com.vmware.bespin.scheduler;

import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;

import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.Placed;
import com.vmware.bespin.scheduler.generated.tables.Nodes;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import static org.jooq.impl.DSL.and;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Random;

class Simulation {
    final Model model;
    final DSLContext conn;
    final Random rand;
    final int allocsPerStep;

    private static final Logger LOG = LogManager.getLogger(Simulation.class);

    // below are percentages
    static final int MIN_CAPACITY = 65;
    static final int MAX_CAPACITY = 95;

    static final Pending PENDING_TABLE = Pending.PENDING;
    static final Placed PLACED_TABLE = Placed.PLACED;
    static final Nodes NODE_TABLE = Nodes.NODES;

    Simulation(final Model model, final DSLContext conn, final int allocsPerStep, final int randomSeed) {
        this.model = model;
        this.conn = conn;
        this.allocsPerStep = allocsPerStep;
        this.rand = new Random(randomSeed);
    }

    public void createTurnover() {
        final int totalCores = this.coreCapacity();
        final int totalMemslices = this.memsliceCapacity();
        final double coreMemsliceRatio = (double) totalCores / (double) (totalCores + totalMemslices);
        LOG.info("core:memslices = {}:{}, fraction = {}:{}", totalCores, totalMemslices,
                coreMemsliceRatio, 1 - coreMemsliceRatio);

        final int coreAllocationsToMake = (int) Math.ceil((double) this.allocsPerStep * coreMemsliceRatio);
        final int maxCoreAllocations = (int) Math.ceil((((double) MAX_CAPACITY) / 100) * totalCores);
        final int minCoreAllocations = (int) Math.ceil((((double) MIN_CAPACITY) / 100) * totalCores);
        LOG.info("Core allocationsToMake: {}, maxAllocations: {}, minAllocation: {}\n",
                coreAllocationsToMake, maxCoreAllocations, minCoreAllocations);

        int turnoverCounter = 0;
        while (turnoverCounter < coreAllocationsToMake) {
            final int usedCores = this.usedCores();
            if (usedCores > minCoreAllocations) {
                if (usedCores >= maxCoreAllocations || rand.nextBoolean()) {
                    // delete core allocation, do not count as turnover
                    final Result<Record> results = conn.select().from(PLACED_TABLE).where(
                            PLACED_TABLE.CORES.greaterThan(0)).fetch();
                    final Record rowToDelete = results.get(rand.nextInt(results.size()));
                    conn.update(PLACED_TABLE)
                            .set(PLACED_TABLE.CORES, PLACED_TABLE.CORES.minus(1))
                            .where(and(
                                    PLACED_TABLE.NODE.eq(rowToDelete.getValue(PLACED_TABLE.NODE)),
                                    PLACED_TABLE.APPLICATION.eq(rowToDelete.get(PLACED_TABLE.APPLICATION))))
                            .execute();
                }
            }
            // Add a pending allocation, count as turnover
            if (usedCores < maxCoreAllocations) {
                conn.insertInto(PENDING_TABLE)
                        .set(PENDING_TABLE.APPLICATION, this.chooseRandomApplication())
                        .set(PENDING_TABLE.CORES, 1)
                        .set(PENDING_TABLE.MEMSLICES, 0)
                        .set(PENDING_TABLE.STATUS, "PENDING")
                        .set(PENDING_TABLE.CURRENT_NODE, -1)
                        .set(PENDING_TABLE.CONTROLLABLE__NODE, (Field<Integer>) null)
                        .execute();
                turnoverCounter++;
            }
        }

        final int memsliceAllocationsToMake = (int) Math.floor((double) this.allocsPerStep * (1 - coreMemsliceRatio));
        final int maxMemsliceAllocations = (int) Math.ceil((((double) MAX_CAPACITY) / 100) * totalMemslices);
        final int minMemsliceAllocations = (int) Math.ceil((((double) MIN_CAPACITY) / 100) * totalMemslices);
        LOG.info("Memslice allocationsToMake: {}, maxAllocations: {}, minAllocation: {}\n",
                memsliceAllocationsToMake, maxMemsliceAllocations, minMemsliceAllocations);

        turnoverCounter = 0;
        while (turnoverCounter < memsliceAllocationsToMake) {
            final int usedMemslices = this.usedMemslices();
            if (usedMemslices > minMemsliceAllocations) {
                if (usedMemslices >= maxMemsliceAllocations || rand.nextBoolean()) {
                    // delete memslice allocation, do not count as turnover
                    final Result<Record> results = conn.select().from(PLACED_TABLE).where(
                            PLACED_TABLE.MEMSLICES.greaterThan(0)).fetch();
                    final Record rowToDelete = results.get(rand.nextInt(results.size()));
                    conn.update(PLACED_TABLE)
                            .set(PLACED_TABLE.MEMSLICES, PLACED_TABLE.MEMSLICES.minus(1))
                            .where(and(
                                    PLACED_TABLE.NODE.eq(rowToDelete.getValue(PLACED_TABLE.NODE)),
                                    PLACED_TABLE.APPLICATION.eq(rowToDelete.get(PLACED_TABLE.APPLICATION))))
                            .execute();
                }
            }
            // Add an allocation, count as turnover
            if (usedMemslices < maxMemsliceAllocations) {
                conn.insertInto(PENDING_TABLE)
                        .set(PENDING_TABLE.APPLICATION, this.chooseRandomApplication())
                        .set(PENDING_TABLE.CORES, 0)
                        .set(PENDING_TABLE.MEMSLICES, 1)
                        .set(PENDING_TABLE.STATUS, "PENDING")
                        .set(PENDING_TABLE.CURRENT_NODE, -1)
                        .set(PENDING_TABLE.CONTROLLABLE__NODE, (Field<Integer>) null)
                        .execute();
                turnoverCounter++;
            }
        }

        // Remove any empty rows that may have been produced
        conn.execute("delete from placed where cores = 0 and memslices = 0");
    }

    public boolean runModelAndUpdateDB() {
       final Result<? extends Record> results;
        try {
            results = model.solve("PENDING");
        } catch (ModelException | SolverException e) {
            LOG.error(e);
            return false;
        }

        // TODO: should find a way to batch these
        // Add new assignments to placed, remove new assignments from pending
        results.forEach(r -> {
            conn.insertInto(PLACED_TABLE, PLACED_TABLE.APPLICATION, PLACED_TABLE.NODE, PLACED_TABLE.CORES,
                            PLACED_TABLE.MEMSLICES)
                    .values((int) r.get("APPLICATION"), (int) r.get("CONTROLLABLE__NODE"), (int) r.get("CORES"),
                            (int) r.get("MEMSLICES"))
                    .onDuplicateKeyUpdate()
                    .set(PLACED_TABLE.CORES,  PLACED_TABLE.CORES.plus((int) r.get("CORES")))
                    .set(PLACED_TABLE.MEMSLICES, PLACED_TABLE.MEMSLICES.plus((int) r.get("MEMSLICES")))
                    .execute();
            conn.deleteFrom(PENDING_TABLE)
                    .where(PENDING_TABLE.ID.eq((long) r.get("ID")))
                    .execute();
            }
        );
        return true;
    }

    public boolean checkForCapacityViolation() {
        // Check total capacity
        // TODO: could make this more efficient by using unallocated view
        final boolean totalCoreCheck = this.coreCapacity() >= this.usedCores();
        final boolean totalMemsliceCheck = this.memsliceCapacity() >= this.usedCores();

        boolean nodesPerCoreCheck = true;
        boolean memslicesPerCoreCheck = true;

        // Iterate over each node, check current memslice allocation state and pending allocations
        final Result<Record> results = conn.fetch("select id from nodes");
        for (final Record record: results) {
            final int node = record.getValue(NODE_TABLE.ID);
            final String allocatedCoresSQL = String.format(
                    "select sum(placed.cores) from placed where node = %d", node);
            final String coresAvailableSQL = String.format("select cores from nodes where id = %d", node);
            Long usedCores = 0L;
            int coreCapacity = 0;
            try {
                usedCores += (Long) this.conn.fetch(allocatedCoresSQL).get(0).getValue(0);
            } catch (final NullPointerException ignored) { }
            try {
                coreCapacity = (int) this.conn.fetch(coresAvailableSQL).get(0).getValue(0);
            } catch (final NullPointerException ignored) { }

            nodesPerCoreCheck = nodesPerCoreCheck && (coreCapacity >= usedCores.intValue());

            final String allocatedMemslicesSQL = String.format(
                    "select sum(placed.memslices) from placed where node = %d", node);
            final String memslicesAvailableSQL = String.format("select memslices from nodes where id = %d", node);
            Long usedMemslices = 0L;
            int memsliceCapacity = 0;
            try {
                usedMemslices += (Long) this.conn.fetch(allocatedMemslicesSQL).get(0).getValue(0);
            } catch (final NullPointerException ignored) { }
            try {
                memsliceCapacity = (int) this.conn.fetch(memslicesAvailableSQL).get(0).getValue(0);
            } catch (final NullPointerException ignored) { }

            memslicesPerCoreCheck = memslicesPerCoreCheck && (memsliceCapacity >= usedMemslices.intValue());
        }

        return !(totalCoreCheck && totalMemsliceCheck && nodesPerCoreCheck && memslicesPerCoreCheck);
    }

    private int usedCores() {
        final String allocatedCoresSQL = "select sum(cores) from placed";
        Long usedCores = 0L;
        try {
            usedCores += (Long) this.conn.fetch(allocatedCoresSQL).get(0).getValue(0);
        } catch (final NullPointerException ignored) { }
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
        } catch (final NullPointerException ignored) { }
        return usedMemslices.intValue();
    }

    private int memsliceCapacity() {
        final String totalMemslices = "select sum(memslices) from nodes";
        return ((Long) this.conn.fetch(totalMemslices).get(0).getValue(0)).intValue();
    }

    private int chooseRandomApplication() {
        final String applicationIds = "select id from applications";
        final Result<Record> results = this.conn.fetch(applicationIds);
        return (int) results.get(rand.nextInt(results.size())).getValue(0);
    }
}

public class SimulationRunner extends DCMRunner {
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

    public static void main(final String[] args) throws ClassNotFoundException {

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
        final Options options = new Options();

        final Option helpOption = Option.builder("h")
                .longOpt("help").argName("h")
                .hasArg(false)
                .desc("print help message")
                .build();
        final Option numStepsOption = Option.builder("s")
                .longOpt(NUM_STEPS_OPTION).argName(NUM_STEPS_OPTION)
                .hasArg(true)
                .desc(String.format("maximum number of steps to run the simulations.\nDefault: %d", NUM_STEPS_DEFAULT))
                .type(Integer.class)
                .build();
        final Option numNodesOption = Option.builder("n")
                .longOpt(NUM_NODES_OPTION).argName(NUM_NODES_OPTION)
                .hasArg()
                .desc(String.format("number of nodes.\nDefault: %d", NUM_NODES_DEFAULT))
                .type(Integer.class)
                .build();
        final Option coresPerNodeOption = Option.builder("c")
                .longOpt(CORES_PER_NODE_OPTION).argName(CORES_PER_NODE_OPTION)
                .hasArg()
                .desc(String.format("cores per node.\nDefault: %d", CORES_PER_NODE_DEFAULT))
                .type(Integer.class)
                .build();
        final Option memslicesPerNodeOption = Option.builder("m")
                .longOpt(MEMSLICES_PER_NODE_OPTION).argName(MEMSLICES_PER_NODE_OPTION)
                .hasArg()
                .desc(String.format("number of 2 MB memory slices per node.\nDefault: %d", MEMSLICES_PER_NODE_DEFAULT))
                .type(Integer.class)
                .build();
        final Option numAppsOption = Option.builder("p")
                .longOpt(NUM_APPS_OPTION).argName(NUM_APPS_OPTION)
                .hasArg()
                .desc(String.format("number of applications running on the cluster.\nDefault: %d", NUM_APPS_DEFAULT))
                .type(Integer.class)
                .build();
        final Option allocsPerStepOption = Option.builder("a")
                .longOpt(ALLOCS_PER_STEP_OPTION).argName(ALLOCS_PER_STEP_OPTION)
                .hasArg()
                .desc(String.format("number of new allocations per step.\nDefault: %d", ALLOCS_PER_STEP_DEFAULT))
                .type(Integer.class)
                .build();
        final Option randomSeedOption = Option.builder("r")
                .longOpt(RANDOM_SEED_OPTION).argName(RANDOM_SEED_OPTION)
                .hasArg()
                .desc(String.format("seed from random.\nDefault: %d", RANDOM_SEED_DEFAULT))
                .type(Integer.class)
                .build();
        final Option useCapFunctionOption = Option.builder("f")
                .longOpt(USE_CAP_FUNCTION_OPTION).argName(USE_CAP_FUNCTION_OPTION)
                .hasArg()
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
        } catch (final ParseException ignored) {
            LOG.error("Failed to parse command line");
            return;
        }

        // Create an in-memory database and get a JOOQ connection to it
        Class.forName("org.h2.Driver");
        final DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner.initDB(conn, numNodes, coresPerNode, memslicesPerNode, numApps, randomSeed, true);

        LOG.info("Creating a simulation with parameters: nodes={}, coresPerNode={}, " +
                        "memSlicesPerNode={} numApps={}, allocsPerStep={}, randomSeed={}, useCapFunction={}",
                numNodes, coresPerNode, memslicesPerNode, numApps, allocsPerStep, randomSeed, useCapFunction);
        final Model model = DCMRunner.createModel(conn, useCapFunction);
        final Simulation sim = new Simulation(model, conn, allocsPerStep, randomSeed);

        int stepCount = 0;
        for (; stepCount < numSteps; stepCount++) {
            LOG.info("Simulation step: {}", stepCount);

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

            DCMRunner.printStats(conn);
        }

        // Print final stats
        DCMRunner.printStats(conn);
        LOG.info("Simulation survived {} steps\n", stepCount + 1);
    }
}