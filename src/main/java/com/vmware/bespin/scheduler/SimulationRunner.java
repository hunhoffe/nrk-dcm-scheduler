package com.vmware.bespin.scheduler;

package com.vmware;

import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.generated.tables.Components;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Update;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.math.BigDecimal;

import org.apache.commons.math3.distribution.*;

class Simulation
{
    final Model model;
    final DSLContext conn;
    final ZipfDistribution componentSizeZip;
    final ZipfDistribution numObjectsPerStepZip;
    final boolean dynamicPlacement;
    final int componentsPerObject;
    final long componentSize;
    final double triggerThreshold;
    final int numReplicas;

    private static final int ZIP_ELEMENTS = 10000;
    private static final ZipfDistribution[] OBJ_ZIPS = {
            new ZipfDistribution(ZIP_ELEMENTS, 0.5),
            //new ZipfDistribution(ZIP_ELEMENTS, 0.9),
            new ZipfDistribution(ZIP_ELEMENTS, 1),
            //new ZipfDistribution(ZIP_ELEMENTS, 1.1),
            new ZipfDistribution(ZIP_ELEMENTS, 1.5)};

    // Weight of each Zipf distribution in OBJ_ZIPS
    private static final int[] OBJ_PROBS = {
            6, 12, 16
    };

    private static final Logger LOG = LoggerFactory.getLogger(Simulation.class);
    private final long SPECIAL_CAP;

    private static int idCounter = 6;
    private static int objectIdCounter = 6;

    Simulation(Model model, DSLContext conn, boolean dynamicPlacement, int componentsPerObject, long componentSize, double sizeZipfExp, double objectZipfExp, double triggerThreshold, int numReplicas) {
        this.model = model;
        this.conn = conn;
        this.dynamicPlacement = dynamicPlacement;
        this.componentSize = componentSize;
        this.componentsPerObject = componentsPerObject;
        this.componentSizeZip = new ZipfDistribution((int) componentSize, sizeZipfExp);
        this.numObjectsPerStepZip = new ZipfDistribution(10, objectZipfExp);
        this.triggerThreshold = triggerThreshold;
        this.numReplicas = numReplicas;

        /*
         * This is a special constant for traditional CLOM that tricks DCM into thinking
         * a new component has capacity. After a component is placed, we change the capacity
         * of any component with SPECIAL_CAP capacity back to 0L.
         */
        SPECIAL_CAP = 1L;
    }

    private static int whichIndex() {
        int which = ((int) System.currentTimeMillis() ) % 16;
        for (int i = 0; i < OBJ_PROBS.length; i++) {
            if (which < OBJ_PROBS[0])
                return i;
        }
        return OBJ_PROBS.length - 1;
    }

    // Probability that a component's capacity is expanded in each round
    private double objectProb() {
        // Pick one of the 5 object distributions
        return OBJ_ZIPS[whichIndex()].sample() / (double) ZIP_ELEMENTS;
    }

    private int getId() {
        return idCounter++;
    }

    private int getObjectId() {
        return objectIdCounter++;
    }

    private int numObjectsThisStep() {
        return numObjectsPerStepZip.sample() - 1;
    }

    public void addComponentsAndUpdateModel() {
        int objectId = getObjectId();
        long util = dynamicPlacement ? 0L : SPECIAL_CAP;
        int numObjects = numObjectsThisStep();
        LOG.info("Adding {} objects this step", numObjects * componentsPerObject);

        for (int j = 0; j < componentsPerObject * numObjects; j++) {
            int id = getId();
            if (dynamicPlacement)
                objectId = getObjectId();

            final Components t = Components.COMPONENTS;
            // TODO: For now, add 1 replica, but make # of replicas configurable

            /*
             * As a hack: add new components with SPECIAL_CAP capacity in order to
             * trick DCM into using the soft capacity constraint. Set this value back to
             * 0L when we write-back the model results.
             */
            final double prob = objectProb();
            for (int replica = 1; replica <= numReplicas; replica++) {
                conn.insertInto(t)
                        .set(t.ID, id)
                        .set(t.REPLICA_ID, replica)
                        .set(t.OBJECT_ID, objectId)
                        .set(t.MAX_CAPACITY, componentSize)
                        .set(t.CURRENT_UTIL, util)
                        .set(t.UPDATE_PROBABILITY, prob)
                        .set(t.STATUS, "PENDING")
                        .set(t.CURRENT_DISK, -1)
                        .execute();
            }
        }
        // Sync the model with the current data in the database
        //model.updateData();
    }

    public boolean runModelAndUpdateDB() {
        Result<? extends Record> results = null;
        try {
            results = model.solve("COMPONENTS");
        } catch (ModelException e) {
            LOG.info("Got a model exception when solving: {}", e);
            return false;
        }

        if (results == null)
            return false;

        /*
         * Write-back the model's answer to the DB.
         * For each PENDING component:
         *   update current_disk with the answer, mark as PLACED
         */
        final Components t = Components.COMPONENTS;
        final List<Update<?>> updates = new ArrayList<>();
        // Each record is a row in the COMPONENTS table
        LOG.debug("new components placed on:");
        results.forEach(r -> {
                    final Integer componentId = (Integer) r.get("ID");
                    final Integer replicaId = (Integer) r.get("REPLICA_ID");
                    final Integer diskNum = (Integer) r.get("CONTROLLABLE__DISK");
                    final String status = (String) r.get("STATUS");
                    Long currentUtil = (Long) r.get("CURRENT_UTIL");
                    if (currentUtil == SPECIAL_CAP)
                        currentUtil = 0L; // Set util to 0L for truly new components
                    if (status.equals("PENDING")) {
                        LOG.debug("{} ", diskNum);
                        updates.add(
                                conn.update(t)
                                        .set(t.CURRENT_DISK, diskNum)
                                        .set(t.CURRENT_UTIL, currentUtil)
                                        .set(t.STATUS, "PLACED")
                                        .where(t.ID.eq(componentId).and(t.REPLICA_ID.eq(replicaId))
                                                .and(t.STATUS.eq("PENDING")))
                        );
                    }
                }
        );
        conn.batch(updates).execute();
        return true;
    }

    // return true if success
    public boolean simulateWork() {
        /* Create a dummy final variable that we can modify during the forEach below */
        final List<Boolean> updateModel = new ArrayList<>();
        updateModel.add(false);

        final List<Update<?>> updates = new ArrayList<>();
        // The random number generator for determining whether to add capacity
        Random rand = new Random(System.currentTimeMillis());

        // Find all non-full components. Only fetch replica 1. Whatever capacity we
        // add to replica 1 should be added to all replicas in the forEach loop.
        final Components t = Components.COMPONENTS;
        Result<? extends Record> eligibleComponents =
                conn.selectFrom(t).where(t.CURRENT_UTIL.lessThan(t.MAX_CAPACITY)
                        .and(t.UPDATE_PROBABILITY.isNotNull()).and(t.REPLICA_ID.eq(1))).fetch();
        LOG.info("Found {} components in simulateWork", eligibleComponents.size());

        final List<Long> totalCapacity = new ArrayList<>();
        totalCapacity.add(0L);

        eligibleComponents.forEach(r -> {
            // Only update if update probability holds
            if (rand.nextFloat() < (Double) r.get("UPDATE_PROBABILITY")) {
                long capacityToAdd = componentSizeZip.sample();
                final Long maxCap = (Long) r.get("MAX_CAPACITY");
                final Integer componentId = (Integer) r.get("ID");

                final Long currentUtil = (Long) r.get("CURRENT_UTIL");
                // Only add as much leftover capacity is left for the component
                if (capacityToAdd + currentUtil > maxCap)
                    capacityToAdd = maxCap - currentUtil;

                if (dynamicPlacement && currentUtil == 0L) { // Then we need to tell DCM to place this guy
                    updates.add(
                            conn.update(t)
                                    .set(t.CURRENT_UTIL, currentUtil + capacityToAdd)
                                    .set(t.STATUS, "PENDING")
                                    .where(t.ID.eq(componentId))
                    );
                    updateModel.set(0,true);
                } else {
                    updates.add(
                            conn.update(t)
                                    .set(t.CURRENT_UTIL, currentUtil + capacityToAdd)
                                    .where(t.ID.eq(componentId))
                    );
                }
                totalCapacity.set(0, totalCapacity.get(0) + capacityToAdd);
            }
        });

        LOG.info("This step added {} capacity", totalCapacity.get(0) * numReplicas);
        conn.batch(updates).execute();
        if (dynamicPlacement && updateModel.get(0)) {
            LOG.info("simulateWork also needs to update component locations");
            //model.updateData();
            if (!runModelAndUpdateDB()) {
                LOG.info("ugh.. run model failed for DP?");
                return false;
            }
        }
        return true;
    }

    public boolean checkForCapacityViolation() {
        final String check_capacity =
                "select sum(components.current_util) as total_disk_util, disks.max_capacity, disks.id " +
                        "from components " +
                        "join disks " +
                        "  on disks.id = components.current_disk " +
                        "group by disks.id, disks.max_capacity";

        Result<? extends Record> diskCapacities = conn.fetch(check_capacity);
        boolean capacityViolated = false;

        LOG.debug("Current disk capacities during violation check:\n {}", diskCapacities);

        // Capacity violation (rebalancing) is triggered when one of the disks is at least 80% full
        for (Record r : diskCapacities) {
            final Long maxCap = (Long) r.get("MAX_CAPACITY");
            if (((BigDecimal) r.get("TOTAL_DISK_UTIL")).longValue() >= triggerThreshold * maxCap) {
                capacityViolated = true;
                break;
            }
        }

        return capacityViolated;
    }
}

public class SimulationRunner {
    private static final Logger LOG = LoggerFactory.getLogger(SimulationRunner.class);
    private static final String NUM_STEPS_OPTION = "steps";
    private static final String COMPONENT_SIZE_OPTION = "componentSize";
    private static final String COMPONENTS_PER_OBJECT_OPTION = "componentsPerObject";
    private static final String DYNAMIC_PLACEMENT_OPTION = "dp";
    private static final String SIZE_ZIPF_EXP_OPTION = "sizeZipfExp";
    private static final String OBJECT_ZIPF_EXP_OPTION = "objectZipfExp";
    private static final String TRIGGER_THRESHOLD_OPTION = "triggerThreshold";
    private static final String NUM_REPLICAS_OPTION = "replicas";

    private static void setupDb(final DSLContext conn) {
        final InputStream resourceAsStream = SimulationRunner.class.getResourceAsStream("/old_tables.sql");
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

    private static void initDB(final DSLContext conn) {
        setupDb(conn);

        // Add disks. Everything is in MB units.
        conn.execute("insert into disks values(1, 1000000, 100, 50)");
        conn.execute("insert into disks values(2, 1000000, 100, 75)");
        conn.execute("insert into disks values(3, 1000000, 200, 100)");
        conn.execute("insert into disks values(4, 1000000, 200, 200)");
        conn.execute("insert into disks values(5, 1000000, 200, 200)");

        // Add dummy components
        conn.execute("insert into components values(1, 1, 1, 0, 0, 0.0, 'PLACED', 1, null)");
        conn.execute("insert into components values(2, 1, 2, 0, 0, 0.0, 'PLACED', 2, null)");
        conn.execute("insert into components values(3, 1, 3, 0, 0, 0.0, 'PLACED', 3, null)");
        conn.execute("insert into components values(4, 1, 4, 0, 0, 0.0, 'PLACED', 4, null)");
        conn.execute("insert into components values(5, 1, 5, 0, 0, 0.0, 'PLACED', 5, null)");
    }

    private static Model createModel(final DSLContext conn, boolean dynamicPlacement) {
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

        // Create a DCM model using the database connection and the above constraint
        if (dynamicPlacement)
            return Model.build(conn, List.of(placed_constraint, replica_constraint, spareDiskCapacity, priorityDisk));

        return Model.build(conn, List.of(placed_constraint, replica_constraint, spareDiskCapacity, priorityDisk, object_constraint));
    }

    private static void printStats(final DSLContext conn) {
        long totalCap = 0;
        long totalTotalUtil = 0;
        final String check_capacity =
                "select sum(components.current_util) as total_disk_util, disks.max_capacity, disks.id " +
                        "from components " +
                        "join disks " +
                        "  on disks.id = components.current_disk " +
                        "group by disks.id, disks.max_capacity";

        Result<? extends Record> diskCapacities = conn.fetch(check_capacity);

        for (Record r : diskCapacities) {
            final Long maxCap = (Long) r.get("MAX_CAPACITY");
            final Long totalUtil = ((BigDecimal) r.get("TOTAL_DISK_UTIL")).longValue();
            LOG.info("Disk utilization: {}", totalUtil * 1.0/ maxCap);
            totalCap += maxCap;
            totalTotalUtil += totalUtil;
        }

        LOG.info("Overall disk utilization: {}", totalTotalUtil * 1.0 / totalCap);
        LOG.info("Total capacity provisioned: {}", conn.fetch("select SUM(max_capacity) from components"));
        LOG.info("Total capacity utilized: {}", conn.fetch("select SUM(current_util) from components"));
    }

    public static void main(String[] args) {
        int i = 0;
        // These are the defaults for these parameters.
        // They should be overridden by commandline arguments.
        int numSteps = 10;
        long componentSize = 3;
        int componentsPerObject = 10;
        boolean dynamicPlacement = false;
        double sizeZipfExp = 0.5;
        double objectZipfExp = 1;
        double triggerThreshold = 0.7;
        int numReplicas = 1;

        // create Options object
        Options options = new Options();

        Option numStepsOption = Option.builder(NUM_STEPS_OPTION)
                .hasArg(true)
                .desc("maximum number of steps to run the simulations.\nDefault: 10")
                .type(Integer.class)
                .build();
        Option componentSizeOption = Option.builder(COMPONENT_SIZE_OPTION)
                .hasArg()
                .desc("size of a component in MB.\nDefault: 3000")
                .build();
        Option componentsPerObjectOption = Option.builder(COMPONENTS_PER_OBJECT_OPTION)
                .hasArg()
                .desc("components per object. Ex: if component size is 3GB, and object is 255GB, then 85.\nDefault: 10")
                .build();
        Option dynamicPlacementOption = Option.builder(DYNAMIC_PLACEMENT_OPTION)
                .desc("use dynamic placement (if false, use traditional CLOM).\nDefault: false")
                .build();
        Option sizeZipfExpOption = Option.builder(SIZE_ZIPF_EXP_OPTION)
                .hasArg()
                .desc("Zipf exponent for distribution of random capacity added to each component.\nDefault: 0.5")
                .build();
        Option objectZipfExpOption = Option.builder(OBJECT_ZIPF_EXP_OPTION)
                .hasArg()
                .desc("Zipf exponent for distribution of random number of objects added each step.\nDefault: 1")
                .build();
        Option triggerThresholdOption = Option.builder(TRIGGER_THRESHOLD_OPTION)
                .hasArg()
                .desc("Disk fullness, as a fraction, that will trigger rebalancing.\nDefault: 0.7")
                .build();
        Option numReplicasOption = Option.builder(NUM_REPLICAS_OPTION)
                .hasArg()
                .desc("Number of replicas of each component.\nDefault: 1")
                .build();


        options.addOption("h", false, "print help message");
        options.addOption(numStepsOption);
        options.addOption(componentSizeOption);
        options.addOption(componentsPerObjectOption);
        options.addOption(dynamicPlacementOption);
        options.addOption(sizeZipfExpOption);
        options.addOption(objectZipfExpOption);
        options.addOption(triggerThresholdOption);
        options.addOption(numReplicasOption);

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse( options, args);
            if(cmd.hasOption("h")) {
                // automatically generate the help statement
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "java -cp target/vsan-dcm-1.0-SNAPSHOT.jar -Dlog4j.configurationFile=src/main/resources/log4j2.xml  com.vmware.SimulationRunner [options]", options);
                return;
            }
            if (cmd.hasOption(NUM_STEPS_OPTION)) {
                numSteps = Integer.parseInt(cmd.getOptionValue(NUM_STEPS_OPTION));
            }
            if (cmd.hasOption(COMPONENT_SIZE_OPTION)) {
                componentSize = Integer.parseInt(cmd.getOptionValue(COMPONENT_SIZE_OPTION));
            }
            if (cmd.hasOption(COMPONENTS_PER_OBJECT_OPTION)) {
                componentsPerObject = Integer.parseInt(cmd.getOptionValue(COMPONENTS_PER_OBJECT_OPTION));
            }
            if (cmd.hasOption(DYNAMIC_PLACEMENT_OPTION)) {
                dynamicPlacement = true;
            }
            if (cmd.hasOption(SIZE_ZIPF_EXP_OPTION)) {
                sizeZipfExp = Double.parseDouble(cmd.getOptionValue(SIZE_ZIPF_EXP_OPTION));
            }
            if (cmd.hasOption(OBJECT_ZIPF_EXP_OPTION)) {
                objectZipfExp = Double.parseDouble(cmd.getOptionValue(OBJECT_ZIPF_EXP_OPTION));
            }
            if (cmd.hasOption(TRIGGER_THRESHOLD_OPTION)) {
                triggerThreshold = Double.parseDouble(cmd.getOptionValue(TRIGGER_THRESHOLD_OPTION));
            }
            if (cmd.hasOption(NUM_REPLICAS_OPTION)) {
                numReplicas = Integer.parseInt(cmd.getOptionValue(NUM_REPLICAS_OPTION));
            }
        } catch (ParseException e) {
            LOG.error("Oops, couldn't parse command line");
            return;
        }

        // Create an in-memory database and get a JOOQ connection to it
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        initDB(conn);

        LOG.debug("Creating a simulation with parameters:\n dP : {}, componentsPerObject: {}, componentSize: {} " +
                "sizeZipfExp: {}, objectZipfExp: {}, triggerThreshold: {}, replicas: {}", dynamicPlacement, componentsPerObject, componentSize, sizeZipfExp, objectZipfExp, triggerThreshold, numReplicas);
        Simulation sim = new Simulation(createModel(conn, dynamicPlacement), conn, dynamicPlacement, componentsPerObject, componentSize, sizeZipfExp, objectZipfExp, triggerThreshold, numReplicas);

        for (; i < numSteps; i++) {
            LOG.info("Simulation step: {}", i);
            /*
             * For each step of the simulation, we
             * 1) determine if we want to allocate a new object(s)  and all its components
             * 2) simulate work on the cluster by adding capacity to existing components
             * 3) check for capacity violations
             */
            sim.addComponentsAndUpdateModel();
            /*
             * Only update the model if we are simulating traditional CLOM.
             * For DP, the model will be updated at the end of simulateWork, if necessary.
             * We make this optimization because running the model is expensive.
             */
            if (!dynamicPlacement) {
                if (!sim.runModelAndUpdateDB()) {
                    LOG.error("model failed?");
                    break;
                }
            }
            if (!sim.simulateWork()) {
                break;
            }
            if (sim.checkForCapacityViolation())
                break;

            LOG.debug("{}", conn.fetch("select * from components"));
            printStats(conn);
        }

        // Print final stats
        printStats(conn);
        LOG.info("Simulation survived {} steps\n", i + 1);
    }
}
