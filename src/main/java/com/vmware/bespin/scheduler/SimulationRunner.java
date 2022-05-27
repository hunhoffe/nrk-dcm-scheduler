package com.vmware.bespin.scheduler;

import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;

import com.vmware.bespin.scheduler.generated.tables.Allocations;
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
import java.math.BigDecimal;
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

    static final Allocations allocTable = Allocations.ALLOCATIONS;
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
        System.out.printf("Core Turnover: %d, allocationsToMake: %d, maxAllocations: %d, minAllocation: %d\n",
                coreTurnover, coreAllocationsToMake, maxCoreAllocations, minCoreAllocations);

        int turnoverCounter = 0;
        while (turnoverCounter < coreAllocationsToMake) {
            int usedCores = this.usedCores();
            if (usedCores > minCoreAllocations) {
                if (usedCores >= maxCoreAllocations || rand.nextBoolean()) {
                    // delete core allocation, do not count as turnover
                    Result<Record> results = conn.select().from(allocTable).where(allocTable.CORES.greaterThan(0)).fetch();
                    Record rowToDelete = results.get(rand.nextInt(results.size()));
                    conn.update(allocTable)
                            .set(allocTable.CORES, allocTable.CORES.minus(1))
                            .where(and(
                                    allocTable.CURRENT_NODE.eq(rowToDelete.getValue(allocTable.CURRENT_NODE)),
                                    allocTable.APPLICATION.eq(rowToDelete.get(allocTable.APPLICATION))))
                            .execute();
                }
            }
            // Add a pending allocation, count as turnover
            if (usedCores < maxCoreAllocations) {
                conn.insertInto(allocTable)
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

        int totalMemslices = this.memsliceCapacity();
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
                    Result<Record> results = conn.select().from(allocTable).where(allocTable.MEMSLICES.greaterThan(0)).fetch();
                    Record rowToDelete = results.get(rand.nextInt(results.size()));
                    conn.update(allocTable)
                            .set(allocTable.MEMSLICES, allocTable.MEMSLICES.minus(1))
                            .where(and(
                                    allocTable.CURRENT_NODE.eq(rowToDelete.getValue(allocTable.CURRENT_NODE)),
                                    allocTable.APPLICATION.eq(rowToDelete.get(allocTable.APPLICATION))))
                            .execute();
                }
            }
            // Add an allocation, count as turnover
            if (usedMemslices < maxMemsliceAllocations) {
                conn.insertInto(allocTable)
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
    }

    public boolean runModelAndUpdateDB() {
        Result<? extends Record> results;
        try {
            results = model.solve("ALLOCATIONS");
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
                    updates.add(conn.update(allocTable)
                            .set(allocTable.CURRENT_NODE, (Integer) r.get("CONTROLLABLE__NODE"))
                            .set(allocTable.STATUS, "PLACED")
                            .where(and(allocTable.ID.eq((Integer) r.get("ID")), allocTable.STATUS.eq("PENDING")))
                    );
            }
        );
        conn.batch(updates).execute();

        // Merge rows when possible
        System.out.println("Before Merge:");
        System.out.println(conn.fetch("select * from allocations"));
        // h2db disallows merge-match-delete commands, so do this in two commands :(
        String updateMerged = "update allocations a, merged_resources m " +
                "set a.cores = m.cores, a.memslices = m.memslices " +
                "inner join merged_resources m " +
                "on a.id = m.id";
        conn.execute(updateMerged);
        String deleteMerged = "delete from allocations " +
                "where id not in (select m.id from merged_resources m)";
        conn.execute(deleteMerged);

        System.out.println("After Merge:");
        System.out.println(conn.fetch("select * from allocations"));
        System.exit(-1);

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

        conn.execute("create view merged_resources as " +
                "select max(id) as id, sum(cores) as cores, sum(memslices) as memslices, application, " +
                "    status, current_node from allocations " +
                "where status = 'PLACED' " +
                "group by application, current_node");
    }

    private static void initDB(final DSLContext conn, int numNodes, int coresPerNode, int memslicesPerNode) {
        final Nodes nodeTable = Nodes.NODES;
        final Applications appTable = Applications.APPLICATIONS;
        final Allocations allocTable = Allocations.ALLOCATIONS;

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

        conn.insertInto(allocTable)
                .set(allocTable.APPLICATION, 1)
                .set(allocTable.CORES, 3)
                .set(allocTable.MEMSLICES, 3)
                .set(allocTable.STATUS, "PLACED")
                .set(allocTable.CURRENT_NODE, 3)
                .set(allocTable.CONTROLLABLE__NODE, 3)
                .execute();
        conn.insertInto(allocTable)
                .set(allocTable.APPLICATION, 1)
                .set(allocTable.CORES, 3)
                .set(allocTable.MEMSLICES, 3)
                .set(allocTable.STATUS, "PLACED")
                .set(allocTable.CURRENT_NODE, 3)
                .set(allocTable.CONTROLLABLE__NODE, 3)
                .execute();
        conn.insertInto(allocTable)
                .set(allocTable.APPLICATION, 1)
                .set(allocTable.CORES, 3)
                .set(allocTable.MEMSLICES, 3)
                .set(allocTable.STATUS, "PLACED")
                .set(allocTable.CURRENT_NODE, 4)
                .set(allocTable.CONTROLLABLE__NODE, 3)
                .execute();

        System.out.println("Merged resources:");
        System.out.println(conn.fetch("select * from merged_resources"));
        System.out.println("Before Merge:");
        System.out.println(conn.fetch("select * from allocations"));
        // h2db disallows merge-match-delete commands, so do this in two commands :(
        String updateMerged = "update allocations a\n" +
                "set a.cores = (select m.cores from merged_resources m where m.id = a.id), " +
                "    a.memslices = (select m.memslices from merged_resources m where m.id = a.id)\n" +
                "where exists\n" +
                "(select * from merged_resources m where m.id = a.id)";
        conn.execute(updateMerged);
        String deleteMerged = "delete from allocations " +
                "where id not in (select m.id from merged_resources m)";
        conn.execute(deleteMerged);

        System.out.println("After Merge:");
        System.out.println(conn.fetch("select * from allocations"));
        System.exit(-1);


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
                String sql = String.format("select sum(allocations.cores) from allocations where current_node = %d", node);
                int coresUsed = 0;
                try {
                    coresUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
                } catch (NullPointerException e) {}
                int coresToAlloc = Math.min(coresPerNode - coresUsed, cores);

                sql = String.format("select sum(allocations.memslices) from allocations where current_node = %d", node);
                int memslicesUsed = 0;
                try {
                    memslicesUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
                } catch (NullPointerException e) {}
                int memslicesToAlloc = Math.min(memslicesPerNode - memslicesUsed, memslices);

                // If we can alloc anything, do so
                if (coresToAlloc > 0 || memslicesToAlloc > 0) {
                    conn.insertInto(allocTable)
                            .set(allocTable.APPLICATION, application)
                            .set(allocTable.CORES, coresToAlloc)
                            .set(allocTable.MEMSLICES, memslicesToAlloc)
                            .set(allocTable.STATUS, "PLACED")
                            .set(allocTable.CURRENT_NODE, node)
                            .set(allocTable.CONTROLLABLE__NODE, (Field<Integer>) null)
                            .execute();

                    // Mark resources as allocated
                    cores -= coresToAlloc;
                    memslices -= memslicesToAlloc;
                }
            }
        }

        System.out.println(conn.fetch("select * from allocations"));

        // Double check correctness
        String sql = "select sum(allocations.cores) from allocations";
        int totalCoresUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
        sql = "select sum(allocations.memslices) from allocations";
        int totalMemslicesUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
        sql = "create view nodeSums as select nodes.id, sum(allocations.cores) as cores, sum(allocations.memslices) as memslices " +
                "from allocations " +
                "join nodes " +
                "  on nodes.id = allocations.current_node " +
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

        // Create load balancing constraint across nodes for cores and memslices
        final String node_loadbalance_constraint = "create constraint node_loadbalance_constraint as " +
                "select * from spare_view " +
                "maximize min(cores + memslices)";

        // Minimize number of nodes per application (e.g., maximize locality)
        final String application_num_nodes_view = "create constraint application_num_nodes as " +
                "select applications.id, count(distinct pending.controllable__node) as num_nodes " +
                "from pending " +
                "join applications " +
                "  on applications.id = pending.application " +
                "group by applications.id";
        final String application_locality_constraint = "create constraint application_locality_constraint as " +
                "select * from application_num_nodes " +
                "maximize -1*sum(num_nodes)";

        OrToolsSolver.Builder b = new OrToolsSolver.Builder();
        b.setPrintDiagnostics(true);
        return Model.build(conn, b.build(), List.of(
                placed_constraint,
                spare_view,
                capacity_constraint,
                node_loadbalance_constraint
                //node_load_memslices,
                //application_num_nodes_view,
                //application_locality_constraint
        ));
    }

    private static void printStats(final DSLContext conn) {
        // TODO: check resources??
        //System.out.print("Spare resources per node:");
        //System.out.println(conn.fetch("select * from spare_view"));
        // TODO: print application statistics
        // TODO: print statistics on cluster usage
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