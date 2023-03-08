/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler;

import com.vmware.bespin.scheduler.generated.tables.Applications;
import com.vmware.bespin.scheduler.generated.tables.Nodes;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.Placed;
import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DCMRunner {
    public static final Nodes NODE_TABLE = Nodes.NODES;
    public static final Applications APP_TABLE = Applications.APPLICATIONS;
    public static final Placed PLACED_TABLE = Placed.PLACED;
    public static final Pending PENDING_TABLE = Pending.PENDING;

    protected Logger LOG = LogManager.getLogger(DCMRunner.class);
    protected final DSLContext conn;
    protected int numNodes = 0;
    protected int coresPerNode = 0;
    protected int memslicesPerNode = 0;
    protected int numApps = 0;
    protected final RandomDataGenerator rand;
    protected final Model model;

    /**
     * DCM is a wrapper object around a database connection and model for modelling
     * a cluster with
     * cores and memslices
     * 
     * @param conn                the connection to the database to use
     * @param numNodes            the number of nodes in the cluster
     * @param coresPerNode        the number of cores (hwthreads) per node
     * @param memslicesPerNode    the number of memslices per node
     * @param numApps             the number of applications running in the cluster
     * @param randomSeed          a seed to use for generating random numbers
     * @param useCapFunc          if true, use capacity function in the DCM model.
     *                            if false, use handwritten capacity
     *                            and load balancing constraints
     * @param usePrintDiagnostics set DCM to output print diagnostics
     */
    public DCMRunner(final DSLContext conn, final int numNodes, final int coresPerNode,
            final int memslicesPerNode, final int numApps, final Integer randomSeed,
            final boolean useCapFunc, final boolean usePrintDiagnostics) {

        // Argument validation
        assert numNodes >= 0;
        assert coresPerNode >= 0;
        assert memslicesPerNode >= 0;
        assert numApps >= 0;
        if (numNodes > 0) {
            assert coresPerNode > 0;
            assert memslicesPerNode > 0;
        } else {
            assert coresPerNode == 0;
            assert memslicesPerNode == 0;
        }

        // Set cores and memslice per node
        this.coresPerNode = coresPerNode;
        this.memslicesPerNode = memslicesPerNode;

        // Initialize internal state
        this.conn = conn;
        if (randomSeed == null) {
            this.rand = new RandomDataGenerator();
        } else {
            this.rand = new RandomDataGenerator(new JDKRandomGenerator(randomSeed));
        }

        // Setup the DB tables
        setupDb();

        // Add nodes with specified cores & memslices
        for (int i = 1; i <= numNodes; i++) {
            addNode(i, coresPerNode, memslicesPerNode);
        }

        // Add initial applications
        for (int i = 1; i <= numApps; i++) {
            addApplication(i);
        }

        // Initialize models
        this.model = createModel(useCapFunc, usePrintDiagnostics);
    }

    /**
     * Initialized the database connection by creating tables and populating nodes
     * and applications
     */
    private void setupDb() {
        final InputStream resourceAsStream = DCMRunner.class.getResourceAsStream("/bespin_tables.sql");
        try {
            assert resourceAsStream != null;
            try (final BufferedReader tables = new BufferedReader(new InputStreamReader(resourceAsStream,
                    StandardCharsets.UTF_8))) {
                // Create a fresh database
                final String schemaAsString = tables.lines()
                        .filter(line -> !line.startsWith("--")) // remove SQL comments
                        .collect(Collectors.joining("\n"));
                final String[] createStatements = schemaAsString.split(";");
                for (final String createStatement : createStatements) {
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

    /**
     * Create the DCM model by defining the constrants used and model configuration
     * 
     * @param useCapFunc          if true, use capacity function in the DCM model.
     *                            if false, use handwritten capacity
     *                            and load balancing constraints
     * @param usePrintDiagnostics set DCM to output print diagnostics
     * @return model the model
     */
    private Model createModel(final boolean useCapFunc, final boolean usePrintDiagnostics) {
        final List<String> constraints = new ArrayList<>();
        constraints.add(Constraints.getPlacedConstraint().sql());

        if (useCapFunc) {
            // this will replace two above, and balance constraint below
            constraints.add(Constraints.getCapacityFunctionCoreConstraint().sql());
            constraints.add(Constraints.getCapacityFunctionMemsliceConstraint().sql());
        } else {
            constraints.add(Constraints.getSpareView().sql());
            constraints.add(Constraints.getCapacityConstraint().sql());

            // TODO: should scale because we're maximizing the sum cores_per_node ratio
            // compared to memslices_per_node
            constraints.add(Constraints.getLoadBalanceCoreConstraint().sql());
            constraints.add(Constraints.getLoadBalanceMemsliceConstraint().sql());
        }
        constraints.add(Constraints.getAppLocalityPlacedConstraint().sql());
        constraints.add(Constraints.getAppLocalityPendingConstraint().sql());

        final OrToolsSolver.Builder builder = new OrToolsSolver.Builder()
                .setPrintDiagnostics(usePrintDiagnostics)
                .setUseCapacityPresenceLiterals(false)
                .setMaxTimeInSeconds(300);
        return Model.build(conn, builder.build(), constraints);
    }

    /**
     * Add a node to the cluster state
     * 
     * @param id        the node id
     * @param cores     the core id
     * @param memslices the memslice id
     */
    protected void addNode(final int id, final int cores, final int memslices) {
        if (numNodes == 0) {
            coresPerNode = cores;
            memslicesPerNode = memslices;
        } else {
            assert cores == coresPerNode;
            assert memslices == memslicesPerNode;
        }

        numNodes += 1;
        conn.insertInto(NODE_TABLE)
                .set(NODE_TABLE.ID, id)
                .set(NODE_TABLE.CORES, cores)
                .set(NODE_TABLE.MEMSLICES, memslices)
                .execute();
    }

    /**
     * Add an application to the cluster state
     * 
     * @param id the application id
     */
    protected void addApplication(final int id) {
        numApps += 1;
        conn.insertInto(APP_TABLE)
                .set(APP_TABLE.ID, id)
                .execute();
    }

    /**
     * The number of nodes based on DCMRunner configuration
     * 
     * @return numNodes the number of nodes
     */
    protected int numNodes() {
        return numNodes;
    }

    /**
     * The number of nodes based on database state
     * 
     * @return numNodes the number of nodes
     */
    protected int actualNumNodes() {
        final String appCount = "select count(id) from nodes";
        try {
            return ((Long) this.conn.fetch(appCount).get(0).getValue(0)).intValue();
        } catch (final NullPointerException e) {
            // If there are no nodes
            return 0;
        }
    }

    /**
     * The number of applications based on DCMRunner configuration
     * 
     * @return numApps the number of applications
     */
    protected int numApps() {
        return numApps;
    }

    /**
     * The number of applications based on database state
     * 
     * @return numApps the number of applicaitons
     */
    protected int actualNumApps() {
        final String appCount = "select count(id) from applications";
        try {
            return ((Long) this.conn.fetch(appCount).get(0).getValue(0)).intValue();
        } catch (final NullPointerException e) {
            // If there are no nodes
            return 0;
        }
    }

    /**
     * The number of cores allocated for a particular application
     * 
     * @param application the application to check for core usage
     * @return usedCores the number of cores currently allocated for an application
     */
    protected int usedCoresForApplication(final int application) {
        final String sql = String.format("select sum(placed.cores) from placed where application = %d", 
                application);
        int coresUsed = 0;
        final Result<Record> coreResults = conn.fetch(sql);
        if (null != coreResults && coreResults.isNotEmpty()) {
            if (null != coreResults.get(0).getValue(0)) {
                coresUsed = ((Long) coreResults.get(0).getValue(0)).intValue();
            }
        }
        return coresUsed;
    }

    /**
     * The number of memslices allocated for a particular application
     * 
     * @param application the application to check for memslice usage
     * @return usedMemslices the number of memslices currently allocated for an application
     */
    protected int usedMemslicesForApplication(final int application) {
        final String sql = String.format("select sum(placed.memslices) from placed where application = %d", 
                application);
        int memslicesUsed = 0;
        final Result<Record> memsliceResults = conn.fetch(sql);
        if (null != memsliceResults && memsliceResults.isNotEmpty()) {
            if (null != memsliceResults.get(0).getValue(0)) {
                memslicesUsed = ((Long) memsliceResults.get(0).getValue(0)).intValue();
            }
        }
        return memslicesUsed;
    }

    /**
     * The number of nodes in application is running on
     * 
     * @param application the application to check for node usage
     * @return nodesUsed the number of nodes an application is running on
     */
    protected int nodesForApplication(final int application) {
        final String sql = String.format("select count(placed.node) from placed where application = %d",
                application);
        int nodesUsed = 0;
        final Result<Record> nodeResults = conn.fetch(sql);
        if (null != nodeResults && nodeResults.isNotEmpty()) {
            if (null != nodeResults.get(0).getValue(0)) {
                nodesUsed = ((Long) nodeResults.get(0).getValue(0)).intValue();
            }
        }
        return nodesUsed;
    }

    /**
     * The number of cores in use
     * 
     * @return usedCores the aggregate number of cores currently allocated
     */
    protected int usedCores() {
        final String allocatedCoresSQL = "select sum(cores) from placed";
        Long usedCores = 0L;
        final Result<Record> coreRequest = conn.fetch(allocatedCoresSQL);
        if (null != coreRequest && coreRequest.isNotEmpty()) {
            try {
                usedCores += (Long) coreRequest.get(0).getValue(0);
            } catch (final NullPointerException e) {
            }
        }
        return usedCores.intValue();
    }

    /**
     * The number of cores in use on a particular node
     * 
     * @param node the node to check for core usage
     * @return usedCores the number of cores currently allocated on a node
     */
    protected int usedCoresForNode(final int node) {
        // Check if there are any cores available
        final String sql = String.format("select sum(placed.cores) from placed where node = %d", node);
        int coresUsed = 0;
        final Result<Record> coreResults = conn.fetch(sql);
        if (null != coreResults && coreResults.isNotEmpty()) {
            if (null != coreResults.get(0).getValue(0)) {
                coresUsed = ((Long) coreResults.get(0).getValue(0)).intValue();
            }
        }
        return coresUsed;
    }

    /**
     * The total number of cores in the cluster based on DCMRunner configuration
     * 
     * @return numCores the number of cores
     */
    protected int coreCapacity() {
        return coresPerNode * numNodes;
    }

    /**
     * The total number of cores in the cluster based on database state
     * 
     * @return numCores the number of cores
     */
    protected int actualCoreCapacity() {
        final String totalCores = "select sum(cores) from nodes";
        try {
            return ((Long) this.conn.fetch(totalCores).get(0).getValue(0)).intValue();
        } catch (final NullPointerException e) {
            // If there are no nodes
            return 0;
        }
    }

    /**
     * The total number of cores a node contains
     * 
     * @return numCores the number of cores
     */
    protected int actualCoreCapacityForNode(final int node) {
        final String coreCapacitySql = String.format("select cores from nodes where id = %d", node);
        try {
            return (int) this.conn.fetch(coreCapacitySql).get(0).getValue(0);
        } catch (final NullPointerException e) {
            // If there are no nodes
            return 0;
        }
    }

    /**
     * The number of memslices in use
     * 
     * @return usedMemslices the aggregate number of memslices currently allocated
     */
    protected int usedMemslices() {
        final String allocatedMemslicesSQL = "select sum(memslices) from placed";
        Long usedMemslices = 0L;
        final Result<Record> memsliceRequest = conn.fetch(allocatedMemslicesSQL);
        if (null != memsliceRequest && memsliceRequest.isNotEmpty()) {
            try {
                usedMemslices += (Long) memsliceRequest.get(0).getValue(0);
            } catch (final NullPointerException e) {
            }
        }
        return usedMemslices.intValue();
    }

    /**
     * The number of memslices in use on a particular node
     * 
     * @param node the node to check for memslice usage
     * @return usedMemslices the number of memslices currently allocated on a node
     */
    protected int usedMemslicesForNode(final int node) {
        // Check if there are any cores available
        final String sql = String.format("select sum(placed.memslices) from placed where node = %d", node);
        int memslicesUsed = 0;
        final Result<Record> memsliceResults = conn.fetch(sql);
        if (null != memsliceResults && memsliceResults.isNotEmpty()) {
            if (null != memsliceResults.get(0).getValue(0)) {
                memslicesUsed = ((Long) memsliceResults.get(0).getValue(0)).intValue();
            }
        }
        return memslicesUsed;
    }

    /**
     * The total number of memslices in the cluster based on DCMRunner configuration
     * 
     * @return numMemslices the number of memslices
     */
    protected int memsliceCapacity() {
        return memslicesPerNode * numNodes;
    }

    /**
     * The total number of memslices in the cluster based on database state
     * 
     * @return numMemslices the number of memslices
     */
    protected int actualMemsliceCapacity() {
        final String totalMemslices = "select sum(memslices) from nodes";
        try {
            return ((Long) this.conn.fetch(totalMemslices).get(0).getValue(0)).intValue();
        } catch (final NullPointerException e) {
            // If no nodes
            return 0;
        }
    }

    /**
     * The total number of memslices a node contains
     * 
     * @return numMemslices the number of memslices
     */
    protected int actualMemsliceCapacityForNode(final int node) {
        final String memsliceCapacitySql = String.format("select memslices from nodes where id = %d", node);
        try {
            return (int) this.conn.fetch(memsliceCapacitySql).get(0).getValue(0);
        } catch (final NullPointerException e) {
            // If there are no nodes
            return 0;
        }
    }

    /**
     * Select an application at random
     * 
     * @return application the application selected
     */
    protected int chooseRandomApplication() {
        final String applicationIds = "select id from applications";
        final Result<Record> results = conn.fetch(applicationIds);
        return (int) results.get(rand.nextInt(0, results.size() - 1)).getValue(0);
    }

    /**
     * Print current cluster information
     */
    public void printStats() {

        // print resource usage statistics by node
        System.out.println("Unallocated resources per node:");
        System.out.println(conn.fetch("select * from unallocated"));

        // print application statistics
        System.out.println("Application resources grouped by node: ");;
        for (int i = 1; i <= numApps(); i++) {
            LOG.info("FRAGMENTATION_PROCESS: app={}, num_nodes={}", i, nodesForApplication(i));
        }
    }

    /**
     * Determine the number of cores that must be allocated to reach a target
     * cluster utilization
     * 
     * @param clusterUtil the target cluster utilization percentage
     * @return numCores the number of cores that must be allocated to read the
     *         cluster utilization
     */
    protected int coresForUtil(final int clusterUtil) {
        // Determine the number of cores to alloc in order to meet utilization
        return (int) Math.ceil(((float) (numNodes * coresPerNode * clusterUtil)) / 100.0);
    }

    /**
     * Determine the number of memslices that must be allocated to reach a target
     * cluster utilization
     * 
     * @param clusterUtil the target cluster utilization percentage
     * @return numMemslices the number of memslices that must be allocated to read
     *         the cluster utilization
     */
    protected int memslicesForUtil(final int clusterUtil) {
        // Determine the number of memslices to alloc in order to meet utilization
        return (int) Math.ceil(((float) (numNodes * memslicesPerNode * clusterUtil)) / 100.0);
    }

    /**
     * Randomly determine the number of cores and memslices to allocate for each
     * application.
     * TODO: rewrite using Poisson generation.
     * 
     * @param coreAllocs     the aggregate number of core allocations to assign
     * @param memsliceAllocs the aggregate number of memslice allocations to assign
     * @return allocMap a map containing a number of cores and memslices for each
     *         application
     */
    protected HashMap<Integer, List<Integer>> generateAllocMap(final int coreAllocs,
            final int memsliceAllocs) {
        // Format of key=application number, values=[num_cores, num_memslices]
        final HashMap<Integer, List<Integer>> appAllocMap = new HashMap<>();

        // Assign cores to applications
        for (int i = 0; i < coreAllocs; i++) {
            final int application = rand.nextInt(1, numApps);
            final List<Integer> key = appAllocMap.getOrDefault(application, List.of(0, 0));
            appAllocMap.put(application, List.of(key.get(0) + 1, key.get(1)));
        }

        // Assign memslices to applications
        for (int i = 0; i < memsliceAllocs; i++) {
            final int application = rand.nextInt(1, numApps);
            final List<Integer> key = appAllocMap.getOrDefault(application, List.of(0, 0));
            appAllocMap.put(application, List.of(key.get(0), key.get(1) + 1));
        }
        return appAllocMap;
    }

    /**
     * The number of requests in the pending table.
     * 
     * @return pendingRequests the number of pending requests
     */
    protected int getNumPendingRequests() {
        final String numRequests = "select count(1) from pending";
        return ((Long) this.conn.fetch(numRequests).get(0).getValue(0)).intValue();
    }

    /**
     * Generate a random request.
     * 
     * Determine if it's for a core or memslice in proportion to that resource.
     * Choose an application at uniform random.
     */
    public void generateRandomRequest() {
        final int totalResources = this.coreCapacity() + this.memsliceCapacity();

        // Select an application at random
        final int application = this.chooseRandomApplication();

        // Determine if core or memslice, randomly chosen but weighted by capacity
        int cores = 0;
        int memslices = 0;
        if (rand.nextInt(1, totalResources) < this.coreCapacity()) {
            cores = 1;
        } else {
            memslices = 1;
        }
        generateRequest(cores, memslices, application);
    }

    /**
     * Submit a request for a resource to the pending table.
     * 
     * @param cores       the number of cores to request
     * @param memslices   the number of memslices to request
     * @param application the application that is requesting the resource(s)
     */
    protected void generateRequest(final int cores, final int memslices, final int application) {
        LOG.info("Created request for application {} ({} cores, {} memslices)", application, cores, memslices);

        // submit the request to the pending table
        conn.insertInto(PENDING_TABLE)
                .set(PENDING_TABLE.APPLICATION, application)
                .set(PENDING_TABLE.CORES, cores)
                .set(PENDING_TABLE.MEMSLICES, memslices)
                .set(PENDING_TABLE.STATUS, "PENDING")
                .set(PENDING_TABLE.CURRENT_NODE, -1)
                .set(PENDING_TABLE.CONTROLLABLE__NODE, (Field<Integer>) null)
                .execute();
    }

    /**
     * Record resources as allocated in the database.
     * 
     * @param node        the node that owns the resources
     * @param application the application the resources are allocated to
     * @param cores       the number of cores allocated
     * @param memslices   the number of memslices allocated
     */
    protected void updateAllocation(final int node, final int application, final int cores, final int memslices) {
        if (memslices == 0 && cores == 0) {
            LOG.warn("Cannot update allocation, nothing to do");
        } else if (memslices == 0) {
            conn.insertInto(PLACED_TABLE,
                    PLACED_TABLE.APPLICATION, PLACED_TABLE.NODE, PLACED_TABLE.CORES,
                    PLACED_TABLE.MEMSLICES)
                    .values(application, node, cores, memslices)
                    .onDuplicateKeyUpdate()
                    .set(PLACED_TABLE.CORES, PLACED_TABLE.CORES.plus(cores))
                    .execute();
        } else if (cores == 0) {
            conn.insertInto(PLACED_TABLE,
                    PLACED_TABLE.APPLICATION, PLACED_TABLE.NODE, PLACED_TABLE.CORES,
                    PLACED_TABLE.MEMSLICES)
                    .values(application, node, cores, memslices)
                    .onDuplicateKeyUpdate()
                    .set(PLACED_TABLE.MEMSLICES, PLACED_TABLE.MEMSLICES.plus(memslices))
                    .execute();
        } else {
            conn.insertInto(PLACED_TABLE,
                    PLACED_TABLE.APPLICATION, PLACED_TABLE.NODE, PLACED_TABLE.CORES,
                    PLACED_TABLE.MEMSLICES)
                    .values(application, node, cores, memslices)
                    .onDuplicateKeyUpdate()
                    .set(PLACED_TABLE.CORES, PLACED_TABLE.CORES.plus(cores))
                    .set(PLACED_TABLE.MEMSLICES, PLACED_TABLE.MEMSLICES.plus(memslices))
                    .execute();
        }
    }

    /**
     * Run the model on pending requests and update the DB based on the assignments.
     * 
     * @param printTimingData
     * @return hasUpdates whether the model made any assignments
     * @throws Exception this should never happen, but overriding subclasses may
     *                   throw errors.
     */
    public boolean runModelAndUpdateDB(final boolean printTimingData) throws Exception {
        final Result<? extends Record> results;
        final long start = System.currentTimeMillis();
        final long solveFinish;
        try {
            results = model.solve("PENDING");
            solveFinish = System.currentTimeMillis();
        } catch (ModelException | SolverException e) {
            LOG.error(e);
            return false;
        }

        // Solver did no work
        if (results.isEmpty()) {
            return false;
        }

        // TODO: should find a way to batch these
        // Add new assignments to placed, remove new assignments from pending
        results.forEach(r -> {
            updateAllocation((Integer) r.get("CONTROLLABLE__NODE"), (Integer) r.get("APPLICATION"),
                    (Integer) r.get("CORES"), (Integer) r.get("MEMSLICES"));
            conn.deleteFrom(PENDING_TABLE)
                    .where(PENDING_TABLE.ID.eq((Long) r.get("ID")))
                    .execute();
        });
        final long updateFinish = System.currentTimeMillis();
        if (printTimingData) {
            LOG.info("SOLVE_RESULTS: solve={}ms, solve_update={}ms", solveFinish - start, updateFinish - start);
        }
        return true;
    }

    /**
     * Double check DCM/model to ensure resources aren't being overprovisioned
     * 
     * @return hasViolation true if capacity is violated, false if capacity is
     *         respected
     */
    public boolean checkForCapacityViolation() {
        // Check total capacity
        // TODO: could make this more efficient by using unallocated view
        final boolean totalCoreCheck = actualCoreCapacity() >= usedCores();
        final boolean totalMemsliceCheck = actualMemsliceCapacity() >= usedCores();

        boolean nodeCoreCheck = true;
        boolean nodeMemsliceCheck = true;

        for (int node = 1; node <= numNodes; node++) {
            nodeCoreCheck = nodeCoreCheck && (actualCoreCapacityForNode(node) >= usedCoresForNode(node));
            nodeMemsliceCheck = nodeMemsliceCheck &&
                    (actualMemsliceCapacityForNode(node) >= usedMemslicesForNode(node));
        }

        return !(totalCoreCheck && totalMemsliceCheck && nodeCoreCheck && nodeMemsliceCheck);
    }

    /**
     * Check if the database contains the correct number of core allocs and memslice
     * alloc, as well as check for capacity violations. There should also be no pending
     * allocations after running a fill algorithm
     * 
     * @param coreAllocs     the exepcted number of core allocations
     * @param memsliceAllocs the expected number of memslice allocations
     */
    protected void checkFill(final int coreAllocs, final int memsliceAllocs) {
        assert (usedCores() == coreAllocs);
        assert (usedMemslices() == memsliceAllocs);
        assert (!checkForCapacityViolation());
        assert (getNumPendingRequests() == 0);
    }

    /**
     * Fill the cluster to a certain utilization percentage by generating random
     * requests as uniform random.
     * 
     * @param clusterUtil target cluster utilization
     */
    public void fillRandom(final int clusterUtil) {
        assert usedCores() == 0;
        assert usedMemslices() == 0;

        // Determine the number of both resources to allocate based on clusterUtil
        final int coreAllocs = coresForUtil(clusterUtil);
        final int memsliceAllocs = memslicesForUtil(clusterUtil);

        // Format of key=application number, values=[num_cores, num_memslices]
        final HashMap<Integer, List<Integer>> appAllocMap = generateAllocMap(coreAllocs, memsliceAllocs);

        // Randomly assign application allocs to nodes
        for (final Map.Entry<Integer, List<Integer>> entry : appAllocMap.entrySet()) {
            final int application = entry.getKey();
            final int cores = entry.getValue().get(0);
            final int memslices = entry.getValue().get(1);

            // Assign cores
            for (int i = 0; i < cores; i++) {
                boolean done = false;
                while (!done) {
                    // Choose a random node
                    final int node = rand.nextInt(1, numNodes);

                    // If there is a core available, allocate it.
                    if (actualCoreCapacityForNode(node) - usedCoresForNode(node) > 0) {
                        updateAllocation(node, application, 1, 0);
                        done = true;
                    }
                }
            }

            // Assign memslices
            for (int i = 0; i < memslices; i++) {
                boolean done = false;
                while (!done) {
                    // Choose a random node
                    final int node = rand.nextInt(1, numNodes);

                    // If there is a core available, allocate it.
                    if (actualMemsliceCapacityForNode(node) - usedMemslicesForNode(node) > 0) {
                        updateAllocation(node, application, 0, 1);
                        done = true;
                    }
                }
            }
        }
        checkFill(coreAllocs, memsliceAllocs);
    }

    /**
     * Fill the cluster to a certain utilization percentage by generating random
     * requests to
     * the pending table, and then running DCM
     * 
     * The requests are split by memslices and cores, and also so no one request
     * exceeds 1/2 node capacity.
     * 
     * @param clusterUtil target cluster utilization
     */
    public void fillSingleStep(final int clusterUtil) throws Exception {
        assert usedCores() == 0;
        assert usedMemslices() == 0;

        // Determine the number of both resources to allocate based on clusterUtil
        final int coreAllocs = coresForUtil(clusterUtil);
        final int memsliceAllocs = memslicesForUtil(clusterUtil);

        // Format of key=application number, values=[num_cores, num_memslices]
        final HashMap<Integer, List<Integer>> appAllocMap = generateAllocMap(coreAllocs, memsliceAllocs);

        // Assume this is SingleStep fill. Chunk as much as possible (up to 1/2 node
        // capabity)
        for (final Map.Entry<Integer, List<Integer>> entry : appAllocMap.entrySet()) {
            final int application = entry.getKey();
            final int cores = entry.getValue().get(0);
            final int memslices = entry.getValue().get(1);

            // Generate core requests
            int totalCoresToRequest = cores;
            while (totalCoresToRequest > 0) {
                final int coresInRequest;
                if (totalCoresToRequest > coresPerNode / 2) {
                    coresInRequest = coresPerNode / 2;
                } else {
                    coresInRequest = totalCoresToRequest;
                }
                generateRequest(coresInRequest, 0, application);
                totalCoresToRequest -= coresInRequest;
            }

            // Generate memslices requests
            int totalMemslicesToRequest = memslices;
            while (totalMemslicesToRequest > 0) {
                final int memslicesInRequest;
                if (totalMemslicesToRequest > memslicesPerNode / 2) {
                    memslicesInRequest = memslicesPerNode / 2;
                } else {
                    memslicesInRequest = totalMemslicesToRequest;
                }
                generateRequest(0, memslicesInRequest, application);
                totalMemslicesToRequest -= memslicesInRequest;
            }
        }

        // Use DCM to assign the resources
        runModelAndUpdateDB(false);
        checkFill(coreAllocs, memsliceAllocs);
    }

    /**
     * Fill the cluster to a certain utilization percentage by generating requests
     * based on a
     * Poisson distribution. There may be some error where the cluster utilization
     * isn't perfectly met.
     * 
     * @param clusterUtil  target cluster utilization
     * @param coreMean     the mean cluster-wide core requests per solve step
     * @param memsliceMean the mean cluster-wide memslice requests per solve step
     */
    public void fillPoisson(final int clusterUtil, final double coreMean, final double memsliceMean) throws Exception {
        assert usedCores() == 0;
        assert usedMemslices() == 0;
        
        // Determine the number of both resources to allocate based on clusterUtil
        final int targetCoreAllocs = coresForUtil(clusterUtil);
        final int targetMemsliceAllocs = memslicesForUtil(clusterUtil);

        final double perAppCoreMean = coreMean / numApps;
        final double perAppMemsliceMean = memsliceMean / numApps;

        int allocatedCores = 0;
        int allocatedMemslices = 0;
        int numSolves = 0;
        while (allocatedCores < targetCoreAllocs || allocatedMemslices < targetMemsliceAllocs) {
            // Generate requests for cores and memslices.
            // TODO: not sure if should give all applications a chance or okay to just give
            // some?
            for (int i = 1; i <= numApps &&
                    (allocatedCores < targetCoreAllocs || allocatedMemslices < targetMemsliceAllocs); i++) {

                if (allocatedCores < targetCoreAllocs) {
                    final int coresToAlloc = (int) rand.nextPoisson(perAppCoreMean);
                    for (int j = 0; j < coresToAlloc; j++) {
                        generateRequest(1, 0, i);
                    }
                    allocatedCores += coresToAlloc;
                }

                if (allocatedMemslices < targetMemsliceAllocs) {
                    final int memslicesToAlloc = (int) rand.nextPoisson(perAppMemsliceMean);
                    for (int j = 0; j < memslicesToAlloc; j++) {
                        generateRequest(0, 1, i);
                    }
                    allocatedMemslices += memslicesToAlloc;
                }
            }

            // Now solve all requests from this round
            runModelAndUpdateDB(false);
            numSolves += 1;
        }

        // Check that we got what we wanted
        checkFill(allocatedCores, allocatedMemslices);

        LOG.info("Poisson fill completed in {} steps (coreMean={}, memsliceMean={})", 
                numSolves, coreMean, memsliceMean);

        if (targetCoreAllocs != allocatedCores) {
            LOG.warn("Poisson fill did not exactly meet target cluster util." +
                    " Core error is {} cores ({}% of total cores)",
                    targetCoreAllocs - allocatedCores,
                    (float) Math.abs(targetCoreAllocs - allocatedCores) / (float) coreCapacity());
        }
        if (targetMemsliceAllocs != allocatedMemslices) {
            LOG.warn("Poisson fill did not exactly meet target cluster util. " +
                    "Memslice error is {} memslices ({}% of total memslices)",
                    targetMemsliceAllocs - allocatedMemslices,
                    (float) Math.abs(targetMemsliceAllocs - allocatedMemslices) / (float) memsliceCapacity());
        }
    }
}
