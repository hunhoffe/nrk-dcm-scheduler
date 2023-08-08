/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler;

import com.vmware.bespin.scheduler.generated.tables.Applications;
import com.vmware.bespin.scheduler.generated.tables.Nodes;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.dcm.SolverException;
import com.vmware.bespin.scheduler.generated.tables.Placed;
import com.vmware.bespin.scheduler.generated.tables.records.PendingRecord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;

import static org.jooq.impl.DSL.and;

import java.util.ArrayList;
import java.util.List;

public class Scheduler {
    public static final Nodes NODE_TABLE = Nodes.NODES;
    public static final Applications APP_TABLE = Applications.APPLICATIONS;
    public static final Placed PLACED_TABLE = Placed.PLACED;
    public static final Pending PENDING_TABLE = Pending.PENDING;

    protected Logger LOG = LogManager.getLogger(Scheduler.class);
    protected final DSLContext conn;
    protected final Solver solver;

    /**
     * Scheduler is a wrapper object around a database connection and solver for modelling
     * a cluster with
     * cores and memslices
     * 
     * @param conn                the connection to the database to use
     * @param solver              solver used to assign request to resources
     */
    public Scheduler(final DSLContext conn, final Solver solver) {

        // Initialize internal state
        this.conn = conn;
        this.solver = solver;
    }

    /**
     * Add a node to the cluster state
     * 
     * @param id        the node id
     * @param cores     the core id
     * @param memslices the memslice id
     */
    public void addNode(final long id, final long cores, final long memslices) {
        conn.insertInto(NODE_TABLE)
                .set(NODE_TABLE.ID, (int) id)
                .set(NODE_TABLE.CORES, (int) cores)
                .set(NODE_TABLE.MEMSLICES, (int) memslices)
                .execute();
    }

    /**
     * Changes the resources belonging to a node by adding or subtracting.
     * 
     * @param id        the node id
     * @param cores     the number of cores to add or subtract from the node
     * @param memslices the number of memslices to add or subtract from the node
     */
    public void updateNode(final long id, final long cores, final long memslices, final boolean isAdd) {
        if (isAdd) {
           conn.update(NODE_TABLE)
                .set(NODE_TABLE.MEMSLICES, NODE_TABLE.MEMSLICES.plus(memslices))
                .set(NODE_TABLE.CORES, NODE_TABLE.CORES.plus(cores))
                .where(NODE_TABLE.ID.eq((int) id))
                .execute();
        } else {
            conn.update(NODE_TABLE)
                .set(NODE_TABLE.MEMSLICES, NODE_TABLE.MEMSLICES.sub(memslices))
                .set(NODE_TABLE.CORES, NODE_TABLE.CORES.sub(cores))
                .where(NODE_TABLE.ID.eq((int) id))
                .execute();
        }
    }

    /**
     * Add an application to the cluster state
     * 
     * @param id the application id
     */
    public void addApplication(final long id) {
        conn.insertInto(APP_TABLE)
                .set(APP_TABLE.ID, (int) id)
                .onDuplicateKeyIgnore()
                .execute();
    }

    /**
     * The number of nodes based on database state
     * 
     * @return numNodes the number of nodes
     */
    public long numNodes() {
        final String appCount = "select count(id) from nodes";
        try {
            return (Long) this.conn.fetch(appCount).get(0).getValue(0);
        } catch (final NullPointerException e) {
            // If there are no nodes
            return 0;
        }
    }

    /**
     * The number of applications based on database state
     * 
     * @return numApps the number of applications
     */
    public long numApps() {
        final String appCount = "select count(id) from applications";
        try {
            return (Long) this.conn.fetch(appCount).get(0).getValue(0);
        } catch (final NullPointerException e) {
            // If there are no applications
            return 0;
        }
    }

    /**
     * The number of cores allocated for a particular application
     * 
     * @param application the application to check for core usage
     * @return usedCores the number of cores currently allocated for an application
     */
    public long usedCoresForApplication(final long application) {
        final String sql = String.format("select sum(placed.cores) from placed where application = %d", 
                application);
        long coresUsed = 0;
        final Result<Record> coreResults = conn.fetch(sql);
        if (null != coreResults && coreResults.isNotEmpty()) {
            if (null != coreResults.get(0).getValue(0)) {
                coresUsed = (Long) coreResults.get(0).getValue(0);
            }
        }
        return coresUsed;
    }

    /**
     * The number of cores allocated for a particular application on a particular node
     * 
     * @param application the application to check for core usage
     * @param node the node to check for core usage
     * @return usedCores the number of cores currently allocated for an application
     */
    public long usedCoresForApplicationOnNode(final long application, final long node) {
        final String sql = String.format("select sum(placed.cores) from placed where application = %d and node = %d", 
                application, node);
        long coresUsed = 0;
        final Result<Record> coreResults = conn.fetch(sql);
        if (null != coreResults && coreResults.isNotEmpty()) {
            if (null != coreResults.get(0).getValue(0)) {
                coresUsed = (Long) coreResults.get(0).getValue(0);
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
    public long usedMemslicesForApplication(final long application) {
        final String sql = String.format("select sum(placed.memslices) from placed where application = %d", 
                application);
        long memslicesUsed = 0;
        final Result<Record> memsliceResults = conn.fetch(sql);
        if (null != memsliceResults && memsliceResults.isNotEmpty()) {
            if (null != memsliceResults.get(0).getValue(0)) {
                memslicesUsed = (Long) memsliceResults.get(0).getValue(0);
            }
        }
        return memslicesUsed;
    }

    /**
     * The number of memslices allocated for a particular application on a particular node
     * 
     * @param application the application to check for memslices usage
     * @param node the node to check for memslice usage
     * @return usedMemslices the number of memslices currently allocated for an application
     */
    public long usedMemslicesForApplicationOnNode(final long application, final long node) {
        final String sql = String.format(
                "select sum(placed.memslices) from placed where application = %d and node = %d", 
                application, node);
        long memslicesUsed = 0;
        final Result<Record> memsliceResults = conn.fetch(sql);
        if (null != memsliceResults && memsliceResults.isNotEmpty()) {
            if (null != memsliceResults.get(0).getValue(0)) {
                memslicesUsed = (Long) memsliceResults.get(0).getValue(0);
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
    public long nodesForApplication(final long application) {
        final String sql = String.format("select count(placed.node) from placed where application = %d",
                application);
        long nodesUsed = 0;
        final Result<Record> nodeResults = conn.fetch(sql);
        if (null != nodeResults && nodeResults.isNotEmpty()) {
            if (null != nodeResults.get(0).getValue(0)) {
                nodesUsed = (Long) nodeResults.get(0).getValue(0);
            }
        }
        return nodesUsed;
    }

    /**
     * The number of cores in use
     * 
     * @return usedCores the aggregate number of cores currently allocated
     */
    public long usedCores() {
        final String allocatedCoresSQL = "select sum(cores) from placed";
        long usedCores = 0L;
        final Result<Record> coreRequest = conn.fetch(allocatedCoresSQL);
        if (null != coreRequest && coreRequest.isNotEmpty()) {
            try {
                usedCores += (Long) coreRequest.get(0).getValue(0);
            } catch (final NullPointerException e) {
            }
        }
        return usedCores;
    }

    /**
     * The number of cores in use on a particular node
     * 
     * @param node the node to check for core usage
     * @return usedCores the number of cores currently allocated on a node
     */
    public long usedCoresForNode(final long node) {
        // Check if there are any cores available
        final String sql = String.format("select sum(placed.cores) from placed where node = %d", node);
        long coresUsed = 0L;
        final Result<Record> coreResults = conn.fetch(sql);
        if (null != coreResults && coreResults.isNotEmpty()) {
            if (null != coreResults.get(0).getValue(0)) {
                coresUsed = (Long) coreResults.get(0).getValue(0);
            }
        }
        return coresUsed;
    }

    /**
     * The total number of cores in the cluster based on database state
     * 
     * @return numCores the number of cores
     */
    public long coreCapacity() {
        final String totalCores = "select sum(cores) from nodes";
        try {
            return (Long) this.conn.fetch(totalCores).get(0).getValue(0);
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
    public long coreCapacityForNode(final long node) {
        final String coreCapacitySql = String.format("select cores from nodes where id = %d", node);
        try {
            return ((Integer) this.conn.fetch(coreCapacitySql).get(0).getValue(0)).longValue();
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
    public long usedMemslices() {
        final String allocatedMemslicesSQL = "select sum(memslices) from placed";
        Long usedMemslices = 0L;
        final Result<Record> memsliceRequest = conn.fetch(allocatedMemslicesSQL);
        if (null != memsliceRequest && memsliceRequest.isNotEmpty()) {
            try {
                usedMemslices += (Long) memsliceRequest.get(0).getValue(0);
            } catch (final NullPointerException e) {
            }
        }
        return usedMemslices;
    }

    /**
     * The number of memslices in use on a particular node
     * 
     * @param node the node to check for memslice usage
     * @return usedMemslices the number of memslices currently allocated on a node
     */
    public long usedMemslicesForNode(final long node) {
        // Check if there are any cores available
        final String sql = String.format("select sum(placed.memslices) from placed where node = %d", node);
        long memslicesUsed = 0L;
        final Result<Record> memsliceResults = conn.fetch(sql);
        if (null != memsliceResults && memsliceResults.isNotEmpty()) {
            if (null != memsliceResults.get(0).getValue(0)) {
                memslicesUsed = (Long) memsliceResults.get(0).getValue(0);
            }
        }
        return memslicesUsed;
    }

    /**
     * The total number of memslices in the cluster based on database state
     * 
     * @return numMemslices the number of memslices
     */
    public long memsliceCapacity() {
        final String totalMemslices = "select sum(memslices) from nodes";
        try {
            return (long) this.conn.fetch(totalMemslices).get(0).getValue(0);
        } catch (final NullPointerException e) {
            // If no nodes
            return 0L;
        }
    }

    /**
     * The total number of memslices a node contains
     * 
     * @return numMemslices the number of memslices
     */
    public long memsliceCapacityForNode(final long node) {
        final String memsliceCapacitySql = String.format("select memslices from nodes where id = %d", node);
        try {
            return ((Integer) this.conn.fetch(memsliceCapacitySql).get(0).getValue(0)).longValue();
        } catch (final NullPointerException e) {
            // If there are no nodes
            return 0L;
        }
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
            System.out.println(String.format("FRAGMENTATION_PROCESS: app=%d, num_nodes=%d", i, nodesForApplication(i)));
        }

        System.out.println("Placed Resources:");
        System.out.println(conn.fetch("select * from placed"));
    }

    /**
     * The number of requests in the pending table.
     * 
     * @return pendingRequests the number of pending requests
     */
    public long getNumPendingRequests() {
        final String numRequests = "select count(1) from pending";
        return (Long) this.conn.fetch(numRequests).get(0).getValue(0);
    }

    /**
     * The number of requests in the pending table.
     * 
     * @return pendingRequests the number of pending requests
     */
    protected long[] getPendingRequestIDs() {
        final String sql = "select id from pending";
        long[] pendingIds = {};
        final Result<Record> pendingIdResults = conn.fetch(sql);
        if (null != pendingIdResults && pendingIdResults.isNotEmpty()) {
            pendingIds = new long[pendingIdResults.size()];
            int counter = 0;
            for (final Record r: pendingIdResults) {
                pendingIds[counter] = (Long) r.getValue(0);
                counter++;
            }
        }
        return pendingIds;
    }

    /**
     * Submit a request for a resource to the pending table.
     * 
     * @param id          the id of the pending request to generate (null if dynamically generate)
     * @param cores       the number of cores to request
     * @param memslices   the number of memslices to request
     * @param application the application that is requesting the resource(s)
     */
    public void generateRequest(final Long id, final long cores, final long memslices, final long application) {
        LOG.info("Created request (id={}) for application {} ({} cores, {} memslices)", 
                id, application, cores, memslices);

        // submit the request to the pending table
        if (id != null) {
            conn.insertInto(PENDING_TABLE)
                .set(PENDING_TABLE.ID, (long) id)
                .set(PENDING_TABLE.APPLICATION, (int) application)
                .set(PENDING_TABLE.CORES, (int) cores)
                .set(PENDING_TABLE.MEMSLICES, (int) memslices)
                .set(PENDING_TABLE.STATUS, "PENDING")
                .set(PENDING_TABLE.CURRENT_NODE, -1)
                .set(PENDING_TABLE.CONTROLLABLE__NODE, (Field<Integer>) null)
                .execute();
        } else {
            conn.insertInto(PENDING_TABLE)
                .set(PENDING_TABLE.APPLICATION, (int) application)
                .set(PENDING_TABLE.CORES, (int) cores)
                .set(PENDING_TABLE.MEMSLICES, (int) memslices)
                .set(PENDING_TABLE.STATUS, "PENDING")
                .set(PENDING_TABLE.CURRENT_NODE, -1)
                .set(PENDING_TABLE.CONTROLLABLE__NODE, (Field<Integer>) null)
                .execute();
        }
    }

    /**
     * Submit requests for a resources to the pending table. Decompose so each request has 1 memslice or 1 core.
     * 
     * @param id          the id of the first pending request to generate (null if dynamically generate)
     * @param cores       the number of cores to request
     * @param memslices   the number of memslices to request
     * @param application the application that is requesting the resource(s)
     */
    public void generateRequests(final Long id, final long cores, final long memslices, final long application) {
        LOG.info("Created request for application {} ({} cores, {} memslices)", 
                application, cores, memslices);
        Long currentId = id;

        // Create a list of records
        final List<PendingRecord> records = new ArrayList();
        for (int i = 0; i < cores + memslices; i++) {
            final PendingRecord record = new PendingRecord();
            record.setValue(PENDING_TABLE.APPLICATION, (int) application);
            if (currentId != null) {
                record.setValue(PENDING_TABLE.ID, (long) currentId);
                currentId++;
            }
            if (i < cores) {
                record.setValue(PENDING_TABLE.CORES, (int) 1);
                record.setValue(PENDING_TABLE.MEMSLICES, (int) 0);
            } else {
                record.setValue(PENDING_TABLE.CORES, (int) 0);
                record.setValue(PENDING_TABLE.MEMSLICES, (int) 1);
            }
            record.setValue(PENDING_TABLE.STATUS, "PENDING");
            record.setValue(PENDING_TABLE.CURRENT_NODE, -1);
            record.setValue(PENDING_TABLE.CONTROLLABLE__NODE, null);

            records.add(record);
        }

        // Batch execute them all at once
        final int[] results = conn.batchInsert(records).execute();
    }
    
    /**
     * Record resources as allocated in the database.
     * 
     * @param node        the node that owns the resources
     * @param application the application the resources are allocated to
     * @param cores       the number of cores allocated
     * @param memslices   the number of memslices allocated
     */
    public void updateAllocation(final long node, final long application, final long cores, final long memslices) {
        if (memslices == 0 && cores == 0) {
            LOG.warn("Cannot update allocation, nothing to do");
        } else if (memslices == 0) {
            conn.insertInto(PLACED_TABLE,
                    PLACED_TABLE.APPLICATION, PLACED_TABLE.NODE, PLACED_TABLE.CORES,
                    PLACED_TABLE.MEMSLICES)
                    .values((int) application, (int) node, (int) cores, (int) memslices)
                    .onDuplicateKeyUpdate()
                    .set(PLACED_TABLE.CORES, PLACED_TABLE.CORES.plus(cores))
                    .execute();
        } else if (cores == 0) {
            conn.insertInto(PLACED_TABLE,
                    PLACED_TABLE.APPLICATION, PLACED_TABLE.NODE, PLACED_TABLE.CORES,
                    PLACED_TABLE.MEMSLICES)
                    .values((int) application, (int) node, (int) cores, (int) memslices)
                    .onDuplicateKeyUpdate()
                    .set(PLACED_TABLE.MEMSLICES, PLACED_TABLE.MEMSLICES.plus(memslices))
                    .execute();
        } else {
            conn.insertInto(PLACED_TABLE,
                    PLACED_TABLE.APPLICATION, PLACED_TABLE.NODE, PLACED_TABLE.CORES,
                    PLACED_TABLE.MEMSLICES)
                    .values((int) application, (int) node, (int) cores, (int) memslices)
                    .onDuplicateKeyUpdate()
                    .set(PLACED_TABLE.CORES, PLACED_TABLE.CORES.plus(cores))
                    .set(PLACED_TABLE.MEMSLICES, PLACED_TABLE.MEMSLICES.plus(memslices))
                    .execute();
        }
    }

    /**
     * Release allocation (e.g., mark resources as unused)
     * 
     * @param node        the node that formerly owned the resources
     * @param application the application the resources were formerly allocated to
     * @param cores       the number of cores to release
     * @param memslices   the number of memslices to release
     */
    public void releaseAllocation(final long node, final long application, final long cores, final long memslices) {
        if (memslices == 0 && cores == 0) {
            LOG.warn("Cannot reduce allocation, nothing to do");
        } else {
            conn.update(PLACED_TABLE)
                .set(PLACED_TABLE.MEMSLICES, PLACED_TABLE.MEMSLICES.sub((int) memslices))
                .set(PLACED_TABLE.CORES, PLACED_TABLE.CORES.sub((int) cores))
                .where(and(PLACED_TABLE.NODE.eq((int) node), PLACED_TABLE.APPLICATION.eq((int) application)))
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
    public boolean runSolverAndUpdateDB(final boolean printTimingData) throws Exception {
        final Result<? extends Record> results;
        final long start = System.currentTimeMillis();
        final long solveFinish;
        try {
            results = solver.solve(conn);
            solveFinish = System.currentTimeMillis();
        } catch (final SolverException e) {
            LOG.error(e);
            return false;
        }

        // Solver did no work
        if (results.isEmpty()) {
            return false;
        }

        // TODO: should find a way to batch these. Add new assignments to placed, remove new assignments from pending
        results.forEach(r -> {
            updateAllocation((Integer) r.get("CONTROLLABLE__NODE"), (Integer) r.get("APPLICATION"),
                    (Integer) r.get("CORES"), (Integer) r.get("MEMSLICES"));
            conn.deleteFrom(PENDING_TABLE)
                    .where(PENDING_TABLE.ID.eq((Long) r.get("ID")))
                    .execute();
        });
        final long updateFinish = System.currentTimeMillis();
        if (printTimingData) {
            System.out.println(String.format("SOLVE_RESULTS: solve=%dms, solve_update=%dms", 
                    solveFinish - start, updateFinish - start));
        }
        return true;
    }

    /**
     * Double check database to ensure resources aren't being overprovisioned
     * 
     * @return hasViolation true if capacity is violated, false if capacity is
     *         respected
     */
    public boolean checkForCapacityViolation() {
        // Check total capacity
        // TODO: could make this more efficient by using unallocated view
        final boolean totalCoreCheck = coreCapacity() >= usedCores();
        final boolean totalMemsliceCheck = memsliceCapacity() >= usedCores();

        boolean nodeCoreCheck = true;
        boolean nodeMemsliceCheck = true;

        for (int node = 1; node <= numNodes(); node++) {
            nodeCoreCheck = nodeCoreCheck && (coreCapacityForNode(node) >= usedCoresForNode(node));
            nodeMemsliceCheck = nodeMemsliceCheck &&
                    (memsliceCapacityForNode(node) >= usedMemslicesForNode(node));
        }

        return !(totalCoreCheck && totalMemsliceCheck && nodeCoreCheck && nodeMemsliceCheck);
    }
}
