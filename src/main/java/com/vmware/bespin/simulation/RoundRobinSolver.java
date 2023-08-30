/*
 * Copyright 2023 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.simulation;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;

import com.vmware.bespin.scheduler.Solver;
import com.vmware.bespin.scheduler.SolverException;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.records.PendingRecord;

public class RoundRobinSolver implements Solver {

    protected Logger LOG = LogManager.getLogger(RoundRobinSolver.class);
    public static final Pending PENDING_TABLE = Pending.PENDING;

    private int numNodes;

    private ArrayList<Long> freeCores = new ArrayList<Long>();
    private ArrayList<Long> freeMemslices = new ArrayList<Long>();

    private int coreIndex = 0;
    private int memsliceIndex = 0;

    /**
     * Assign requests for cores and memslices to nodes in round-robin fashion.
     * @param numNodes the number of worker hosts in the cluster
     * @param coresPerNode the number of cores per worker host
     * @param memslicesPerNode the number of memslices per worker host
     */
    public RoundRobinSolver(final int numNodes, final long coresPerNode, final long memslicesPerNode) {
        this.numNodes = numNodes;

        for (int i = 0; i < this.numNodes; i++) {
            freeMemslices.add(memslicesPerNode);
            freeCores.add(coresPerNode);
        }
    }

    /**
     * Solve all outstanding requests in the pending table
     * 
     * @param conn database connection
     * @throws Exception this should never happen, but overriding subclasses may
     *                   throw errors.
     */
    public Result<? extends org.jooq.Record> solve(final DSLContext conn) throws SolverException {

        // Fetch the requests to solve for
        final Result<org.jooq.Record> pendingRequests = conn.select().from(PENDING_TABLE).fetch();

        if (null != pendingRequests && pendingRequests.isNotEmpty()) {
            // For every request, randomly set the controllable node.
            for (int i = 0; i < pendingRequests.size(); i++) {
            //for (final org.jooq.Record r: pendingRequests) {
                // Cast as pending and parse out the resources it's asking for.
                final PendingRecord pending = pendingRequests.get(i).into(PENDING_TABLE);
                final long coresToPlace = pending.getCores();
                final long memslicesToPlace = pending.getMemslices();
                assert coresToPlace == 0 || memslicesToPlace == 0;

                // Place the cores
                if (coresToPlace > 0) {
                    boolean placed = false;
                    final int coreIndexStart = coreIndex;
                    while (!placed) {
                        final long freeCoresForNode = freeCores.get(coreIndex);
                        if (freeCoresForNode >= coresToPlace) {
                            freeCores.set(coreIndex, freeCoresForNode - coresToPlace);
                            pending.setControllable_Node(coreIndex + 1);
                            placed = true;
                        }
                        coreIndex = (coreIndex + 1) % numNodes;
                        if ((coreIndex == coreIndexStart) && !placed) {
                            throw new SolverException("Infeasible", null);
                        }
                    }
                }

                // Place the memslices
                if (memslicesToPlace > 0) {
                    boolean placed = false;
                    final int memsliceIndexStart = memsliceIndex;
                    while (!placed) {
                        final long freeMemslicesForNode = freeMemslices.get(memsliceIndex);
                        if (freeMemslicesForNode >= memslicesToPlace) {
                            freeMemslices.set(memsliceIndex, freeMemslicesForNode - memslicesToPlace);
                            pending.setControllable_Node(memsliceIndex + 1);
                            placed = true;
                        }
                        memsliceIndex = (memsliceIndex + 1) % numNodes;
                        if ((memsliceIndex == memsliceIndexStart) && !placed) {
                            throw new SolverException("Infeasible", null);
                        }
                    }
                }

                pendingRequests.set(i, pending);
            }
        }
        return pendingRequests;
    }
}
