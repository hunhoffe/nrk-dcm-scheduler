/*
 * Copyright 2023 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.simulation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;

import com.vmware.bespin.scheduler.Scheduler;
import com.vmware.bespin.scheduler.Solver;
import com.vmware.bespin.scheduler.SolverException;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.records.PendingRecord;

public class RoundRobinSolver implements Solver {

    protected Logger LOG = LogManager.getLogger(RoundRobinSolver.class);
    public static final Pending PENDING_TABLE = Pending.PENDING;

    private int coreIndex = 0;
    private int memsliceIndex = 0;

    /**
     * Assign requests for cores and memslices to nodes in round-robin fashion.
     */
    public RoundRobinSolver() { }

    /**
     * Solve all outstanding requests in the pending table
     * 
     * @param conn database connection
     * @throws Exception this should never happen, but overriding subclasses may
     *                   throw errors.
     */
    public Result<? extends org.jooq.Record> solve(final DSLContext conn, 
                                                   final Scheduler scheduler) throws SolverException {

        // Fetch the requests to solve for
        final Result<org.jooq.Record> pendingRequests = conn.select().from(PENDING_TABLE).fetch();
        final Integer[][] unallocatedResources = scheduler.unallocatedResources();

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
                        final long freeCoresForNode = unallocatedResources[1][coreIndex];
                        if (freeCoresForNode >= coresToPlace) {
                            unallocatedResources[1][coreIndex] -= (int) coresToPlace;
                            pending.setControllable_Node(unallocatedResources[0][coreIndex]);
                            placed = true;
                        }
                        coreIndex = (coreIndex + 1) % unallocatedResources[0].length;
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
                        final long freeMemslicesForNode = unallocatedResources[2][memsliceIndex];
                        if (freeMemslicesForNode >= memslicesToPlace) {
                            unallocatedResources[2][memsliceIndex] -= (int) memslicesToPlace;
                            pending.setControllable_Node(unallocatedResources[0][memsliceIndex]);
                            placed = true;
                        }
                        memsliceIndex = (memsliceIndex + 1) % unallocatedResources[0].length;
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
