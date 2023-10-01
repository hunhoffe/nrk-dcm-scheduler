/*
 * Copyright 2023 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.simulation;

import java.util.ArrayList;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;

import com.vmware.bespin.scheduler.Scheduler;
import com.vmware.bespin.scheduler.Solver;
import com.vmware.bespin.scheduler.SolverException;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.records.PendingRecord;

public class RandomSolver implements Solver {

    protected Logger LOG = LogManager.getLogger(RandomSolver.class);
    public static final Pending PENDING_TABLE = Pending.PENDING;

    private final RandomDataGenerator rand;

    /**
     * Randomly assign requests for cores and memslices to nodes.
     */
    public RandomSolver() {
        this.rand = new RandomDataGenerator();
    }

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
                // Cast as pending and parse out the resources it's asking for.
                final PendingRecord pending = pendingRequests.get(i).into(PENDING_TABLE);
                final long coresToPlace = pending.getCores();
                final long memslicesToPlace = pending.getMemslices();
                assert coresToPlace == 0 || memslicesToPlace == 0;

                // Place the cores
                if (coresToPlace > 0) {
                    final ArrayList<Integer> options = new ArrayList<Integer>();
                    for (int j = 0; j < unallocatedResources[0].length; j++) {
                        if (unallocatedResources[1][j] >= coresToPlace) {
                            options.add(j);
                        }
                    }
                    if (options.size() == 0) {
                        throw new SolverException("Infeasible", null);
                    }

                    final int trialPlacement = rand.nextInt(0, options.size() - 1);
                    final int coreIndex = options.get(trialPlacement);
                    unallocatedResources[1][coreIndex] -= (int) coresToPlace;
                    pending.setControllable_Node(unallocatedResources[0][coreIndex]);
                }

                // Place the memslices
                if (memslicesToPlace > 0) {
                    final ArrayList<Integer> options = new ArrayList<Integer>();
                    for (int j = 0; j < unallocatedResources[0].length; j++) {
                        if (unallocatedResources[2][j] >= memslicesToPlace) {
                            options.add(j);
                        }
                    }
                    if (options.size() == 0) {
                        throw new SolverException("Infeasible", null);
                    }

                    final int trialPlacement = rand.nextInt(0, options.size() - 1);
                    final int memsliceIndex = options.get(trialPlacement);
                    unallocatedResources[2][memsliceIndex] -= (int) memslicesToPlace;
                    pending.setControllable_Node(unallocatedResources[0][memsliceIndex]);
                }
                pendingRequests.set(i, pending);
            }
        }
        return pendingRequests;
    }
}
