/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.simulation;

import java.util.ArrayList;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;

import com.vmware.bespin.scheduler.Solver;
import com.vmware.bespin.scheduler.SolverException;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.records.PendingRecord;

public class RandomSolver implements Solver {

    protected Logger LOG = LogManager.getLogger(RandomSolver.class);
    public static final Pending PENDING_TABLE = Pending.PENDING;

    private int numNodes;

    private ArrayList<Long> freeCores = new ArrayList<Long>();
    private ArrayList<Long> freeMemslices = new ArrayList<Long>();

    private final RandomDataGenerator rand;

    /**
     * Randomly assign requests for cores and memslices to nodes.
     * cores and memslices
     * @param conn The database connection.
     */
    public RandomSolver(final int numNodes, final long coresPerNode, final long memslicesPerNode) {
        this.numNodes = numNodes;
        this.rand = new RandomDataGenerator();

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
                    final ArrayList<Integer> options = new ArrayList<Integer>();
                    for (int j = 0; j < freeCores.size(); j++) {
                        if (freeCores.get(j) >= coresToPlace) {
                            options.add(j);
                        }
                    }
                    if (options.size() == 0) {
                        throw new SolverException("Infeasible", null);
                    }

                    final int trialPlacement = rand.nextInt(0, options.size() - 1);
                    final int coreIndex = options.get(trialPlacement);
                    final long freeCoresForNode = freeCores.get(coreIndex);
                    if (freeCoresForNode >= coresToPlace) {
                        freeCores.set(coreIndex, freeCoresForNode - coresToPlace);
                        pending.setControllable_Node(coreIndex + 1);
                    }
                }

                // Place the memslices
                if (memslicesToPlace > 0) {
                    final ArrayList<Integer> options = new ArrayList<Integer>();
                    for (int j = 0; j < freeMemslices.size(); j++) {
                        if (freeMemslices.get(j) >= memslicesToPlace) {
                            options.add(j);
                        }
                    }
                    if (options.size() == 0) {
                        throw new SolverException("Infeasible", null);
                    }

                    final int trialPlacement = rand.nextInt(0, options.size() - 1);
                    final int memsliceIndex = options.get(trialPlacement);
                    final long freeMemslicesForNode = freeMemslices.get(memsliceIndex);
                    if (freeMemslicesForNode >= memslicesToPlace) {
                        freeMemslices.set(memsliceIndex, freeMemslicesForNode - memslicesToPlace);
                        pending.setControllable_Node(memsliceIndex + 1);
                    }
                }
                pendingRequests.set(i, pending);
            }
        }
        return pendingRequests;
    }
}
