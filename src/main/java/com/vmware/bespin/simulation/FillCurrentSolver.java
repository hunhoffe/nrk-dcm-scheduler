/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.simulation;

import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;

import com.vmware.bespin.scheduler.Scheduler;
import com.vmware.bespin.scheduler.Solver;
import com.vmware.bespin.scheduler.SolverException;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.records.PendingRecord;

public class FillCurrentSolver implements Solver {

    protected Logger LOG = LogManager.getLogger(FillCurrentSolver.class);
    public static final Pending PENDING_TABLE = Pending.PENDING;

    public static final int MAX_APPLICATIONS = 256;  // somewhat arbitrary, could also be done dynamically
    private int nodeIterator = 17; // This number should be relatively prime towards numNodes

    private ArrayList<Integer> coreIndices = new ArrayList<Integer>();
    private ArrayList<Integer> memsliceIndices = new ArrayList<Integer>();

    /**
     * Assign requests for cores and memslices to nodes in a 'sticky' fashion,
     * that is, fill the current node before moving on.
     */
    public FillCurrentSolver() {
        for (int i = 0; i < MAX_APPLICATIONS; i++) {
            coreIndices.add(-1);
            memsliceIndices.add(-1);
        }
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
        final int numNodes = unallocatedResources[0].length;

        if (null != pendingRequests && pendingRequests.isNotEmpty()) {
            // For every request, randomly set the controllable node.
            for (int i = 0; i < pendingRequests.size(); i++) {
            //for (final org.jooq.Record r: pendingRequests) {
                // Cast as pending and parse out the resources it's asking for.
                final PendingRecord pending = pendingRequests.get(i).into(PENDING_TABLE);

                final long coresToPlace = pending.getCores();
                final long memslicesToPlace = pending.getMemslices();
                final int application = pending.getApplication();
                assert coresToPlace == 0 || memslicesToPlace == 0;

                // Place the cores
                if (coresToPlace > 0) {
                    int coreIndex = coreIndices.get(application);
                    if (coreIndex == -1) {
                        // Initialize the index
                        coreIndices.set(application, application % numNodes);
                        coreIndex = application % numNodes;
                    }
                    long freeCoresForNode = unallocatedResources[1][coreIndex];

                    if (freeCoresForNode >= coresToPlace) {
                        // If current node has space, allocate from there
                        unallocatedResources[1][coreIndex] -= (int) coresToPlace;
                        pending.setControllable_Node(unallocatedResources[0][coreIndex]);
                    } else {
                        // If current node does not have space, find a new node.
                        boolean placed = false;
                        int newCoreIndex = coreIndex;

                        while (!placed) {
                            newCoreIndex = (newCoreIndex + nodeIterator) % numNodes;
                            freeCoresForNode = unallocatedResources[1][newCoreIndex];
                            if (freeCoresForNode >= coresToPlace) {
                                // If current node has space, allocate from there and update core index
                                unallocatedResources[1][newCoreIndex] -= (int) coresToPlace;
                                pending.setControllable_Node(unallocatedResources[0][newCoreIndex]);
                                coreIndices.set(application, newCoreIndex);
                                placed = true;
                            } else if (newCoreIndex == coreIndex) {
                                // We've reached the end of the cycle, and didn't find anything free
                                // We can conclude there's no room in the cluster
                                throw new SolverException("Infeasible", null);
                            }

                        }
                    }
                }

                // Place the memslices
                if (memslicesToPlace > 0) {
                    int memsliceIndex = memsliceIndices.get(application);
                    if (memsliceIndex == -1) {
                        // Initialize the index
                        memsliceIndices.set(application, application % numNodes);
                        memsliceIndex = application % numNodes;
                    }

                    long freeMemslicesForNode = unallocatedResources[2][memsliceIndex];

                    if (freeMemslicesForNode >= memslicesToPlace) {
                        // If current node has space, allocate from there
                        unallocatedResources[2][memsliceIndex] -= (int) memslicesToPlace;
                        pending.setControllable_Node(memsliceIndex + 1);
                    } else {
                        // If current node does not have space, find a new node.
                        boolean placed = false;
                        int newMemsliceIndex = memsliceIndex;

                        while (!placed) {
                            newMemsliceIndex = (newMemsliceIndex + nodeIterator) % numNodes;
                            freeMemslicesForNode =  unallocatedResources[2][newMemsliceIndex];
                            if (freeMemslicesForNode >= memslicesToPlace) {
                                // If current node has space, allocate from there and update memslice index
                                unallocatedResources[2][newMemsliceIndex] -= (int) memslicesToPlace;
                                pending.setControllable_Node(unallocatedResources[0][newMemsliceIndex]);
                                memsliceIndices.set(application, newMemsliceIndex);
                                placed = true;
                            } else if (newMemsliceIndex == memsliceIndex) {
                                // We've reached the end of the cycle, and didn't find anything free
                                // We can conclude there's no room in the cluster
                                throw new SolverException("Infeasible", null);
                            }

                        }
                    }
                }

                pendingRequests.set(i, pending);
            }
        }
        return pendingRequests;
    }


    // From https://stackoverflow.com/questions/28575416/how-to-find-out-if-two-numbers-are-relatively-prime
    private static int gcd(int a, int b) {
        int tmp;
        if (b < a) {
            tmp = b;
            b = a;
            a = tmp;
        }
        while (b != 0) {
            tmp = a;
            a = b;
            b = tmp % b;
        }
        return a;
    }

    // From https://stackoverflow.com/questions/28575416/how-to-find-out-if-two-numbers-are-relatively-prime
    private static boolean relativelyPrime(final int a, final int b) {
        return gcd(a, b) == 1;
    }
    
}
