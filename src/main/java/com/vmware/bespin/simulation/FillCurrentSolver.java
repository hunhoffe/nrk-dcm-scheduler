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

import com.vmware.bespin.scheduler.Solver;
import com.vmware.bespin.scheduler.SolverException;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.records.PendingRecord;

public class FillCurrentSolver implements Solver {

    protected Logger LOG = LogManager.getLogger(FillCurrentSolver.class);
    public static final Pending PENDING_TABLE = Pending.PENDING;

    private int numNodes;
    private int numApplications;
    private int nodeIterator = 17; // This number should be relatively prime towards numNodes

    private ArrayList<Long> freeCores = new ArrayList<Long>();
    private ArrayList<Long> freeMemslices = new ArrayList<Long>();

    private ArrayList<Integer> coreIndices = new ArrayList<Integer>();
    private ArrayList<Integer> memsliceIndices = new ArrayList<Integer>();

    /**
     * Assign requests for cores and memslices to nodes in a 'sticky' fashion,
     * that is, fill the current node before moving on.
     * @param numApplications the number of applications
     * @param numNodes the number of worker hosts in the cluster
     * @param coresPerNode the number of cores per worker host
     * @param memslicesPerNode the number of memslices per worker host
     */
    public FillCurrentSolver(final int numApplications, final int numNodes, final long coresPerNode, 
            final long memslicesPerNode) {
        this.numNodes = numNodes;
        this.numApplications = numApplications;
        assert relativelyPrime(numNodes, nodeIterator);

        for (int i = 0; i < this.numNodes; i++) {
            freeMemslices.add(memslicesPerNode);
            freeCores.add(coresPerNode);
        }

        for (int i = 0; i < this.numApplications; i++) {
            coreIndices.add(i % numNodes);
            memsliceIndices.add(i % numNodes);
        }

        /*
        // we can remove this, this is just sanity checking that we get a complete cycle over the nodes
        for (int app = 0; app < numApplications; app++) {
            final Set<Integer> visits = new HashSet<Integer>();
            int currentNode = app % numNodes;
            for (int i = 0; i < numNodes; i++) {
                visits.add(currentNode);
                currentNode = (currentNode + nodeIterator) % numNodes;
            }
            // Should start and end in the same place
            assert (app % numNodes) == currentNode;
            // Each currentNode shoud be unique to be full cycle over the nodes
            assert visits.size() == numNodes;
        }
        */
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
                final int application = pending.getApplication();
                assert coresToPlace == 0 || memslicesToPlace == 0;

                // Place the cores
                if (coresToPlace > 0) {
                    final int coreIndex = coreIndices.get(application - 1);
                    long freeCoresForNode = freeCores.get(coreIndex);

                    if (freeCoresForNode >= coresToPlace) {
                        // If current node has space, allocate from there
                        freeCores.set(coreIndex, freeCoresForNode - coresToPlace);
                        pending.setControllable_Node(coreIndex + 1);
                    } else {
                        // If current node does not have space, find a new node.
                        boolean placed = false;
                        int newCoreIndex = coreIndex;

                        while (!placed) {
                            newCoreIndex = (newCoreIndex + nodeIterator) % numNodes;
                            freeCoresForNode = freeCores.get(newCoreIndex);
                            if (freeCoresForNode >= coresToPlace) {
                                // If current node has space, allocate from there and update core index
                                freeCores.set(newCoreIndex, freeCoresForNode - coresToPlace);
                                pending.setControllable_Node(newCoreIndex + 1);
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
                    final int memsliceIndex = memsliceIndices.get(application - 1);
                    long freeMemslicesForNode = freeMemslices.get(memsliceIndex);

                    if (freeMemslicesForNode >= memslicesToPlace) {
                        // If current node has space, allocate from there
                        freeMemslices.set(memsliceIndex, freeMemslicesForNode - memslicesToPlace);
                        pending.setControllable_Node(memsliceIndex + 1);
                    } else {
                        // If current node does not have space, find a new node.
                        boolean placed = false;
                        int newMemsliceIndex = memsliceIndex;

                        while (!placed) {
                            newMemsliceIndex = (newMemsliceIndex + nodeIterator) % numNodes;
                            freeMemslicesForNode = freeMemslices.get(newMemsliceIndex);
                            if (freeMemslicesForNode >= memslicesToPlace) {
                                // If current node has space, allocate from there and update memslice index
                                freeMemslices.set(newMemsliceIndex, freeMemslicesForNode - memslicesToPlace);
                                pending.setControllable_Node(newMemsliceIndex + 1);
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
