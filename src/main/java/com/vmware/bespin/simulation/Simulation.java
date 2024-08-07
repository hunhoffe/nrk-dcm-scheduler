/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.simulation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.Record;

import com.vmware.bespin.scheduler.Scheduler;
import com.vmware.bespin.scheduler.generated.tables.Applications;
import com.vmware.bespin.scheduler.generated.tables.Nodes;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.Placed;

public class Simulation {
    public static final Nodes NODE_TABLE = Nodes.NODES;
    public static final Applications APP_TABLE = Applications.APPLICATIONS;
    public static final Placed PLACED_TABLE = Placed.PLACED;
    public static final Pending PENDING_TABLE = Pending.PENDING;

    protected Logger LOG = LogManager.getLogger(Simulation.class);
    protected Scheduler scheduler;
    protected DSLContext conn;
    protected final RandomDataGenerator rand;
    protected long numApps;
    protected long coresPerNode;
    protected long memslicesPerNode;

    public Simulation(final DSLContext conn, final Scheduler scheduler, final Integer randomSeed, 
            final long numNodes, final long coresPerNode, final long memslicesPerNode, final long numApps) {
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

        this.scheduler = scheduler;
        this.conn = conn;
        this.numApps = numApps;
        this.coresPerNode = coresPerNode;
        this.memslicesPerNode = memslicesPerNode;

        if (randomSeed == null) {
            this.rand = new RandomDataGenerator();
        } else {
            this.rand = new RandomDataGenerator(new JDKRandomGenerator(randomSeed));
        }

        // Add nodes with specified cores & memslices
        for (int i = 1; i <= numNodes; i++) {
            scheduler.addNode(i, coresPerNode, memslicesPerNode);
        }
        
        // Add initial applications
        for (int i = 0; i < numApps; i++) {
            scheduler.addApplication(i);
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
    protected long coresForUtil(final int clusterUtil) {
        // Determine the number of cores to alloc in order to meet utilization
        return (long) Math.ceil(((float) (scheduler.coreCapacity() * clusterUtil)) / 100.0);
    }

    /**
     * Select an application at random
     * 
     * @return application the application selected
     */
    protected long chooseRandomApplication() {
        final String applicationIds = "select id from applications";
        final Result<Record> results = conn.fetch(applicationIds);
        return ((Integer) results.get(rand.nextInt(0, results.size() - 1)).getValue(0)).longValue();
    }

    /**
     * Generate a random request.
     * 
     * Determine if it's for a core or memslice in proportion to that resource.
     * Choose an application at uniform random.
     */
    public void generateRandomRequest() {
        final long totalResources = scheduler.coreCapacity() + scheduler.memsliceCapacity();

        // Select an application at random
        final long application = this.chooseRandomApplication();

        // Determine if core or memslice, randomly chosen but weighted by capacity
        int cores = 0;
        int memslices = 0;
        if (rand.nextLong(1, totalResources) < scheduler.coreCapacity()) {
            cores = 1;
        } else {
            memslices = 1;
        }
        scheduler.generateRequest(null, cores, memslices, application);
    }

    /**
     * Select an application based on a Gaussian distribution.
     * 
     * @return application the application selected
     */
    protected long chooseGaussianApplication() {
        final double appMean = numApps / 2;
        final double appStdev = numApps / 6;
        int count = 0;
        
        // Choose an application to make the request
        long application = Math.round(rand.nextGaussian(appMean, appStdev));
        while (application < 0 || application >= numApps) {
            application = Math.round(rand.nextGaussian(appMean, appStdev));
            count++;
            if (count > 100) {
                System.out.println("ERROR(SPINNING): TRIED TO GENERATE AN APPLICATION ID 100 TIMES");
                System.exit(-1);
            }
        }
        return application;
    }

    /**
     * Determine the number of memslices that must be allocated to reach a target
     * cluster utilization
     * 
     * @param clusterUtil the target cluster utilization percentage
     * @return numMemslices the number of memslices that must be allocated to read
     *         the cluster utilization
     */
    protected long memslicesForUtil(final int clusterUtil) {
        // Determine the number of memslices to alloc in order to meet utilization
        return (long) Math.ceil(((float) (scheduler.memsliceCapacity() * clusterUtil)) / 100.0);
    }

    /**
     * Randomly determine the number of cores and memslices to allocate for each
     * application.
     * 
     * @param coreAllocs     the aggregate number of core allocations to assign
     * @param memsliceAllocs the aggregate number of memslice allocations to assign
     * @return allocMap a map containing a number of cores and memslices for each
     *         application
     */
    protected HashMap<Long, List<Long>> generateAllocMap(final long coreAllocs,
            final long memsliceAllocs) {
        // Format of key=application number, values=[num_cores, num_memslices]
        final HashMap<Long, List<Long>> appAllocMap = new HashMap<>();

        // Assign cores to applications
        for (long i = 0; i < coreAllocs; i++) {
            final long application = rand.nextLong(0, numApps - 1);
            final List<Long> key = appAllocMap.getOrDefault(application, List.of(0L, 0L));
            appAllocMap.put(application, List.of(key.get(0) + 1, key.get(1)));
        }

        // Assign memslices to applications
        for (long i = 0; i < memsliceAllocs; i++) {
            final long application = rand.nextLong(0, numApps - 1);
            final List<Long> key = appAllocMap.getOrDefault(application, List.of(0L, 0L));
            appAllocMap.put(application, List.of(key.get(0), key.get(1) + 1));
        }
        return appAllocMap;
    }

    /**
     * Check if the database contains the correct number of core allocs and memslice
     * alloc, as well as check for capacity violations. There should also be no pending
     * allocations after running a fill algorithm
     * 
     * @param coreAllocs     the exepcted number of core allocations
     * @param memsliceAllocs the expected number of memslice allocations
     */
    protected void checkFill(final long coreAllocs, final long memsliceAllocs) {
        assert (scheduler.usedCores() == coreAllocs);
        assert (scheduler.usedMemslices() == memsliceAllocs);
        assert (!scheduler.checkForCapacityViolation());
        assert (scheduler.getNumPendingRequests() == 0);
    }

    /**
     * Fill the cluster to a certain utilization percentage by generating random
     * requests as uniform random.
     * 
     * @param clusterUtil target cluster utilization
     */
    public void fillRandom(final int clusterUtil) {
        assert scheduler.usedCores() == 0;
        assert scheduler.usedMemslices() == 0;

        // Determine the number of both resources to allocate based on clusterUtil
        final long coreAllocs = coresForUtil(clusterUtil);
        final long memsliceAllocs = memslicesForUtil(clusterUtil);
        final long numNodes = scheduler.numNodes();

        // Format of key=application number, values=[num_cores, num_memslices]
        final HashMap<Long, List<Long>> appAllocMap = generateAllocMap(coreAllocs, memsliceAllocs);

        // Randomly assign application allocs to nodes
        for (final Map.Entry<Long, List<Long>> entry : appAllocMap.entrySet()) {
            final long application = entry.getKey();
            final long cores = entry.getValue().get(0);
            final long memslices = entry.getValue().get(1);

            // Assign cores
            for (int i = 0; i < cores; i++) {
                boolean done = false;
                while (!done) {
                    // Choose a random node
                    final long node = rand.nextLong(1, numNodes);

                    // If there is a core available, allocate it.
                    if (scheduler.coreCapacityForNode(node) - scheduler.usedCoresForNode(node) > 0) {
                        scheduler.updateAllocation(node, application, 1L, 0L);
                        done = true;
                    }
                }
            }

            // Assign memslices
            for (int i = 0; i < memslices; i++) {
                boolean done = false;
                while (!done) {
                    // Choose a random node
                    final long node = rand.nextLong(1, numNodes);

                    // If there is a core available, allocate it.
                    if (scheduler.memsliceCapacityForNode(node) - scheduler.usedMemslicesForNode(node) > 0) {
                        scheduler.updateAllocation(node, application, 0L, 1L);
                        done = true;
                    }
                }
            }
        }
        checkFill(coreAllocs, memsliceAllocs);
    }


    /**
     * Fill the cluster to a certain utilization percentage by generating requests
     * based on a Poisson distribution. There may be some error where the cluster utilization
     * isn't perfectly met.
     * 
     * @param clusterUtil  target cluster utilization
     * @param coreMean     the mean cluster-wide core requests per solve step
     * @param memsliceMean the mean cluster-wide memslice requests per solve step
     */
    public void fillPoisson(final int clusterUtil, final double coreMean, final double memsliceMean) throws Exception {
        assert scheduler.usedCores() == 0;
        assert scheduler.usedMemslices() == 0;
        
        // Determine the number of both resources to allocate based on clusterUtil
        final long targetCoreAllocs = coresForUtil(clusterUtil);
        final long targetMemsliceAllocs = memslicesForUtil(clusterUtil);

        long allocatedCores = scheduler.usedCores();
        long allocatedMemslices = scheduler.usedMemslices();
        int numSolves = 0;
        while (allocatedCores < targetCoreAllocs || allocatedMemslices < targetMemsliceAllocs) {
            stepPoisson(coreMean, allocatedCores < targetCoreAllocs, memsliceMean,
                    allocatedMemslices < targetMemsliceAllocs);
            numSolves += 1;
            allocatedCores = scheduler.usedCores();
            allocatedMemslices = scheduler.usedMemslices();

            if (numSolves > 1000) {
                System.out.println("ERROR(SPINNING): FILL POISSON STEPPED 1000 TIMES");
                System.exit(-1);
            }
        }

        // Check that we got what we wanted
        checkFill(allocatedCores, allocatedMemslices);

        LOG.warn("Poisson fill completed in {} steps (coreMean={}, memsliceMean={})", 
                numSolves, coreMean, memsliceMean);

        if (targetCoreAllocs != allocatedCores) {
            LOG.warn("Poisson fill did not exactly meet target cluster util." +
                    " Core error is {} cores ({}% of total cores)",
                    targetCoreAllocs - allocatedCores,
                    (float) Math.abs(targetCoreAllocs - allocatedCores) / (float) scheduler.coreCapacity());
        }
        if (targetMemsliceAllocs != allocatedMemslices) {
            LOG.warn("Poisson fill did not exactly meet target cluster util. " +
                    "Memslice error is {} memslices ({}% of total memslices)",
                    targetMemsliceAllocs - allocatedMemslices,
                    (float) Math.abs(targetMemsliceAllocs - allocatedMemslices) / (float) scheduler.memsliceCapacity());
        }
    }

    /**
     * Fill the cluster to a certain utilization percentage by generating requests
     * based on a Poisson distribution. There may be some error where the cluster utilization
     * isn't perfectly met.
     * 
     * @param coreMean     the mean cluster-wide core requests per solve step
     * @param allocCores   if the step should include core requests
     * @param memsliceMean the mean cluster-wide memslice requests per solve step
     * @param allocMemslices if the step should include memslice requests
     */
    public void stepPoisson(final double coreMean, final boolean allocCores, final double memsliceMean, 
            final boolean allocMemslices) throws Exception {       
        long coresToAlloc = 0;
        long memslicesToAlloc = 0;
        int count = 0;

        // Generate requests for cores
        while (coresToAlloc == 0 && memslicesToAlloc == 0) {
            if (allocCores) {
                // Ensure we don't overfill the cluster
                coresToAlloc = Math.min(scheduler.coreCapacity() - scheduler.usedCores(), 
                        rand.nextPoisson(coreMean));
                // Generate as individual requests
                for (int i = 0; i < coresToAlloc; i++) {
                    scheduler.generateRequest(null, 1L, 0L, chooseGaussianApplication());
                }
            }

            // Generate requests for memslices
            if (allocMemslices) {
                // Ensure we don't overfill the cluster
                memslicesToAlloc = Math.min(scheduler.memsliceCapacity() - scheduler.usedMemslices(), 
                        rand.nextPoisson(memsliceMean));
                // Generate as individual requests
                for (int i = 0; i < memslicesToAlloc; i++) {
                    scheduler.generateRequest(null, 0L, 1L, chooseGaussianApplication());
                }
            }
            count++;
            if (count > 100) {
                System.out.println("ERROR(SPINNING): TRIED TO MAKE PROGRESS IN POISSON STEP 100 TIMES");
                System.exit(-1);
            }
        }

        // Now solve all requests from this round
        scheduler.runSolverAndUpdateDB();
    }
}