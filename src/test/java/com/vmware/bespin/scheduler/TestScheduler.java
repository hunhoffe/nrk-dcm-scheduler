/*
 * Copyright 2023 University of Colorado and VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler;

import org.junit.jupiter.api.Test;

import com.vmware.bespin.scheduler.dinos.DiNOSSolver;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.jooq.DSLContext;


public class TestScheduler {

    private void populateCluster(Scheduler scheduler, long numNodes, long coresPerNode, long memslicesPerNode, long numApps) {
        // Populate applications
        for (int i = 1; i <= numApps; i++) {
            scheduler.addApplication(i);
        }

        // Populate nodes
        for (int i = 1; i <= numNodes; i++) {
            scheduler.addNode(i, coresPerNode, memslicesPerNode);
        }
    }

    @Test
    public void testRunnerInstantiationZero() throws ClassNotFoundException {
        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);

        // Check number of nodes
        assertEquals(0, scheduler.numNodes());
        
        // Check core capacity and use
        assertEquals(0, scheduler.coreCapacity());
        assertEquals(0, scheduler.usedCores());

        // Check memslice capacity
        assertEquals(0, scheduler.memsliceCapacity());
        assertEquals(0, scheduler.usedMemslices());

        // Check number of applications
        assertEquals(0, scheduler.numApps());

        // Check pending requests
        assertEquals(0, scheduler.getNumPendingRequests());
        long[] idList = scheduler.getPendingRequestIDs();
        assertEquals(0, idList.length);

        // Check capacity
        assertFalse(scheduler.checkForCapacityViolation());
    }

    /* TODO: move to simuation
    @Test
    public void testRunnerInstantiation() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);

        // Check number of nodes
        assertEquals(runner.numNodes(), NUM_NODES);
        
        // Check core capacity and use
        assertEquals(runner.coreCapacity(), NUM_NODES * CORES_PER_NODE);
        assertEquals(runner.usedCores(), 0);

        // Check memslice capacity
        assertEquals(runner.memsliceCapacity(), NUM_NODES * MEMSLICES_PER_NODE);
        assertEquals(runner.usedMemslices(), 0);

        // Check per-node valudes
        for (long i = 1; i <= NUM_NODES; i++) {
            assertEquals(runner.coreCapacityForNode(i), CORES_PER_NODE);
            assertEquals(runner.usedCoresForNode(i), 0);
            assertEquals(runner.memsliceCapacityForNode(i), MEMSLICES_PER_NODE);
            assertEquals(runner.usedMemslicesForNode(i), 0);
        }

        // Check number of applications
        assertEquals(runner.numApps(), NUM_APPS);

        // Check pending requests
        assertEquals(runner.getNumPendingRequests(), 0);
        long[] idList = runner.getPendingRequestIDs();
        assertEquals(0, idList.length);

        // Check capacity
        assertFalse(runner.checkForCapacityViolation());
    }
    */

    @Test
    public void testAddNode() throws ClassNotFoundException {
        long numNodes = 0;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        populateCluster(scheduler, numNodes, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        // Check number of nodes
        assertEquals(scheduler.numNodes(), 0);
        
        // Check core capacity and use
        assertEquals(scheduler.coreCapacity(), numNodes * CORES_PER_NODE);
        assertEquals(scheduler.usedCores(), 0);

        // Check memslice capacity
        assertEquals(scheduler.memsliceCapacity(), numNodes * MEMSLICES_PER_NODE);
        assertEquals(scheduler.usedMemslices(), 0);

        // Check per-node values
        for (long i = 1; i <= numNodes; i++) {
            assertEquals(scheduler.coreCapacityForNode(i), CORES_PER_NODE);
            assertEquals(scheduler.usedCoresForNode(i), 0);
            assertEquals(scheduler.memsliceCapacityForNode(i), MEMSLICES_PER_NODE);
            assertEquals(scheduler.usedMemslicesForNode(i), 0);
        }

        // Check number of applications
        assertEquals(scheduler.numApps(), NUM_APPS);

        assertFalse(scheduler.checkForCapacityViolation());

        numNodes += 1;
        scheduler.addNode(numNodes, CORES_PER_NODE, MEMSLICES_PER_NODE);
        
        // Check number of nodes
        assertEquals(scheduler.numNodes(), numNodes);
                
        // Check core capacity and use
        assertEquals(scheduler.coreCapacity(), numNodes * CORES_PER_NODE);
        assertEquals(scheduler.usedCores(), 0);
        
        // Check memslice capacity
        assertEquals(scheduler.memsliceCapacity(), numNodes * MEMSLICES_PER_NODE);
        assertEquals(scheduler.usedMemslices(), 0);
        
        // Check per-node values
        for (int i = 1; i <= numNodes; i++) {
            assertEquals(scheduler.coreCapacityForNode(i), CORES_PER_NODE);
            assertEquals(scheduler.usedCoresForNode(i), 0);
            assertEquals(scheduler.memsliceCapacityForNode(i), MEMSLICES_PER_NODE);
            assertEquals(scheduler.usedMemslicesForNode(i), 0);
        }
        
        // Check number of applications
        assertEquals(scheduler.numApps(), NUM_APPS);
        
        assertFalse(scheduler.checkForCapacityViolation());
    }

    @Test
    public void testUpdateNode() throws ClassNotFoundException {
        long numNodes = 0;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
    
        // Check number of nodes
        assertEquals(scheduler.numNodes(), numNodes);

        scheduler.addNode(1, CORES_PER_NODE, MEMSLICES_PER_NODE);
        numNodes += 1;
        assertEquals(scheduler.coreCapacity(), numNodes * CORES_PER_NODE);
        assertEquals(scheduler.usedCores(), 0);

        scheduler.addNode(2, CORES_PER_NODE, MEMSLICES_PER_NODE);
        numNodes += 1;
        assertEquals(scheduler.coreCapacity(), numNodes*CORES_PER_NODE);
        assertEquals(scheduler.usedCores(), 0);

        // Add CORES_PER_NODE to node 1, subtract one memslice from node 2
        scheduler.updateNode(1, CORES_PER_NODE, 0, true);
        scheduler.updateNode(2, 0, 1, false);
        
        // Check core capacity and use
        assertEquals(scheduler.coreCapacity(), numNodes * CORES_PER_NODE + CORES_PER_NODE);
        assertEquals(scheduler.usedCores(), 0);

        // Check memslice capacity
        assertEquals(scheduler.memsliceCapacity(), numNodes * MEMSLICES_PER_NODE - 1);
        assertEquals(scheduler.usedMemslices(), 0);

        // Check per-node values
        assertEquals(scheduler.coreCapacityForNode(1), CORES_PER_NODE * 2);
        assertEquals(scheduler.memsliceCapacityForNode(1), MEMSLICES_PER_NODE);
        assertEquals(scheduler.coreCapacityForNode(2), CORES_PER_NODE);
        assertEquals(scheduler.memsliceCapacityForNode(2), MEMSLICES_PER_NODE - 1);
    }

    @Test
    public void testAddApp() throws ClassNotFoundException {
        long numApps = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);

        for (int i = 1; i <= numApps; i++) {
            scheduler.addApplication(i);
            assertEquals(scheduler.numApps(), i);
        }
    }

    /* TODO: move to simulation tests
    @Test
    public void testChooseRandomApplication() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        DiNOSSolver solver = new DiNOSSolver(conn, false, false, false);
        Scheduler scheduler = new Scheduler(conn, solver);

        // Populate applications
        for (int i = 1; i <= NUM_APPS; i++) {
            scheduler.addApplication(i);
            assertEquals(scheduler.numApps(), i);
        }

        // Check number of applications
        for (int i = 0; i < 10; i++) {
            final long randomApp = scheduler.chooseRandomApplication();
            assert(randomApp >= 1 && randomApp <= NUM_APPS);
        }

        // Check number of applications
        assertEquals(runner.numApps(), NUM_APPS);
    }

    @Test
    public void testCoresForUtil() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        DCMScheduler runner = new DCMScheduler(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        // check cores for util at 100%
        assertEquals(runner.coresForUtil(100), runner.coreCapacity());
        assertEquals(runner.coresForUtil(100), NUM_NODES * CORES_PER_NODE);

        // check cores for util at 50%
        assertEquals(runner.coresForUtil(50), runner.coreCapacity() / 2);        
        assertEquals(runner.coresForUtil(50), (NUM_NODES * CORES_PER_NODE) / 2);
    }

    @Test
    public void testMemslicesForUtil() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        DCMScheduler runner = new DCMScheduler(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        // check cores for util at 100%
        assertEquals(runner.memslicesForUtil(100), runner.memsliceCapacity());
        assertEquals(runner.memslicesForUtil(100), NUM_NODES * MEMSLICES_PER_NODE);

        // check cores for util at 50%
        assertEquals(runner.memslicesForUtil(50), runner.memsliceCapacity() / 2);        
        assertEquals(runner.memslicesForUtil(50), (NUM_NODES * MEMSLICES_PER_NODE) / 2);
    }
    */

    @Test
    public void testPendingRequests() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        populateCluster(scheduler, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        // Add a request
        for (long i = 0; i < scheduler.coreCapacity() + scheduler.memsliceCapacity(); i++) {
            assertEquals(scheduler.getNumPendingRequests(), i);
            if (i < scheduler.coreCapacity()) {
                scheduler.generateRequest(null, 1, 0, (i % NUM_APPS) + 1);
            } else {
                scheduler.generateRequest(null, 0, 1, (i % NUM_APPS) + 1);
            }
        }
    }

    @Test
    public void testPendingRequestIdUnique() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        populateCluster(scheduler, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        Set<Long> pendingIdSet = new HashSet<Long>();

        // Add a request
        for (long i = 0; i < scheduler.coreCapacity() + scheduler.memsliceCapacity(); i++) {
            assertEquals(scheduler.getNumPendingRequests(), i);
            final long[] pendingIds = scheduler.getPendingRequestIDs();
            assertEquals(i, pendingIds.length);
            for (final long id: pendingIds) {
                pendingIdSet.add(id);
            }
            assertEquals(i, pendingIdSet.size());

            if (i < scheduler.coreCapacity()) {
                scheduler.generateRequest(null, 1, 0, (i % NUM_APPS) + 1);
            } else {
                scheduler.generateRequest(null, 0, 1, (i % NUM_APPS) + 1);
            }
        }
    }

    @Test
    public void testPendingRequestIds() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        populateCluster(scheduler, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        Set<Long> pendingIdSet = new HashSet<Long>();
        Set<Long> pendingIdSetBaseline = new HashSet<Long>();

        // Add a request
        for (long i = 0; i < scheduler.coreCapacity() + scheduler.memsliceCapacity(); i++) {
            assertEquals(scheduler.getNumPendingRequests(), i);
            final long[] pendingIds = scheduler.getPendingRequestIDs();
            assertEquals(i, pendingIds.length);
            for (final long id: pendingIds) {
                pendingIdSet.add(id);
            }
            assertEquals(i, pendingIdSet.size());
            assertEquals(pendingIdSetBaseline, pendingIdSet);

            if (i < scheduler.coreCapacity()) {
                scheduler.generateRequest(i, 1, 0, (i % NUM_APPS) + 1);
            } else {
                scheduler.generateRequest(i, 0, 1, (i % NUM_APPS) + 1);
            }
            pendingIdSetBaseline.add(i);
        }
    }

    /* TODO: add to simulator tests
    @Test
    public void testPendingRandomRequests() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        DCMScheduler runner = new DCMScheduler(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        // Add a request
        for (long i = 0; i < runner.coreCapacity() + runner.memsliceCapacity(); i++) {
            assertEquals(runner.getNumPendingRequests(), i);
            runner.generateRandomRequest();
        }
    }
    */

    @Test
    public void testNothingToSolve() throws Exception {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Solver solver = new DiNOSSolver(conn, true, true, false);
        Scheduler scheduler = new Scheduler(conn, solver);
        populateCluster(scheduler, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        assertEquals(scheduler.getNumPendingRequests(), 0);
        assertEquals(scheduler.usedCores() + scheduler.usedMemslices(), 0);

        // No requests to fulfill
        assertFalse(scheduler.runSolverAndUpdateDB(false));

        assertEquals(scheduler.getNumPendingRequests(), 0);
        assertEquals(scheduler.usedCores() + scheduler.usedMemslices(), 0);
    }

    @Test
    public void testUpdateAllocation() throws Exception {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 4;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        RandomDataGenerator rand = new RandomDataGenerator(new JDKRandomGenerator(0xc0ffee));
        populateCluster(scheduler, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        // Below code is dependent on NUM_NODE==2, and CORES_PER_NODE==MEMSLICES_PER_NODE
        for (long i = 0; i < (scheduler.coreCapacity() + scheduler.memsliceCapacity()) / 2; i++) {
            final long coreNode = (i % 2) + 1;
            final long memNode = ((i + 1) % 2) + 1;

            // Ensure this does nothing
            final long usedCores = scheduler.usedCores();
            final long usedMemslices = scheduler.usedMemslices();
            long usedCoresForNode = scheduler.usedCoresForNode(coreNode);
            long usedMemslicesForNode = scheduler.usedMemslicesForNode(coreNode);
            
            // add nothing
            scheduler.updateAllocation(coreNode, rand.nextLong(1, NUM_APPS), 0, 0);
            
            // check cores
            assertEquals(usedCores, scheduler.usedCores());
            assertEquals(usedCoresForNode, scheduler.usedCoresForNode(coreNode));
            
            // check memslices
            assertEquals(usedMemslices, scheduler.usedMemslices());
            assertEquals(usedMemslicesForNode, scheduler.usedMemslicesForNode(coreNode));

            // add a core
            final long coreApplication = rand.nextLong(1, NUM_APPS);
            long coresPerApplicationOnNode = scheduler.usedCoresForApplicationOnNode(coreApplication, coreNode);
            long memslicesPerApplicationOnNode = scheduler.usedMemslicesForApplicationOnNode(coreApplication, coreNode);
            scheduler.updateAllocation(coreNode, coreApplication, 1, 0);

            // check cores
            assertEquals(usedCores + 1, scheduler.usedCores());
            assertEquals(usedCoresForNode + 1, scheduler.usedCoresForNode(coreNode));
            assertEquals(scheduler.coreCapacityForNode(coreNode), CORES_PER_NODE);
            assertTrue(scheduler.usedCoresForNode(coreNode) <= scheduler.coreCapacityForNode(coreNode));
            assertEquals(scheduler.usedCoresForApplicationOnNode(coreApplication, coreNode), coresPerApplicationOnNode + 1);

            // check memslices
            assertEquals(usedMemslices, scheduler.usedMemslices());
            assertEquals(usedMemslicesForNode, scheduler.usedMemslicesForNode(coreNode));
            assertTrue(scheduler.usedMemslicesForNode(coreNode) <= scheduler.memsliceCapacityForNode(coreNode));
            assertEquals(scheduler.usedMemslicesForApplicationOnNode(coreApplication, coreNode), memslicesPerApplicationOnNode);

            // switch nodes
            usedCoresForNode = scheduler.usedCoresForNode(memNode);
            usedMemslicesForNode = scheduler.usedMemslicesForNode(memNode);
            
            // add a memslice
            final long memsliceApplication = rand.nextLong(1, NUM_APPS);
            coresPerApplicationOnNode = scheduler.usedCoresForApplicationOnNode(memsliceApplication, memNode);
            memslicesPerApplicationOnNode = scheduler.usedMemslicesForApplicationOnNode(memsliceApplication, memNode);
            scheduler.updateAllocation(memNode, memsliceApplication, 0, 1);

            // check cores didn't change
            assertEquals(usedCores + 1, scheduler.usedCores());
            assertEquals(usedCoresForNode, scheduler.usedCoresForNode(memNode));
            assertEquals(scheduler.usedCoresForApplicationOnNode(memsliceApplication, memNode), coresPerApplicationOnNode);

            // check memslices did change
            assertEquals(usedMemslices + 1, scheduler.usedMemslices());
            assertEquals(usedMemslicesForNode + 1, scheduler.usedMemslicesForNode(memNode));
            assertTrue(scheduler.usedMemslicesForNode(memNode) <= scheduler.memsliceCapacityForNode(memNode));
            assertEquals(scheduler.usedMemslicesForApplicationOnNode(memsliceApplication, memNode), memslicesPerApplicationOnNode + 1);
            
            // check overall capacity & runner state  
            assertTrue(scheduler.usedCores() + scheduler.usedMemslices() <= scheduler.coreCapacity() + scheduler.memsliceCapacity());
            assertEquals(scheduler.getNumPendingRequests(), 0);
            assertFalse(scheduler.checkForCapacityViolation());
        }
    }

    /* TODO: rewrite with simulation
    @Test
    public void testReleaseAllocation() throws Exception {
        final long NUM_NODES = 5;
        final long CORES_PER_NODE = 5;
        final long MEMSLICES_PER_NODE = 5;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        
        final Random rand = new Random(0xc0ffee);

        // Fill entire cluster
        runner.fillRandom(100);

        for (long application = 1; application <= NUM_APPS; application++) {
            for (long node = 1; node <= NUM_NODES; node++) {
                final long cores = scheduler.usedCoresForApplicationOnNode(application, node);
                final long memslices = scheduler.usedMemslicesForApplicationOnNode(application, node);
    
                final long coresToRelease;
                final long memslicesToRelease;
                if (0 == cores) {
                    coresToRelease = 0;
                } else {
                    coresToRelease = (long) rand.nextInt((int) cores + 1);
                }
                if (0 == memslices) {
                    memslicesToRelease = 0;
                } else {
                    memslicesToRelease = (long) rand.nextInt((int) memslices + 1);
                }

                final long initialCoreCapacity = scheduler.coreCapacity();
                final long initialMemsliceCapacity = scheduler.memsliceCapacity();
                final long initialUsedCores = scheduler.usedCores();
                final long initialUsedMemslices = scheduler.usedMemslices();
                final long initialCoresForApplication = scheduler.usedCoresForApplication(application);
                final long initialMemslicesForApplication = scheduler.usedMemslicesForApplication(application);
                final long initialCoresForNode = scheduler.usedCoresForNode(node);
                final long initialMemslicesForNode = scheduler.usedMemslicesForNode(node);
                scheduler.releaseAllocation(node, application, coresToRelease, memslicesToRelease);
                assertEquals(initialCoreCapacity, scheduler.coreCapacity());
                assertEquals(initialMemsliceCapacity, scheduler.memsliceCapacity());
                assertEquals(initialUsedCores - coresToRelease, scheduler.usedCores());
                assertEquals(initialUsedMemslices - memslicesToRelease, scheduler.usedMemslices());
                assertEquals(initialCoresForApplication - coresToRelease, scheduler.usedCoresForApplication(application));
                assertEquals(initialMemslicesForApplication - memslicesToRelease, scheduler.usedMemslicesForApplication(application));
                assertEquals(initialCoresForNode - coresToRelease, scheduler.usedCoresForNode(node));
                assertEquals(initialMemslicesForNode - memslicesToRelease, scheduler.usedMemslicesForNode(node));
                assertEquals(cores - coresToRelease, scheduler.usedCoresForApplicationOnNode(application, node));
                assertEquals(memslices - memslicesToRelease, scheduler.usedMemslicesForApplicationOnNode(application, node));
            }
        }
    }
    */

    @Test
    public void testApplication() throws Exception {
        final long NUM_NODES = 5;
        final long CORES_PER_NODE = 5;
        final long MEMSLICES_PER_NODE = 5;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        populateCluster(scheduler, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        scheduler.updateAllocation(1, 1, 1, 0);
        scheduler.updateAllocation(1, 1, 1, 0);
        assertEquals(1, scheduler.nodesForApplication(1));
        assertEquals(2, scheduler.usedCoresForApplication(1));
        assertEquals(0, scheduler.usedMemslicesForApplication(1));
        for (long i = 2; i <= NUM_APPS; i++) {
            assertEquals(0, scheduler.nodesForApplication(i));
            assertEquals(0, scheduler.usedCoresForApplication(i));
            assertEquals(0, scheduler.usedMemslicesForApplication(i));
        }

        scheduler.updateAllocation(2, 1, 1, 1);
        assertEquals(2, scheduler.nodesForApplication(1));
        assertEquals(3, scheduler.usedCoresForApplication(1));
        assertEquals(1, scheduler.usedMemslicesForApplication(1));
        for (long i = 2; i <= NUM_APPS; i++) {
            assertEquals(0, scheduler.nodesForApplication(i));
            assertEquals(0, scheduler.usedCoresForApplication(i));
            assertEquals(0, scheduler.usedMemslicesForApplication(i));
        }

        scheduler.updateAllocation(3, 2, 1, 1);
        assertEquals(2, scheduler.nodesForApplication(1));
        assertEquals(3, scheduler.usedCoresForApplication(1));
        assertEquals(1, scheduler.usedMemslicesForApplication(1));
        assertEquals(1, scheduler.nodesForApplication(2));
        assertEquals(1, scheduler.usedCoresForApplication(2));
        assertEquals(1, scheduler.usedMemslicesForApplication(2));
        for (int i = 3; i <= NUM_APPS; i++) {
            assertEquals(0, scheduler.nodesForApplication(i));
            assertEquals(0, scheduler.usedCoresForApplication(i));
            assertEquals(0, scheduler.usedMemslicesForApplication(i));
        }
    }

    @Test
    public void testUpdateAllocationMulti() throws Exception {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 4;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        RandomDataGenerator rand = new RandomDataGenerator(new JDKRandomGenerator(0xc0ffee));
        populateCluster(scheduler, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        // Request two cores
        long node = 1;
        final long usedCores = scheduler.usedCores();
        final long usedCoresForNode = scheduler.usedCoresForNode(node);
        assertEquals(usedCores, 0);
        scheduler.updateAllocation(node, rand.nextLong(1, NUM_APPS), 2, 0);
        assertEquals(usedCores + 2, scheduler.usedCores());
        assertEquals(usedCoresForNode + 2, scheduler.usedCoresForNode(node));
        assertEquals(0, scheduler.usedMemslices());
        assertEquals(0, scheduler.usedMemslicesForNode(node));
        assertEquals(scheduler.getNumPendingRequests(), 0);
        assertFalse(scheduler.checkForCapacityViolation());

        // Request 3 memslices
        node = 2;
        final long usedMemslices = scheduler.usedMemslices();
        final long usedMemslicesForNode = scheduler.usedMemslicesForNode(node);
        assertEquals(usedCores, 0);
        scheduler.updateAllocation(node, rand.nextLong(1, NUM_APPS), 0, 3);
        assertEquals(usedCores + 2, scheduler.usedCores());
        assertEquals(0, scheduler.usedCoresForNode(node));
        assertEquals(usedMemslices + 3, scheduler.usedMemslices());
        assertEquals(usedMemslicesForNode + 3, scheduler.usedMemslicesForNode(node));
        assertEquals(scheduler.getNumPendingRequests(), 0);
        assertFalse(scheduler.checkForCapacityViolation());

        // Request 2 cores and 4 memslices
        node = 1;
        assertEquals(usedCores, 0);
        scheduler.updateAllocation(node, rand.nextLong(1, NUM_APPS), 2, 4);
        assertEquals(usedCores + 4, scheduler.usedCores());
        assertEquals(usedCoresForNode + 4, scheduler.usedCoresForNode(node));
        assertEquals(usedMemslices + 7, scheduler.usedMemslices());
        assertEquals(usedMemslicesForNode + 4, scheduler.usedMemslicesForNode(node));
        assertEquals(scheduler.getNumPendingRequests(), 0);
        assertFalse(scheduler.checkForCapacityViolation());
    }

    @Test 
    public void testCheckCapacityViolation() throws Exception {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 4;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        RandomDataGenerator rand = new RandomDataGenerator(new JDKRandomGenerator(0xc0ffee));
        populateCluster(scheduler, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        // check with empty cluster
        assertFalse(scheduler.checkForCapacityViolation());

        // Fill the first node
        scheduler.updateAllocation(1, rand.nextLong(1, NUM_APPS), CORES_PER_NODE, MEMSLICES_PER_NODE);
        assertFalse(scheduler.checkForCapacityViolation());

        // Overfill the first node
        scheduler.updateAllocation(1, rand.nextLong(1, NUM_APPS), 1, 0);
        assertTrue(scheduler.checkForCapacityViolation());
    }

    @Test
    public void testRequestAndSolve() throws Exception {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Solver solver = new DiNOSSolver(conn, true, true, false);
        Scheduler scheduler = new Scheduler(conn, solver);
        populateCluster(scheduler, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        assertEquals(scheduler.getNumPendingRequests(), 0);
        assertEquals(scheduler.usedCores() + scheduler.usedMemslices(), 0);

        // Add a request
        for (long i = 1; i <= scheduler.coreCapacity() + scheduler.memsliceCapacity(); i++) {

            // Generate a random request
            if (i <= scheduler.coreCapacity()) {
                scheduler.generateRequest(null, 1, 0, (i % NUM_APPS) + 1);
            } else {
                scheduler.generateRequest(null, 0, 1, (i % NUM_APPS) + 1);
            }
            assertEquals(1, scheduler.getNumPendingRequests());

            // Fulfull the request
            assertTrue(scheduler.runSolverAndUpdateDB(false));

            // Check used resources is correct
            assertEquals(i, scheduler.usedCores() + scheduler.usedMemslices());
            if (i <= scheduler.coreCapacity()) {
                assertEquals(i, scheduler.usedCores());
                assertEquals(0, scheduler.usedMemslices());

                int aggregatedUsedCores = 0;
                for (long j = 1; j <= NUM_NODES; j++) {
                    aggregatedUsedCores += scheduler.usedCoresForNode(j);
                    assertEquals(0, scheduler.usedMemslicesForNode(j));
                }  
                assertEquals(i, aggregatedUsedCores);

            } else {
                assertEquals(i - scheduler.coreCapacity(), scheduler.usedMemslices());
                assertEquals(scheduler.coreCapacity(), scheduler.usedCores());

                int aggregatedUsedMemslices = 0;
                for (long j = 1; j <= NUM_NODES; j++) {
                    aggregatedUsedMemslices += scheduler.usedMemslicesForNode(j);
                }  
                assertEquals(i - scheduler.coreCapacity(), aggregatedUsedMemslices);         
            }
            
            // Check capacity
            assertFalse(scheduler.checkForCapacityViolation());
        }

        // All nodes should be at full capacity now
        for (int i = 1; i <= NUM_NODES; i++) {
            assertEquals(CORES_PER_NODE, scheduler.usedCoresForNode(i));
            assertEquals(MEMSLICES_PER_NODE, scheduler.usedMemslicesForNode(i));
        }
    }

    @Test
    public void testGenerateRequestsAndSolve() throws Exception {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Solver solver = new DiNOSSolver(conn, true, true, false);
        Scheduler scheduler = new Scheduler(conn, solver);
        populateCluster(scheduler, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        assertEquals(scheduler.getNumPendingRequests(), 0);
        assertEquals(scheduler.usedCores() + scheduler.usedMemslices(), 0);

        // Generate a couple of core requests
        scheduler.generateRequests(0L, 3, 0, 1);
        assertEquals(scheduler.getNumPendingRequests(), 3);

        // Fulfull the request
        assertTrue(scheduler.runSolverAndUpdateDB(false));
        assertEquals(scheduler.usedCores(), 3);
        assertEquals(scheduler.usedMemslices(), 0);
        assertEquals(scheduler.getNumPendingRequests(), 0);

        // Generate a couple of memslice requests
        scheduler.generateRequests(null, 0, 2, 1);
        assertEquals(scheduler.getNumPendingRequests(), 2);

        // Fulfull the request
        assertTrue(scheduler.runSolverAndUpdateDB(false));
        assertEquals(scheduler.usedCores(), 3);
        assertEquals(scheduler.usedMemslices(), 2);
        assertEquals(scheduler.getNumPendingRequests(), 0);

        // Generate a mix of requests
        scheduler.generateRequests(10L, 2, 2, 1);
        assertEquals(scheduler.getNumPendingRequests(), 4);
        
        // Fulfull the request
        assertTrue(scheduler.runSolverAndUpdateDB(false));
        assertEquals(scheduler.usedCores(), 5);
        assertEquals(scheduler.usedMemslices(), 4);
        assertEquals(scheduler.getNumPendingRequests(), 0);
    }

    /* TODO: add to simulator test
    @Test
    public void testFillRandomUtil() throws Exception {
        final long NUM_NODES = 10;
        final long CORES_PER_NODE = 10;
        final long MEMSLICES_PER_NODE = 20;
        final long NUM_APPS = 10;

        // Create database
        DSLContext conn = DBUtils.getConn();
        DCMScheduler runner1 = new DCMScheduler(conn1, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        runner1.fillRandom(100);

        // Make sure filled all the way.
        assertEquals(runner1.coreCapacity(), runner1.usedCores());
        assertEquals(runner1.memsliceCapacity(), runner1.usedMemslices());
    }

    @Test
    public void testFillRandomDistribution() throws Exception {
        final long NUM_NODES = 10;
        final long CORES_PER_NODE = 10;
        final long MEMSLICES_PER_NODE = 20;
        final long NUM_APPS = 10;

        // Test randomness of fill per node.
        // Get the average of averages of number of cores on a node each iter.
        int[] aggregate_core_fill = new int[(int) NUM_NODES];
        int[] aggregate_memslice_fill = new int[(int) NUM_NODES];

        int[] aggregate_core_per_app = new int[(int) NUM_APPS];
        int[] aggregate_memslice_per_app = new int[(int) NUM_APPS];

        final int ITERS = 100;
        final int FILL_UTIL = 50;

        for (long i = 0; i < ITERS; i++) {
            // Create database
            DSLContext conn = DBUtils.getConn();
            DCMScheduler runner = new DCMScheduler(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
                NUM_APPS, null, false, false);

            // This calls check fill - so some checks just from calling it.
            runner.fillRandom(FILL_UTIL);
            assertEquals((runner.coreCapacity() * FILL_UTIL) / 100, runner.usedCores());
            assertEquals((runner.memsliceCapacity() * FILL_UTIL) / 100, runner.usedMemslices());
        
            for (int j = 1; j <= NUM_NODES; j++) {
                aggregate_core_fill[j - 1] += runner.usedCoresForNode((long) j);
                aggregate_memslice_fill[j - 1] += runner.usedMemslicesForNode((long) j);
            }

            for (int j = 1; j <= NUM_APPS; j++) {
                aggregate_core_per_app[j - 1] += runner.usedCoresForApplication((long) j);
                aggregate_memslice_per_app[j - 1] += runner.usedMemslicesForApplication((long) j);
            }
        }

        float avg_core_fill = 0;
        float avg_memslice_fill = 0;
        for (int j = 1; j <= NUM_NODES; j++) {
            avg_core_fill += (float) aggregate_core_fill[j - 1] / (float) ITERS;
            avg_memslice_fill += (float) aggregate_memslice_fill[j - 1] / (float) ITERS;
        }
        avg_core_fill /= (float) NUM_NODES;
        avg_memslice_fill /= (float) NUM_NODES;

        // On average, each node should be filled with cores and memslices to FILL_UTIL.
        // Since it's random, there's a chance this will fail and everything is okay.
        assertEquals(avg_core_fill, CORES_PER_NODE * ((float) FILL_UTIL / 100.0), 1.0);
        assertEquals(avg_memslice_fill, MEMSLICES_PER_NODE * ((float) FILL_UTIL / 100.0), 1.0);

        float avg_core_for_app = 0;
        float avg_memslice_for_app = 0;
        for (int j = 1; j <= NUM_APPS; j++) {
            avg_core_for_app += (float) aggregate_core_per_app[j - 1] / (float) ITERS;
            avg_memslice_for_app += (float) aggregate_memslice_per_app[j - 1] / (float) ITERS;
        }
        avg_core_for_app /= (float) NUM_APPS;
        avg_memslice_for_app /= (float) NUM_APPS;

        // On average, each application should receive (TOTAL_CLUSTER_RESOURCE * FILL%) / NUM_APPS
        // Since it's random, there's a chance this will fail and everything is okay.
        assertEquals(avg_core_for_app, 
                CORES_PER_NODE * NUM_NODES * ((float) FILL_UTIL / 100.0) / (float) NUM_APPS, 1.0);
        assertEquals(avg_memslice_for_app, 
                MEMSLICES_PER_NODE * NUM_NODES * ((float) FILL_UTIL / 100.0) / (float) NUM_APPS, 1.0);
    }

    @Test
    public void testFillSingleStep() throws Exception {
        final long NUM_NODES = 4;
        final long CORES_PER_NODE = 4;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 4;

        final long ITERS = 10;
        final int FILL_UTIL = 50;

        int[] aggregate_core_per_app = new int[(int) NUM_APPS];
        int[] aggregate_memslice_per_app = new int[(int) NUM_APPS];

        for (int i = 0; i <= ITERS; i++) {
            // Create database
            DSLContext conn = DBUtils.getConn();
            DCMScheduler runner = new DCMScheduler(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
                NUM_APPS, null, false, false);

            runner.fillSingleStep(FILL_UTIL);

            // Make sure filled half way, assume capacities are even
            assertEquals(runner.coreCapacity() / 2, runner.usedCores());
            assertEquals(runner.memsliceCapacity() / 2, runner.usedMemslices());

            for (int j = 1; j <= NUM_APPS; j++) {
                aggregate_core_per_app[j - 1] += runner.usedCoresForApplication((long) j);
                aggregate_memslice_per_app[j - 1] += runner.usedMemslicesForApplication((long) j);
            }
        }

        float avg_core_for_app = 0;
        float avg_memslice_for_app = 0;
        for (int j = 1; j <= NUM_APPS; j++) {
            avg_core_for_app += (float) aggregate_core_per_app[j - 1] / (float) ITERS;
            avg_memslice_for_app += (float) aggregate_memslice_per_app[j - 1] / (float) ITERS;
        }
        avg_core_for_app /= (float) NUM_APPS;
        avg_memslice_for_app /= (float) NUM_APPS;

        // On average, each application should receive (TOTAL_CLUSTER_RESOURCE * FILL%) / NUM_APPS
        // Since it's random, there's a chance this will fail and everything is okay.
        assertEquals(avg_core_for_app, 
                CORES_PER_NODE * NUM_NODES * ((float) FILL_UTIL / 100.0) / (float) NUM_APPS, 1.0);
        assertEquals(avg_memslice_for_app, 
                MEMSLICES_PER_NODE * NUM_NODES * ((float) FILL_UTIL / 100.0) / (float) NUM_APPS, 1.0);
    }

    @Test
    public void testFillPoissonUtil() throws Exception {
        final long NUM_NODES = 10;
        final long CORES_PER_NODE = 10;
        final long MEMSLICES_PER_NODE = 20;
        final long NUM_APPS = 10;

        // Test randomness of fill per node.
        int aggregate_core_fill = 0;
        int aggregate_memslice_fill = 0;

        final long ITERS = 10;
        final int FILL_UTIL = 50;

        // Assume mean requests is for 20% of the cluster per step. This is set high
        // to reduce the number of steps needed to meet the fill.
        final double CORE_MEAN = (double) CORES_PER_NODE * 0.2;
        final double MEMSLICE_MEAN = (double) MEMSLICES_PER_NODE * 0.2;

        int[] aggregate_core_per_app = new int[(int) NUM_APPS];
        int[] aggregate_memslice_per_app = new int[(int) NUM_APPS];

        for (int i = 0; i < ITERS; i++) {
            // Create database
            DSLContext conn = DBUtils.getConn();
            DCMScheduler runner = new DCMScheduler(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
                NUM_APPS, null, false, false);

            // This calls check fill - so some checks just from calling it.
            runner.fillPoisson(FILL_UTIL, CORE_MEAN, MEMSLICE_MEAN);
        
            aggregate_core_fill += runner.usedCores();
            aggregate_memslice_fill += runner.usedMemslices();

            for (int j = 1; j <= NUM_APPS; j++) {
                aggregate_core_per_app[j - 1] += runner.usedCoresForApplication((long) j);
                aggregate_memslice_per_app[j - 1] += runner.usedMemslicesForApplication((long) j);
            }
        }
        float avg_core_fill = ((float) aggregate_core_fill) / ((float) ITERS);
        float avg_memslice_fill = ((float) aggregate_memslice_fill) / ((float) ITERS);

        // On average, each node should be filled with cores and memslices to FILL_UTIL.
        // allow for error equal to mean (standard deviation of poisson)
        assertEquals(avg_core_fill, NUM_NODES * CORES_PER_NODE * ((float) FILL_UTIL / 100.0), CORE_MEAN);
        assertEquals(avg_memslice_fill, NUM_NODES * MEMSLICES_PER_NODE * ((float) FILL_UTIL / 100.0), MEMSLICE_MEAN);

        float avg_core_for_app = 0;
        float avg_memslice_for_app = 0;
        for (int j = 1; j <= NUM_APPS; j++) {
            avg_core_for_app += (float) aggregate_core_per_app[j - 1] / (float) ITERS;
            avg_memslice_for_app += (float) aggregate_memslice_per_app[j - 1] / (float) ITERS;
        }
        avg_core_for_app /= (float) NUM_APPS;
        avg_memslice_for_app /= (float) NUM_APPS;

        // On average, each application should receive (TOTAL_CLUSTER_RESOURCE * FILL%) / NUM_APPS
        // Since it's random, there's a chance this will fail and everything is okay.
        assertEquals(avg_core_for_app, 
                CORES_PER_NODE * NUM_NODES * ((float) FILL_UTIL / 100.0) / (float) NUM_APPS, 1.0);
        assertEquals(avg_memslice_for_app, 
                MEMSLICES_PER_NODE * NUM_NODES * ((float) FILL_UTIL / 100.0) / (float) NUM_APPS, 1.0);
    }
    */
}
