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

    @Test
    public void testNothingToSolve() throws Exception {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Solver solver = new DiNOSSolver(conn, true, false);
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
        Solver solver = new DiNOSSolver(conn, true, false);
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
        Solver solver = new DiNOSSolver(conn, true, false);
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
}
