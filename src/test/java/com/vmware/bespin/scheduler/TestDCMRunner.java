/*
 * Copyright 2023 University of Colorado and VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler;

import org.junit.jupiter.api.Test; 
import static org.junit.jupiter.api.Assertions.*;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;


public class TestDCMRunner {
    @Test
    public void testRunnerInstantiationZero() throws ClassNotFoundException {
        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, 0, 0, 0, 
            0, 0, false, false);

        // Check number of nodes
        assertEquals(runner.numNodes(), runner.actualNumNodes());
        assertEquals(0, runner.numNodes());
        
        // Check core capacity and use
        assertEquals(runner.coreCapacity(), runner.actualCoreCapacity());
        assertEquals(0, runner.coreCapacity());
        assertEquals(0, runner.usedCores());

        // Check memslice capacity
        assertEquals(runner.memsliceCapacity(), runner.actualMemsliceCapacity());
        assertEquals(0, runner.memsliceCapacity());
        assertEquals(0, runner.usedMemslices());

        // Check number of applications
        assertEquals(runner.numApps(), runner.actualNumApps());
        assertEquals(0, runner.numApps());

        // Check pending requests
        assertEquals(0, runner.getNumPendingRequests());

        // Check capacity
        assertFalse(runner.checkForCapacityViolation());
    }

    @Test
    public void testRunnerInstantiation() throws ClassNotFoundException {
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 3;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, 0, false, false);

        // Check number of nodes
        assertEquals(runner.actualNumNodes(), runner.numNodes());
        assertEquals(runner.numNodes(), NUM_NODES);
        
        // Check core capacity and use
        assertEquals(runner.actualCoreCapacity(), runner.coreCapacity());
        assertEquals(runner.coreCapacity(), NUM_NODES * CORES_PER_NODE);
        assertEquals(runner.usedCores(), 0);

        // Check memslice capacity
        assertEquals(runner.actualMemsliceCapacity(), runner.memsliceCapacity());
        assertEquals(runner.memsliceCapacity(), NUM_NODES * MEMSLICES_PER_NODE);
        assertEquals(runner.usedMemslices(), 0);

        // Check per-node valudes
        for (int i = 1; i <= NUM_NODES; i++) {
            assertEquals(runner.actualCoreCapacityForNode(i), CORES_PER_NODE);
            assertEquals(runner.usedCoresForNode(i), 0);
            assertEquals(runner.actualMemsliceCapacityForNode(i), MEMSLICES_PER_NODE);
            assertEquals(runner.usedMemslicesForNode(i), 0);
        }

        // Check number of applications
        assertEquals(runner.actualNumApps(), runner.numApps());
        assertEquals(runner.numApps(), NUM_APPS);

        // Check pending requests
        assertEquals(runner.getNumPendingRequests(), 0);

        // Check capacity
        assertFalse(runner.checkForCapacityViolation());
    }

    @Test
    public void testAddNode() throws ClassNotFoundException {
        int numNodes = 2;
        final int CORES_PER_NODE = 3;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, numNodes, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, 0, false, false);

        // Check number of nodes
        assertEquals(runner.actualNumNodes(), runner.numNodes());
        assertEquals(runner.numNodes(), numNodes);
        
        // Check core capacity and use
        assertEquals(runner.actualCoreCapacity(), runner.coreCapacity());
        assertEquals(runner.coreCapacity(), numNodes * CORES_PER_NODE);
        assertEquals(runner.usedCores(), 0);

        // Check memslice capacity
        assertEquals(runner.actualMemsliceCapacity(), runner.memsliceCapacity());
        assertEquals(runner.memsliceCapacity(), numNodes * MEMSLICES_PER_NODE);
        assertEquals(runner.usedMemslices(), 0);

        // Check per-node values
        for (int i = 1; i <= numNodes; i++) {
            assertEquals(runner.actualCoreCapacityForNode(i), CORES_PER_NODE);
            assertEquals(runner.usedCoresForNode(i), 0);
            assertEquals(runner.actualMemsliceCapacityForNode(i), MEMSLICES_PER_NODE);
            assertEquals(runner.usedMemslicesForNode(i), 0);
        }

        // Check number of applications
        assertEquals(runner.actualNumApps(), runner.numApps());
        assertEquals(runner.numApps(), NUM_APPS);

        assertFalse(runner.checkForCapacityViolation());

        numNodes += 1;
        runner.addNode(numNodes, CORES_PER_NODE, MEMSLICES_PER_NODE);
        
        // Check number of nodes
        assertEquals(runner.actualNumNodes(), runner.numNodes());
        assertEquals(runner.numNodes(), numNodes);
                
        // Check core capacity and use
        assertEquals(runner.actualCoreCapacity(), runner.coreCapacity());
        assertEquals(runner.coreCapacity(), numNodes * CORES_PER_NODE);
        assertEquals(runner.usedCores(), 0);
        
        // Check memslice capacity
        assertEquals(runner.actualMemsliceCapacity(), runner.memsliceCapacity());
        assertEquals(runner.memsliceCapacity(), numNodes * MEMSLICES_PER_NODE);
        assertEquals(runner.usedMemslices(), 0);
        
        // Check per-node values
        for (int i = 1; i <= numNodes; i++) {
            assertEquals(runner.actualCoreCapacityForNode(i), CORES_PER_NODE);
            assertEquals(runner.usedCoresForNode(i), 0);
            assertEquals(runner.actualMemsliceCapacityForNode(i), MEMSLICES_PER_NODE);
            assertEquals(runner.usedMemslicesForNode(i), 0);
        }
        
        // Check number of applications
        assertEquals(runner.actualNumApps(), runner.numApps());
        assertEquals(runner.numApps(), NUM_APPS);
        
        assertFalse(runner.checkForCapacityViolation());
    }

    @Test
    public void testAddApp() throws ClassNotFoundException {
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 3;
        final int MEMSLICES_PER_NODE = 4;
        int numApps = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            numApps, 0, false, false);

        // Check number of applications
        assertEquals(runner.actualNumApps(), runner.numApps());
        assertEquals(runner.numApps(), numApps);

        numApps += 1;
        runner.addApplication(numApps);

        // Check number of applications
        assertEquals(runner.actualNumApps(), runner.numApps());
        assertEquals(runner.numApps(), numApps);
    }

    @Test
    public void testChooseRandomApplication() throws ClassNotFoundException {
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 3;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        // Check number of applications
        assertEquals(runner.actualNumApps(), runner.numApps());
        assertEquals(runner.numApps(), NUM_APPS);

        // Check number of applications
        for (int i = 0; i < 10; i++) {
            final int randomApp = runner.chooseRandomApplication();
            assert(randomApp >= 1 && randomApp <= NUM_APPS);
        }

        // Check number of applications
        assertEquals(runner.actualNumApps(), runner.numApps());
        assertEquals(runner.numApps(), NUM_APPS);
    }

    @Test
    public void testCoresForUtil() throws ClassNotFoundException {
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 3;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
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
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 3;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        // check cores for util at 100%
        assertEquals(runner.memslicesForUtil(100), runner.memsliceCapacity());
        assertEquals(runner.memslicesForUtil(100), NUM_NODES * MEMSLICES_PER_NODE);

        // check cores for util at 50%
        assertEquals(runner.memslicesForUtil(50), runner.memsliceCapacity() / 2);        
        assertEquals(runner.memslicesForUtil(50), (NUM_NODES * MEMSLICES_PER_NODE) / 2);
    }

    @Test
    public void testPendingRequests() throws ClassNotFoundException {
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 3;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        // Add a request
        for (int i = 0; i < runner.coreCapacity() + runner.memsliceCapacity(); i++) {
            assertEquals(runner.getNumPendingRequests(), i);
            if (i < runner.coreCapacity()) {
                runner.generateRequest(1, 0, (i % NUM_APPS) + 1);
            } else {
                runner.generateRequest(0, 1, (i % NUM_APPS) + 1);
            }
        }
    }

    @Test
    public void testPendingRandomRequests() throws ClassNotFoundException {
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 3;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        // Add a request
        for (int i = 0; i < runner.coreCapacity() + runner.memsliceCapacity(); i++) {
            assertEquals(runner.getNumPendingRequests(), i);
            runner.generateRandomRequest();
        }
    }

    @Test
    public void testNothingToSolve() throws Exception {
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 3;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        assertEquals(runner.getNumPendingRequests(), 0);
        assertEquals(runner.usedCores() + runner.usedMemslices(), 0);

        // No requests to fulfill
        assertFalse(runner.runModelAndUpdateDB(false));

        assertEquals(runner.getNumPendingRequests(), 0);
        assertEquals(runner.usedCores() + runner.usedMemslices(), 0);
    }

    @Test
    public void testUpdateAllocation() throws Exception {
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 4;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        // Below code is dependent on NUM_NODE==2, and CORES_PER_NODE==MEMSLICES_PER_NODE
        for (int i = 0; i < (runner.coreCapacity() + runner.memsliceCapacity()) / 2; i++) {
            final int coreNode = (i % 2) + 1;
            final int memNode = ((i + 1) % 2) + 1;

            // Ensure this does nothing
            final int usedCores = runner.usedCores();
            final int usedMemslices = runner.usedMemslices();
            int usedCoresForNode = runner.usedCoresForNode(coreNode);
            int usedMemslicesForNode = runner.usedMemslicesForNode(coreNode);
            
            // add nothing
            runner.updateAllocation(coreNode, runner.chooseRandomApplication(), 0, 0);
            
            // check cores
            assertEquals(usedCores, runner.usedCores());
            assertEquals(usedCoresForNode, runner.usedCoresForNode(coreNode));
            
            // check memslices
            assertEquals(usedMemslices, runner.usedMemslices());
            assertEquals(usedMemslicesForNode, runner.usedMemslicesForNode(coreNode));

            // add a core
            runner.updateAllocation(coreNode, runner.chooseRandomApplication(), 1, 0);

            // check cores
            assertEquals(usedCores + 1, runner.usedCores());
            assertEquals(usedCoresForNode + 1, runner.usedCoresForNode(coreNode));
            assertTrue(runner.coresPerNode == CORES_PER_NODE);
            assertTrue(runner.usedCoresForNode(coreNode) <= runner.coresPerNode);

            // check memslices
            assertEquals(usedMemslices, runner.usedMemslices());
            assertEquals(usedMemslicesForNode, runner.usedMemslicesForNode(coreNode));
            assertTrue(runner.usedMemslicesForNode(coreNode) <= runner.memslicesPerNode);

            // switch nodes
            usedCoresForNode = runner.usedCoresForNode(memNode);
            usedMemslicesForNode = runner.usedMemslicesForNode(memNode);
            
            // add a memslice
            runner.updateAllocation(memNode, runner.chooseRandomApplication(), 0, 1);

            // check cores didn't change
            assertEquals(usedCores + 1, runner.usedCores());
            assertEquals(usedCoresForNode, runner.usedCoresForNode(memNode));

            // check memslices
            assertEquals(usedMemslices + 1, runner.usedMemslices());
            assertEquals(usedMemslicesForNode + 1, runner.usedMemslicesForNode(memNode));
            assertTrue(runner.usedMemslicesForNode(memNode) <= runner.memslicesPerNode);

            // check overall capacity & runner state  
            assertTrue(runner.usedCores() + runner.usedMemslices() <= runner.coreCapacity() + runner.memsliceCapacity());
            assertEquals(runner.getNumPendingRequests(), 0);
            assertFalse(runner.checkForCapacityViolation());
        }
    }

    @Test
    public void testApplication() throws Exception {
        final int NUM_NODES = 5;
        final int CORES_PER_NODE = 5;
        final int MEMSLICES_PER_NODE = 5;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        runner.updateAllocation(1, 1, 1, 0);
        runner.updateAllocation(1, 1, 1, 0);
        assertEquals(1, runner.nodesForApplication(1));
        assertEquals(2, runner.usedCoresForApplication(1));
        assertEquals(0, runner.usedMemslicesForApplication(1));
        for (int i = 2; i <= NUM_APPS; i++) {
            assertEquals(0, runner.nodesForApplication(i));
            assertEquals(0, runner.usedCoresForApplication(i));
            assertEquals(0, runner.usedMemslicesForApplication(i));
        }

        runner.updateAllocation(2, 1, 1, 1);
        assertEquals(2, runner.nodesForApplication(1));
        assertEquals(3, runner.usedCoresForApplication(1));
        assertEquals(1, runner.usedMemslicesForApplication(1));
        for (int i = 2; i <= NUM_APPS; i++) {
            assertEquals(0, runner.nodesForApplication(i));
            assertEquals(0, runner.usedCoresForApplication(i));
            assertEquals(0, runner.usedMemslicesForApplication(i));
        }

        runner.updateAllocation(3, 2, 1, 1);
        assertEquals(2, runner.nodesForApplication(1));
        assertEquals(3, runner.usedCoresForApplication(1));
        assertEquals(1, runner.usedMemslicesForApplication(1));
        assertEquals(1, runner.nodesForApplication(2));
        assertEquals(1, runner.usedCoresForApplication(2));
        assertEquals(1, runner.usedMemslicesForApplication(2));
        for (int i = 3; i <= NUM_APPS; i++) {
            assertEquals(0, runner.nodesForApplication(i));
            assertEquals(0, runner.usedCoresForApplication(i));
            assertEquals(0, runner.usedMemslicesForApplication(i));
        }
    }

    @Test
    public void testUpdateAllocationMulti() throws Exception {
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 4;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        // Request two cores
        int node = 1;
        final int usedCores = runner.usedCores();
        final int usedCoresForNode = runner.usedCoresForNode(node);
        assertEquals(usedCores, 0);
        runner.updateAllocation(node, runner.chooseRandomApplication(), 2, 0);
        assertEquals(usedCores + 2, runner.usedCores());
        assertEquals(usedCoresForNode + 2, runner.usedCoresForNode(node));
        assertEquals(0, runner.usedMemslices());
        assertEquals(0, runner.usedMemslicesForNode(node));
        assertEquals(runner.getNumPendingRequests(), 0);
        assertFalse(runner.checkForCapacityViolation());

        // Request 3 memslices
        node = 2;
        final int usedMemslices = runner.usedMemslices();
        final int usedMemslicesForNode = runner.usedMemslicesForNode(node);
        assertEquals(usedCores, 0);
        runner.updateAllocation(node, runner.chooseRandomApplication(), 0, 3);
        assertEquals(usedCores + 2, runner.usedCores());
        assertEquals(0, runner.usedCoresForNode(node));
        assertEquals(usedMemslices + 3, runner.usedMemslices());
        assertEquals(usedMemslicesForNode + 3, runner.usedMemslicesForNode(node));
        assertEquals(runner.getNumPendingRequests(), 0);
        assertFalse(runner.checkForCapacityViolation());

        // Request 2 cores and 4 memslices
        node = 1;
        assertEquals(usedCores, 0);
        runner.updateAllocation(node, runner.chooseRandomApplication(), 2, 4);
        assertEquals(usedCores + 4, runner.usedCores());
        assertEquals(usedCoresForNode + 4, runner.usedCoresForNode(node));
        assertEquals(usedMemslices + 7, runner.usedMemslices());
        assertEquals(usedMemslicesForNode + 4, runner.usedMemslicesForNode(node));
        assertEquals(runner.getNumPendingRequests(), 0);
        assertFalse(runner.checkForCapacityViolation());
    }

    @Test 
    public void testCheckCapacityViolation() throws Exception {
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 4;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        // check with empty cluster
        assertFalse(runner.checkForCapacityViolation());

        // Fill the first node
        runner.updateAllocation(1, runner.chooseRandomApplication(), CORES_PER_NODE, MEMSLICES_PER_NODE);
        assertFalse(runner.checkForCapacityViolation());

        // Overfill the first node
        runner.updateAllocation(1, runner.chooseRandomApplication(), 1, 0);
        assertTrue(runner.checkForCapacityViolation());
    }

    @Test
    public void testRequestAndSolve() throws Exception {
        final int NUM_NODES = 2;
        final int CORES_PER_NODE = 3;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        assertEquals(runner.getNumPendingRequests(), 0);
        assertEquals(runner.usedCores() + runner.usedMemslices(), 0);

        // Add a request
        for (int i = 1; i <= runner.coreCapacity() + runner.memsliceCapacity(); i++) {

            // Generate a random request
            if (i <= runner.coreCapacity()) {
                runner.generateRequest(1, 0, (i % NUM_APPS) + 1);
            } else {
                runner.generateRequest(0, 1, (i % NUM_APPS) + 1);
            }
            assertEquals(1, runner.getNumPendingRequests());

            // Fulfull the request
            assertTrue(runner.runModelAndUpdateDB(false));

            // Check used resources is correct
            assertEquals(i, runner.usedCores() + runner.usedMemslices());
            if (i <= runner.coreCapacity()) {
                assertEquals(i, runner.usedCores());
                assertEquals(0, runner.usedMemslices());

                int aggregatedUsedCores = 0;
                for (int j = 1; j <= NUM_NODES; j++) {
                    aggregatedUsedCores += runner.usedCoresForNode(j);
                    assertEquals(0, runner.usedMemslicesForNode(j));
                }  
                assertEquals(i, aggregatedUsedCores);

            } else {
                assertEquals(i - runner.coreCapacity(), runner.usedMemslices());
                assertEquals(runner.coreCapacity(), runner.usedCores());
                assertEquals(runner.actualCoreCapacity(), runner.usedCores());

                int aggregatedUsedMemslices = 0;
                for (int j = 1; j <= NUM_NODES; j++) {
                    aggregatedUsedMemslices += runner.usedMemslicesForNode(j);
                }  
                assertEquals(i - runner.coreCapacity(), aggregatedUsedMemslices);         
            }
            
            // Check capacity
            assertFalse(runner.checkForCapacityViolation());
        }

        // All nodes should be at full capacity now
        for (int i = 1; i <= NUM_NODES; i++) {
            assertEquals(CORES_PER_NODE, runner.usedCoresForNode(i));
            assertEquals(MEMSLICES_PER_NODE, runner.usedMemslicesForNode(i));
        }
    }

    @Test
    public void testFillRandomUtil() throws Exception {
        final int NUM_NODES = 10;
        final int CORES_PER_NODE = 10;
        final int MEMSLICES_PER_NODE = 20;
        final int NUM_APPS = 10;

        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn1 = DSL.using("jdbc:h2:mem:");
        DCMRunner runner1 = new DCMRunner(conn1, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
            NUM_APPS, null, false, false);

        runner1.fillRandom(100);

        // Make sure filled all the way.
        assertEquals(runner1.coreCapacity(), runner1.usedCores());
        assertEquals(runner1.memsliceCapacity(), runner1.usedMemslices());
    }

    @Test
    public void testFillRandomDistribution() throws Exception {
        final int NUM_NODES = 10;
        final int CORES_PER_NODE = 10;
        final int MEMSLICES_PER_NODE = 20;
        final int NUM_APPS = 10;

        // Test randomness of fill per node.
        // Get the average of averages of number of cores on a node each iter.
        int[] aggregate_core_fill = new int[NUM_NODES];
        int[] aggregate_memslice_fill = new int[NUM_NODES];

        int[] aggregate_core_per_app = new int[NUM_APPS];
        int[] aggregate_memslice_per_app = new int[NUM_APPS];

        final int ITERS = 100;
        final int FILL_UTIL = 50;

        for (int i = 0; i < ITERS; i++) {
            // Create database
            Class.forName("org.h2.Driver");
            DSLContext conn = DSL.using("jdbc:h2:mem:");
            DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
                NUM_APPS, null, false, false);

            // This calls check fill - so some checks just from calling it.
            runner.fillRandom(FILL_UTIL);
            assertEquals((runner.coreCapacity() * FILL_UTIL) / 100, runner.usedCores());
            assertEquals((runner.memsliceCapacity() * FILL_UTIL) / 100, runner.usedMemslices());
        
            for (int j = 1; j <= NUM_NODES; j++) {
                aggregate_core_fill[j - 1] += runner.usedCoresForNode(j);
                aggregate_memslice_fill[j - 1] += runner.usedMemslicesForNode(j);
            }

            for (int j = 1; j <= NUM_APPS; j++) {
                aggregate_core_per_app[j - 1] += runner.usedCoresForApplication(j);
                aggregate_memslice_per_app[j - 1] += runner.usedMemslicesForApplication(j);
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
        final int NUM_NODES = 4;
        final int CORES_PER_NODE = 4;
        final int MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 4;

        final int ITERS = 10;
        final int FILL_UTIL = 50;

        int[] aggregate_core_per_app = new int[NUM_APPS];
        int[] aggregate_memslice_per_app = new int[NUM_APPS];

        for (int i = 0; i <= ITERS; i++) {
            // Create database
            Class.forName("org.h2.Driver");
            DSLContext conn = DSL.using("jdbc:h2:mem:");
            DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
                NUM_APPS, null, false, false);

            runner.fillSingleStep(FILL_UTIL);

            // Make sure filled half way, assume capacities are even
            assertEquals(runner.coreCapacity() / 2, runner.usedCores());
            assertEquals(runner.memsliceCapacity() / 2, runner.usedMemslices());

            for (int j = 1; j <= NUM_APPS; j++) {
                aggregate_core_per_app[j - 1] += runner.usedCoresForApplication(j);
                aggregate_memslice_per_app[j - 1] += runner.usedMemslicesForApplication(j);
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
        final int NUM_NODES = 10;
        final int CORES_PER_NODE = 10;
        final int MEMSLICES_PER_NODE = 20;
        final int NUM_APPS = 10;

        // Test randomness of fill per node.
        int aggregate_core_fill = 0;
        int aggregate_memslice_fill = 0;

        final int ITERS = 10;
        final int FILL_UTIL = 50;

        // Assume mean requests is for 20% of the cluster per step. This is set high
        // to reduce the number of steps needed to meet the fill.
        final double CORE_MEAN = (double) CORES_PER_NODE * 0.2;
        final double MEMSLICE_MEAN = (double) MEMSLICES_PER_NODE * 0.2;

        int[] aggregate_core_per_app = new int[NUM_APPS];
        int[] aggregate_memslice_per_app = new int[NUM_APPS];

        for (int i = 0; i < ITERS; i++) {
            // Create database
            Class.forName("org.h2.Driver");
            DSLContext conn = DSL.using("jdbc:h2:mem:");
            DCMRunner runner = new DCMRunner(conn, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, 
                NUM_APPS, null, false, false);

            // This calls check fill - so some checks just from calling it.
            runner.fillPoisson(FILL_UTIL, CORE_MEAN, MEMSLICE_MEAN);
        
            aggregate_core_fill += runner.usedCores();
            aggregate_memslice_fill += runner.usedMemslices();

            for (int j = 1; j <= NUM_APPS; j++) {
                aggregate_core_per_app[j - 1] += runner.usedCoresForApplication(j);
                aggregate_memslice_per_app[j - 1] += runner.usedMemslicesForApplication(j);
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
}
