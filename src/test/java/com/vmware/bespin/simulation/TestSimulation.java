package com.vmware.bespin.simulation;

import org.junit.jupiter.api.Test;

import com.vmware.bespin.scheduler.DBUtils;
import com.vmware.bespin.scheduler.Scheduler;
import com.vmware.bespin.scheduler.Solver;
import com.vmware.bespin.scheduler.dinos.DiNOSSolver;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Random;

import org.jooq.DSLContext;

public class TestSimulation {

    @Test
    public void testSimulationInstantiation() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        Simulation sim = new Simulation(conn, scheduler, null, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        // Check number of nodes
        assertEquals(scheduler.numNodes(), NUM_NODES);
        
        // Check core capacity and use
        assertEquals(scheduler.coreCapacity(), NUM_NODES * CORES_PER_NODE);
        assertEquals(scheduler.usedCores(), 0);

        // Check memslice capacity
        assertEquals(scheduler.memsliceCapacity(), NUM_NODES * MEMSLICES_PER_NODE);
        assertEquals(scheduler.usedMemslices(), 0);

        // Check per-node valudes
        for (long i = 1; i <= NUM_NODES; i++) {
            assertEquals(scheduler.coreCapacityForNode(i), CORES_PER_NODE);
            assertEquals(scheduler.usedCoresForNode(i), 0);
            assertEquals(scheduler.memsliceCapacityForNode(i), MEMSLICES_PER_NODE);
            assertEquals(scheduler.usedMemslicesForNode(i), 0);
        }

        // Check number of applications
        assertEquals(scheduler.numApps(), NUM_APPS);

        // Check pending requests
        assertEquals(scheduler.getNumPendingRequests(), 0);
        long[] idList = scheduler.getPendingRequestIDs();
        assertEquals(0, idList.length);

        // Check capacity
        assertFalse(scheduler.checkForCapacityViolation());
    }

    @Test
    public void testChooseRandomApplication() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        Simulation sim = new Simulation(conn, scheduler, null, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        // Check number of applications
        for (int i = 0; i < 10; i++) {
            final long randomApp = sim.chooseRandomApplication();
            assert(randomApp >= 1 && randomApp <= NUM_APPS);
        }

        // Check number of applications
        assertEquals(scheduler.numApps(), NUM_APPS);
    }

    @Test
    public void testChooseGaussianApplication() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 100;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        Simulation sim = new Simulation(conn, scheduler, null, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        // Check number of applications
        for (int i = 0; i < 100000; i++) {
            final long randomApp = sim.chooseGaussianApplication();
            assert(randomApp >= 1 && randomApp <= NUM_APPS);
        }

        // Check number of applications
        assertEquals(scheduler.numApps(), NUM_APPS);
    }

    @Test
    public void testCoresForUtil() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        Simulation sim = new Simulation(conn, scheduler, null, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        // check cores for util at 100%
        assertEquals(sim.coresForUtil(100), scheduler.coreCapacity());
        assertEquals(sim.coresForUtil(100), NUM_NODES * CORES_PER_NODE);

        // check cores for util at 50%
        assertEquals(sim.coresForUtil(50), scheduler.coreCapacity() / 2);        
        assertEquals(sim.coresForUtil(50), (NUM_NODES * CORES_PER_NODE) / 2);
    }

    @Test
    public void testMemslicesForUtil() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        Simulation sim = new Simulation(conn, scheduler, null, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        // check cores for util at 100%
        assertEquals(sim.memslicesForUtil(100), scheduler.memsliceCapacity());
        assertEquals(sim.memslicesForUtil(100), NUM_NODES * MEMSLICES_PER_NODE);

        // check cores for util at 50%
        assertEquals(sim.memslicesForUtil(50), scheduler.memsliceCapacity() / 2);        
        assertEquals(sim.memslicesForUtil(50), (NUM_NODES * MEMSLICES_PER_NODE) / 2);
    }

    @Test
    public void testPendingRandomRequests() throws ClassNotFoundException {
        final long NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        Simulation sim = new Simulation(conn, scheduler, null, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        // Add a request
        for (long i = 0; i < scheduler.coreCapacity() + scheduler.memsliceCapacity(); i++) {
            assertEquals(scheduler.getNumPendingRequests(), i);
            sim.generateRandomRequest();
        }
    }

    @Test
    public void testReleaseAllocation() throws Exception {
        final long NUM_NODES = 5;
        final long CORES_PER_NODE = 5;
        final long MEMSLICES_PER_NODE = 5;
        final long NUM_APPS = 5;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null);
        Simulation sim = new Simulation(conn, scheduler, null, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);
        
        final Random rand = new Random();

        // Fill entire cluster
        sim.fillRandom(100);

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

    @Test
    public void testFillRandomUtil() throws Exception {
        final long NUM_NODES = 10;
        final long CORES_PER_NODE = 10;
        final long MEMSLICES_PER_NODE = 20;
        final long NUM_APPS = 10;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Solver solver = new DiNOSSolver(conn, true, false);
        Scheduler scheduler = new Scheduler(conn, solver);
        Simulation sim = new Simulation(conn, scheduler, null, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

        sim.fillRandom(100);

        // Make sure filled all the way.
        assertEquals(scheduler.coreCapacity(), scheduler.usedCores());
        assertEquals(scheduler.memsliceCapacity(), scheduler.usedMemslices());
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
            Solver solver = new DiNOSSolver(conn, true, false);
            Scheduler scheduler = new Scheduler(conn, solver);
            Simulation sim = new Simulation(conn, scheduler, null, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

            // This calls check fill - so some checks just from calling it.
            sim.fillRandom(FILL_UTIL);
            assertEquals((scheduler.coreCapacity() * FILL_UTIL) / 100, scheduler.usedCores());
            assertEquals((scheduler.memsliceCapacity() * FILL_UTIL) / 100, scheduler.usedMemslices());
        
            for (int j = 1; j <= NUM_NODES; j++) {
                aggregate_core_fill[j - 1] += scheduler.usedCoresForNode((long) j);
                aggregate_memslice_fill[j - 1] += scheduler.usedMemslicesForNode((long) j);
            }

            for (int j = 1; j <= NUM_APPS; j++) {
                aggregate_core_per_app[j - 1] += scheduler.usedCoresForApplication((long) j);
                aggregate_memslice_per_app[j - 1] += scheduler.usedMemslicesForApplication((long) j);
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
    public void testFillPoissonUtil() throws Exception {
        final long NUM_NODES = 10;
        final long CORES_PER_NODE = 10;
        final long MEMSLICES_PER_NODE = 64;
        final long NUM_APPS = 4*NUM_NODES;

        // Test randomness of fill per node.
        int aggregate_core_fill = 0;
        int aggregate_memslice_fill = 0;

        final long ITERS = 10;
        final int FILL_UTIL = 90;

        // Assume mean requests is for 20% of the cluster per step. This is set high
        // to reduce the number of steps needed to meet the fill.
        final double CORE_MEAN =  0.05 * ((double) CORES_PER_NODE);
        final double MEMSLICE_MEAN = 0.05 * ((double) MEMSLICES_PER_NODE);

        int[] aggregate_core_per_app = new int[(int) NUM_APPS];
        int[] aggregate_memslice_per_app = new int[(int) NUM_APPS];

        for (int i = 0; i < ITERS; i++) {
            // Create database
            DSLContext conn = DBUtils.getConn();
            Solver solver = new DiNOSSolver(conn, true, false);
            Scheduler scheduler = new Scheduler(conn, solver);
            Simulation sim = new Simulation(conn, scheduler, null, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, NUM_APPS);

            // This calls check fill - so some checks just from calling it.
            sim.fillPoisson(FILL_UTIL, CORE_MEAN, MEMSLICE_MEAN);
        
            aggregate_core_fill += scheduler.usedCores();
            aggregate_memslice_fill += scheduler.usedMemslices();

            for (int j = 1; j <= NUM_APPS; j++) {
                aggregate_core_per_app[j - 1] += scheduler.usedCoresForApplication((long) j);
                aggregate_memslice_per_app[j - 1] += scheduler.usedMemslicesForApplication((long) j);
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
                CORES_PER_NODE * NUM_NODES * ((float) FILL_UTIL / 100.0) / (float) NUM_APPS, 0.10);
        assertEquals(avg_memslice_for_app, 
                MEMSLICES_PER_NODE * NUM_NODES * ((float) FILL_UTIL / 100.0) / (float) NUM_APPS, 0.10);
    }
}
