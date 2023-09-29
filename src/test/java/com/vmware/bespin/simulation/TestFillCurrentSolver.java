package com.vmware.bespin.simulation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.Set;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.jupiter.api.Test;

import com.vmware.bespin.scheduler.DBUtils;
import com.vmware.bespin.scheduler.Scheduler;
import com.vmware.bespin.scheduler.Solver;
import com.vmware.bespin.scheduler.SolverException;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.records.PendingRecord;

public class TestFillCurrentSolver {

    @Test
    public void testFillCurrentSolverSingleSolve() throws ClassNotFoundException, SolverException {
        final int NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;
        final Pending PENDING_TABLE = Pending.PENDING;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Solver solver = new FillCurrentSolver(NUM_APPS, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE);
        Scheduler scheduler = new Scheduler(conn, solver);
        Simulation sim = new Simulation(conn, scheduler, null, (long) NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, (long) NUM_APPS);

        // Generate a random request
        sim.generateRandomRequest();

        // Run solver
        final Result<? extends org.jooq.Record> results = solver.solve(conn);

        // Check result
        assertEquals(1, results.size());
        final PendingRecord pending = results.get(0).into(PENDING_TABLE);

        final Integer controllableNode = pending.getControllable_Node();
        assert controllableNode != null;
        assert controllableNode >= 1 && controllableNode <= NUM_NODES;
    }

    @Test
    public void testFillCurrentSolverMultiSolve() throws ClassNotFoundException, SolverException {
        final int NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;
        final Pending PENDING_TABLE = Pending.PENDING;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Solver solver = new FillCurrentSolver(NUM_APPS, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE);
        Scheduler scheduler = new Scheduler(conn, solver);
        Simulation sim = new Simulation(conn, scheduler, null, (long) NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, (long) NUM_APPS);

        for (int i = 0; i < 3; i++) {
            // Generate a random request
            sim.generateRandomRequest();
        }

        // Run solver and check result
        final Result<? extends org.jooq.Record> results = solver.solve(conn);
        assertEquals(3, results.size());

        for (int i = 0; i < 3; i++) {
            final PendingRecord pending = results.get(i).into(PENDING_TABLE);
            final Integer controllableNode = pending.getControllable_Node();
            assert controllableNode != null;
            assert controllableNode >= 1 && controllableNode <= NUM_NODES;
        }
    }

    @Test
    public void testFillCurrentSolverMultiStep() throws ClassNotFoundException, SolverException {
        final int NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;
        final Pending PENDING_TABLE = Pending.PENDING;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Solver solver = new FillCurrentSolver(NUM_APPS, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE);
        Scheduler scheduler = new Scheduler(conn, solver);
        Simulation sim = new Simulation(conn, scheduler, null, (long) NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, (long) NUM_APPS);

        for (int i = 0; i < 3; i++) {
            // Generate a random request
            sim.generateRandomRequest();

            // Run solver and check result
            final Result<? extends org.jooq.Record> results = solver.solve(conn);
            assertEquals(1, results.size());

            final PendingRecord pending = results.get(0).into(PENDING_TABLE);
            final Integer controllableNode = pending.getControllable_Node();
            assert controllableNode != null;
            assert controllableNode >= 1 && controllableNode <= NUM_NODES;

            conn.execute("truncate table pending;");
        }
    }

    @Test
    public void testFillCurrentSolverFillExact() throws ClassNotFoundException, SolverException {
        final int NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;
        final Pending PENDING_TABLE = Pending.PENDING;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Solver solver = new FillCurrentSolver(NUM_APPS, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE);
        Scheduler scheduler = new Scheduler(conn, solver);
        Simulation sim = new Simulation(conn, scheduler, null, (long) NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, (long) NUM_APPS);

        for (int i = 0; i < NUM_NODES; i++) {

            for (int j = 0; j < CORES_PER_NODE; j++) {
                scheduler.generateRequest(null, 1L, 0L, 1);

                // Run solver and check result
                final Result<? extends org.jooq.Record> results = solver.solve(conn);
                assertEquals(1, results.size());

                final PendingRecord pending = results.get(0).into(PENDING_TABLE);
                final Integer controllableNode = pending.getControllable_Node();
                assert controllableNode != null;
                assert controllableNode >= 1 && controllableNode <= NUM_NODES;

                conn.execute("truncate table pending;");
            }

            for (int j = 0; j < MEMSLICES_PER_NODE; j++) {
                scheduler.generateRequest(null, 0L, 1L, 1);

                // Run solver and check result
                final Result<? extends org.jooq.Record> results = solver.solve(conn);
                assertEquals(1, results.size());

                final PendingRecord pending = results.get(0).into(PENDING_TABLE);
                final Integer controllableNode = pending.getControllable_Node();
                assert controllableNode != null;
                assert controllableNode >= 1 && controllableNode <= NUM_NODES;

                conn.execute("truncate table pending;");
            }
        }
    }

    @Test
    public void testFillCurrentSolverOverfill() throws ClassNotFoundException, SolverException {
        final int NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;
        final Pending PENDING_TABLE = Pending.PENDING;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Solver solver = new FillCurrentSolver(NUM_APPS, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE);
        Scheduler scheduler = new Scheduler(conn, solver);
        Simulation sim = new Simulation(conn, scheduler, null, (long) NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, (long) NUM_APPS);

        for (int i = 0; i < NUM_NODES; i++) {

            for (int j = 0; j < CORES_PER_NODE; j++) {
                scheduler.generateRequest(null, 1L, 0L, 1);

                // Run solver and check result
                final Result<? extends org.jooq.Record> results = solver.solve(conn);
                assertEquals(1, results.size());

                final PendingRecord pending = results.get(0).into(PENDING_TABLE);
                final Integer controllableNode = pending.getControllable_Node();
                assert controllableNode != null;
                assert controllableNode >= 1 && controllableNode <= NUM_NODES;

                conn.execute("truncate table pending;");
            }

            for (int j = 0; j < MEMSLICES_PER_NODE; j++) {
                scheduler.generateRequest(null, 0L, 1L, 1);

                // Run solver and check result
                final Result<? extends org.jooq.Record> results = solver.solve(conn);
                assertEquals(1, results.size());

                final PendingRecord pending = results.get(0).into(PENDING_TABLE);
                final Integer controllableNode = pending.getControllable_Node();
                assert controllableNode != null;
                assert controllableNode >= 1 && controllableNode <= NUM_NODES;

                conn.execute("truncate table pending;");
            }
        }

        scheduler.generateRequest(null, 1L, 0L, 1);
        try {
            solver.solve(conn);
            fail("Should fail with solver exception when overfilling");
        } catch (final SolverException e) {
            // good
        }
    }

    @Test
    public void testFillCurrentSolverPlacement() throws ClassNotFoundException, SolverException {
        final int NUM_NODES = 2;
        final long CORES_PER_NODE = 3;
        final long MEMSLICES_PER_NODE = 4;
        final int NUM_APPS = 5;
        final Pending PENDING_TABLE = Pending.PENDING;

        // Create database
        DSLContext conn = DBUtils.getConn();
        Solver solver = new FillCurrentSolver(NUM_APPS, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE);
        Scheduler scheduler = new Scheduler(conn, solver);
        Simulation sim = new Simulation(conn, scheduler, null, (long) NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, (long) NUM_APPS);

        Set<Integer> totalCoreNodeSet = new HashSet<Integer>();
        Set<Integer> totalMemsliceNodeSet = new HashSet<Integer>();
        for (int i = 0; i < NUM_NODES; i++) {

            Set<Integer> nodeSet = new HashSet<Integer>();
            for (int j = 0; j < CORES_PER_NODE; j++) {
                scheduler.generateRequest(null, 1L, 0L, 1);

                // Run solver and check result
                final Result<? extends org.jooq.Record> results = solver.solve(conn);
                assertEquals(1, results.size());

                final PendingRecord pending = results.get(0).into(PENDING_TABLE);
                final Integer controllableNode = pending.getControllable_Node();
                assert controllableNode != null;
                assert controllableNode >= 1 && controllableNode <= NUM_NODES;
                nodeSet.add(controllableNode);
                totalCoreNodeSet.add(controllableNode);

                conn.execute("truncate table pending;");
            }
            // Should fill one node at a time.
            assertEquals(1, nodeSet.size());

            nodeSet = new HashSet<Integer>();
            for (int j = 0; j < MEMSLICES_PER_NODE; j++) {
                scheduler.generateRequest(null, 0L, 1L, 1);

                // Run solver and check result
                final Result<? extends org.jooq.Record> results = solver.solve(conn);
                assertEquals(1, results.size());

                final PendingRecord pending = results.get(0).into(PENDING_TABLE);
                final Integer controllableNode = pending.getControllable_Node();
                assert controllableNode != null;
                assert controllableNode >= 1 && controllableNode <= NUM_NODES;
                nodeSet.add(controllableNode);
                totalMemsliceNodeSet.add(controllableNode);

                conn.execute("truncate table pending;");
            }
            // Should fill one node at a time.
            assertEquals(1, nodeSet.size());

            assertEquals(i + 1, totalCoreNodeSet.size());
            assertEquals(i + 1, totalMemsliceNodeSet.size());
        }
    }

    @Test
    public void testFillCurrentSolverDistribution() throws Exception {
        final int NUM_NODES = 10;
        final long CORES_PER_NODE = 20;
        final long MEMSLICES_PER_NODE = 30;
        final int NUM_APPS = 10;

        // Test randomness of fill per node.
        // Get the average of averages of number of cores on a node each iter.
        int[] aggregate_core_fill = new int[(int) NUM_NODES];
        int[] aggregate_memslice_fill = new int[(int) NUM_NODES];

        int[] aggregate_core_per_app = new int[(int) NUM_APPS];
        int[] aggregate_memslice_per_app = new int[(int) NUM_APPS];

        final int ITERS = 100;
        final int FILL_UTIL = 90;

        for (long i = 0; i < ITERS; i++) {
            // Create database
            DSLContext conn = DBUtils.getConn();
            Solver solver = new FillCurrentSolver(NUM_APPS, NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE);
            Scheduler scheduler = new Scheduler(conn, solver);
            Simulation sim = new Simulation(conn, scheduler, null, (long) NUM_NODES, CORES_PER_NODE, MEMSLICES_PER_NODE, (long) NUM_APPS);

            // This calls check fill - so some checks just from calling it.
            // Note that there is some error here, so it's possible the asserts below could fail and everything is okay.
            sim.fillPoisson(FILL_UTIL, 2, 3);
            assertEquals((scheduler.coreCapacity() * FILL_UTIL) / 100, scheduler.usedCores(), 2.0 * 3);
            assertEquals((scheduler.memsliceCapacity() * FILL_UTIL) / 100, scheduler.usedMemslices(), 4.0 * 3);
        
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

}
