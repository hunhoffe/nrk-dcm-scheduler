package com.vmware.bespin.scheduler;

import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class TestConstraints {
    @Test
    public void testPlacedConstraint() throws ClassNotFoundException {
        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        SimulationRunner.setupDb(conn);

        // Create and build model
        OrToolsSolver.Builder b = new OrToolsSolver.Builder()
                .setPrintDiagnostics(true)
                .setMaxTimeInSeconds(300);
        Model model = Model.build(conn, b.build(), List.of(Constraints.getPlacedConstraint().sql));

        // two nodes with 1 core, 1 memslice each
        conn.execute("insert into nodes values(1, 1, 1)");
        conn.execute("insert into nodes values(2, 1, 1)");
        // one application
        conn.execute("insert into applications values(1)");
        // create 2 pending allocation
        conn.execute("insert into pending values(1, 1, 1, 0, 'PENDING', null, null)");
        conn.execute("insert into pending values(2, 1, 0, 1, 'PENDING', null, null)");
        // one placed pending allocation
        conn.execute("insert into pending values(3, 1, 1, 1, 'PLACED', 1, 1)");

        // run model and check result
        Result<? extends Record> results = model.solve("PENDING");

        // Should be two placements by the model
        assertEquals(3, results.size());
        results.forEach(r -> {
            // double check values in results are as expected
            assertEquals(1, (int) r.get("APPLICATION"));
            assertTrue(1 == (int) r.get("CONTROLLABLE__NODE") || 2 == (int) r.get("CONTROLLABLE__NODE"));

            // this is the one that was already placed
            if ((int) r.get("ID") == 3) {
                assertEquals("PLACED", (String) r.get("STATUS"));
                assertEquals(1, (int) r.get("CURRENT_NODE"));
                assertEquals((int) r.get("CURRENT_NODE"), (int) r.get("CONTROLLABLE__NODE"));
                assertEquals(1, (int) r.get("CORES"));
                assertEquals(1, (int) r.get("MEMSLICES"));
            } else {
                assertEquals("PENDING", (String) r.get("STATUS"));
                assertEquals(1, (int) r.get("CORES") + (int) r.get("MEMSLICES"));
            }
        });
    }

    @Test
    public void testCapacity() throws ClassNotFoundException {
        capacityTestWithPlaced(List.of(
                Constraints.getPlacedConstraint().sql,
                Constraints.getSpareView().sql,
                Constraints.getCapacityConstraint().sql));

        capacityTestWithoutPlaced(List.of(
                Constraints.getPlacedConstraint().sql,
                Constraints.getSpareView().sql,
                Constraints.getCapacityConstraint().sql));
    }

    @Test
    public void testLoadBalance() throws ClassNotFoundException {
        loadBalanceTest(List.of(
                Constraints.getPlacedConstraint().sql,
                Constraints.getSpareView().sql,
                Constraints.getCapacityConstraint().sql,
                Constraints.getLoadBalanceCoreConstraint().sql,
                Constraints.getLoadBalanceMemsliceConstraint().sql));
    }

    @Test
    public void testCapacityFunction() throws ClassNotFoundException {
        capacityTestWithPlaced(List.of(
                Constraints.getPlacedConstraint().sql,
                Constraints.getCapacityFunctionCoreConstraint().sql,
                Constraints.getCapacityFunctionMemsliceConstraint().sql));

        capacityTestWithoutPlaced(List.of(
                Constraints.getPlacedConstraint().sql,
                Constraints.getCapacityFunctionCoreConstraint().sql,
                Constraints.getCapacityFunctionMemsliceConstraint().sql));

        loadBalanceTest(List.of(
                Constraints.getPlacedConstraint().sql,
                Constraints.getCapacityFunctionCoreConstraint().sql,
                Constraints.getCapacityFunctionMemsliceConstraint().sql));
    }

    private void capacityTestWithPlaced(List<String> constraints) throws ClassNotFoundException {
        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        SimulationRunner.setupDb(conn);

        // Create and build model
        OrToolsSolver.Builder b = new OrToolsSolver.Builder()
                .setPrintDiagnostics(true)
                .setMaxTimeInSeconds(300);
        Model model = Model.build(conn, b.build(), constraints);

        // two nodes with 1 core, 1 memslice each
        conn.execute("insert into nodes values(1, 2, 2)");
        conn.execute("insert into nodes values(2, 2, 2)");
        // one application
        conn.execute("insert into applications values(1)");
        // placed work
        conn.execute("insert into placed values(1, 1, 1, 1)");
        conn.execute("insert into placed values(1, 2, 1, 1)");
        // create enough pending allocations to fill all capacity
        conn.execute("insert into pending values(1, 1, 1, 0, 'PENDING', null, null)");
        conn.execute("insert into pending values(2, 1, 1, 0, 'PENDING', null, null)");
        conn.execute("insert into pending values(3, 1, 0, 1, 'PENDING', null, null)");
        conn.execute("insert into pending values(4, 1, 0, 1, 'PENDING', null, null)");

        // run model and check result
        Result<? extends Record> results = model.solve("PENDING");

        AtomicInteger oneCore = new AtomicInteger(0);
        AtomicInteger oneMemslice = new AtomicInteger(0);
        AtomicInteger twoCore = new AtomicInteger(0);
        AtomicInteger twoMemslice = new AtomicInteger(0);

        // Should be two placements by the model
        assertEquals(4, results.size());
        results.forEach(r -> {
            // double check values in results are as expected
            assertEquals(1, (int) r.get("APPLICATION"));
            assertTrue(1 == (int) r.get("CONTROLLABLE__NODE") || 2 == (int) r.get("CONTROLLABLE__NODE"));
            assertEquals("PENDING", (String) r.get("STATUS"));
            assertEquals(1, (int) r.get("CORES") + (int) r.get("MEMSLICES"));
            if ((int) r.get("CORES") == 1) {
                if ((int) r.get("CONTROLLABLE__NODE") == 1) {
                    oneCore.getAndIncrement();
                } else {
                    twoCore.getAndIncrement();
                }
            } else {
                if ((int) r.get("CONTROLLABLE__NODE") == 1) {
                    oneMemslice.getAndIncrement();
                } else {
                    twoMemslice.getAndIncrement();
                }
            }
        });
        assertEquals(1, oneCore.get());
        assertEquals(1, oneMemslice.get());
        assertEquals(1, twoCore.get());
        assertEquals(1, twoMemslice.get());

        // now add a pending allocation we don't have space for
        conn.execute("insert into pending values(5, 1, 0, 1, 'PENDING', null, null)");
        // run model and check result
        try {
            results = model.solve("PENDING");
            fail("Model should fail");
        } catch (SolverException e) {
            assertTrue(e.toString().contains("INFEASIBLE"));
        }
    }

    private void capacityTestWithoutPlaced(List<String> constraints) throws ClassNotFoundException {
        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        SimulationRunner.setupDb(conn);

        // Create and build model
        OrToolsSolver.Builder b = new OrToolsSolver.Builder()
                .setPrintDiagnostics(true)
                .setMaxTimeInSeconds(300);
        Model model = Model.build(conn, b.build(), constraints);

        // two nodes with 1 core, 1 memslice each
        conn.execute("insert into nodes values(1, 1, 1)");
        conn.execute("insert into nodes values(2, 1, 1)");

        // one application
        conn.execute("insert into applications values(1)");

        // create enough pending allocations to fill all capacity
        conn.execute("insert into pending values(1, 1, 1, 0, 'PENDING', null, null)");
        conn.execute("insert into pending values(2, 1, 1, 0, 'PENDING', null, null)");
        conn.execute("insert into pending values(3, 1, 0, 1, 'PENDING', null, null)");
        conn.execute("insert into pending values(4, 1, 0, 1, 'PENDING', null, null)");

        // run model and check result
        Result<? extends Record> results = model.solve("PENDING");

        AtomicInteger oneCore = new AtomicInteger(0);
        AtomicInteger oneMemslice = new AtomicInteger(0);
        AtomicInteger twoCore = new AtomicInteger(0);
        AtomicInteger twoMemslice = new AtomicInteger(0);

        // Should be two placements by the model
        assertEquals(4, results.size());
        results.forEach(r -> {
            // double check values in results are as expected
            assertEquals(1, (int) r.get("APPLICATION"));
            assertTrue(1 == (int) r.get("CONTROLLABLE__NODE") || 2 == (int) r.get("CONTROLLABLE__NODE"));
            assertEquals("PENDING", (String) r.get("STATUS"));
            assertEquals(1, (int) r.get("CORES") + (int) r.get("MEMSLICES"));
            if ((int) r.get("CORES") == 1) {
                if ((int) r.get("CONTROLLABLE__NODE") == 1) {
                    oneCore.getAndIncrement();
                } else {
                    twoCore.getAndIncrement();
                }
            } else {
                if ((int) r.get("CONTROLLABLE__NODE") == 1) {
                    oneMemslice.getAndIncrement();
                } else {
                    twoMemslice.getAndIncrement();
                }
            }
        });
        assertEquals(1, oneCore.get());
        assertEquals(1, oneMemslice.get());
        assertEquals(1, twoCore.get());
        assertEquals(1, twoMemslice.get());

        // now add a pending allocation we don't have space for
        conn.execute("insert into pending values(5, 1, 0, 1, 'PENDING', null, null)");
        // run model and check result
        try {
            results = model.solve("PENDING");
            fail("Model should fail");
        } catch (SolverException e) {
            assertTrue(e.toString().contains("INFEASIBLE"));
        }
    }

    private void loadBalanceTest(List<String> constraints) throws ClassNotFoundException {
        // Create database
        Class.forName("org.h2.Driver");
        DSLContext conn = DSL.using("jdbc:h2:mem:");
        SimulationRunner.setupDb(conn);

        // Create and build model
        OrToolsSolver.Builder b = new OrToolsSolver.Builder()
                .setPrintDiagnostics(true)
                .setMaxTimeInSeconds(300);
        Model model = Model.build(conn, b.build(), constraints);

        // three nodes with two memslices each
        conn.execute("insert into nodes values(1, 2, 1)");
        conn.execute("insert into nodes values(2, 2, 1)");
        conn.execute("insert into nodes values(3, 2, 1)");

        // one application
        conn.execute("insert into applications values(1)");

        // create three allocations so we can show balancing across all nodes
        conn.execute("insert into pending values(1, 1, 1, 0, 'PENDING', null, null)");
        conn.execute("insert into pending values(2, 1, 1, 0, 'PENDING', null, null)");
        conn.execute("insert into pending values(3, 1, 1, 0, 'PENDING', null, null)");

        // run model and check result
        Result<? extends Record> results = model.solve("PENDING");

        AtomicInteger oneCore = new AtomicInteger(0);
        AtomicInteger twoCore = new AtomicInteger(0);
        AtomicInteger threeCore = new AtomicInteger(0);

        // Should be two placements by the model
        assertEquals(3, results.size());
        results.forEach(r -> {
            // double check values in results are as expected
            assertEquals(1, (int) r.get("APPLICATION"));
            assertTrue(1 == (int) r.get("CONTROLLABLE__NODE") || 2 == (int) r.get("CONTROLLABLE__NODE") ||
                    3 == (int) r.get("CONTROLLABLE__NODE"));
            assertEquals("PENDING", (String) r.get("STATUS"));
            assertEquals(1, (int) r.get("CORES"));
            assertEquals(0, (int) r.get("MEMSLICES"));
            if ((int) r.get("CORES") == 1) {
                if ((int) r.get("CONTROLLABLE__NODE") == 1) {
                    oneCore.getAndIncrement();
                } else if ((int) r.get("CONTROLLABLE__NODE") == 2) {
                    twoCore.getAndIncrement();
                } else {
                    threeCore.getAndIncrement();
                }
            }
        });

        assertEquals(1, oneCore.get());
        assertEquals(1, twoCore.get());
        assertEquals(1, threeCore.get());

        // create three allocations so we can show balancing across all nodes
        conn.execute("insert into pending values(4, 1, 0, 1, 'PENDING', null, null)");
        conn.execute("insert into pending values(5, 1, 0, 1, 'PENDING', null, null)");
        conn.execute("insert into pending values(6, 1, 0, 1, 'PENDING', null, null)");

        AtomicInteger oneMemslice = new AtomicInteger(0);
        AtomicInteger twoMemslice = new AtomicInteger(0);
        AtomicInteger threeMemslice = new AtomicInteger(0);

        // run model and check result
       results = model.solve("PENDING");

        // Should be two placements by the model
        assertEquals(6, results.size());
        results.forEach(r -> {
            if ((int) r.get("MEMSLICES") == 1) {
                // double check values in results are as expected
                assertEquals(1, (int) r.get("APPLICATION"));
                assertTrue(1 == (int) r.get("CONTROLLABLE__NODE") || 2 == (int) r.get("CONTROLLABLE__NODE") ||
                        3 == (int) r.get("CONTROLLABLE__NODE"));
                assertEquals("PENDING", (String) r.get("STATUS"));
                assertEquals(0, (int) r.get("CORES"));
                assertEquals(1, (int) r.get("MEMSLICES"));

                if ((int) r.get("CONTROLLABLE__NODE") == 1) {
                    oneMemslice.getAndIncrement();
                } else if ((int) r.get("CONTROLLABLE__NODE") == 2) {
                    twoMemslice.getAndIncrement();
                } else {
                    threeMemslice.getAndIncrement();
                }
            }
        });

        assertEquals(1, oneCore.get());
        assertEquals(1, twoCore.get());
        assertEquals(1, threeCore.get());
    }
}
