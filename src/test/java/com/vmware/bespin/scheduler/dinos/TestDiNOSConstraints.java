/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos;

import com.vmware.bespin.scheduler.Scheduler;
import com.vmware.bespin.scheduler.DBUtils;
import com.vmware.dcm.Model;
import com.vmware.dcm.SolverException;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class TestDiNOSConstraints {
    @Test
    public void testCapacityFunction() throws ClassNotFoundException {
        capacityTestWithPlaced(List.of(
                DiNOSConstraints.getCapacityFunctionCoreConstraint().sql(),
                DiNOSConstraints.getCapacityFunctionMemsliceConstraint().sql()));

        capacityTestWithoutPlaced(List.of(
                DiNOSConstraints.getCapacityFunctionCoreConstraint().sql(),
                DiNOSConstraints.getCapacityFunctionMemsliceConstraint().sql()));

        loadBalanceTest(List.of(
                DiNOSConstraints.getCapacityFunctionCoreConstraint().sql(),
                DiNOSConstraints.getCapacityFunctionMemsliceConstraint().sql()));
    }

    @Test
    public void testAppLocality() throws ClassNotFoundException {
        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null, false);

        // Create and build model
        OrToolsSolver.Builder b = new OrToolsSolver.Builder()
                .setPrintDiagnostics(false)
                .setMaxTimeInSeconds(300);
        Model model = Model.build(conn, b.build(), List.of(
                DiNOSConstraints.getCapacityFunctionCoreConstraint().sql(),
                DiNOSConstraints.getCapacityFunctionMemsliceConstraint().sql(),
                DiNOSConstraints.getAppLocalityPlacedConstraint().sql(),
                DiNOSConstraints.getAppLocalityPendingConstraint().sql()
        ));

        // three nodes with two cores, two memslices each
        scheduler.addNode(1, 4, 4);
        scheduler.addNode(2, 4, 4);
        scheduler.addNode(3, 4, 4);

        // add applications
        scheduler.addApplication(1);
        scheduler.addApplication(2);

        // place a memslice from application 1 on node 2
        scheduler.updateAllocation(2, 1, 0, 1);

        // place a memslice from application 2 on node 1 for confusion
        scheduler.updateAllocation(1, 2, 0, 1);

        // create 2 pending allocations - will check to see if they are placed on node 2
        scheduler.generateRequest(null, 1, 0, 1);
        scheduler.generateRequest(null, 0, 1, 1);

        // run model and check result
        Result<? extends Record> results = model.solve("PENDING");

        // Should be two placements by the model
        assertEquals(2, results.size());
        results.forEach(r -> {
            // double check values in results are as expected
            assertEquals(1, (int) r.get("APPLICATION"));
            assertEquals(2, (int) r.get("CONTROLLABLE__NODE"));
            assertEquals("PENDING", r.get("STATUS"));
            assertEquals(1, (int) r.get("CORES") + (int) r.get("MEMSLICES"));
        });
    }


    @Test
    public void testAppLocalitySingle() throws ClassNotFoundException {
        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null, false);

        // Create and build model
        OrToolsSolver.Builder b = new OrToolsSolver.Builder()
                .setPrintDiagnostics(false)
                .setMaxTimeInSeconds(300);
        Model model = Model.build(conn, b.build(), List.of(
                DiNOSConstraints.getCapacityFunctionCoreConstraint().sql(),
                DiNOSConstraints.getCapacityFunctionMemsliceConstraint().sql(),
                DiNOSConstraints.getAppLocalitySingleConstraint().sql()
        ));

        // three nodes with two cores, two memslices each
        scheduler.addNode(1, 4, 4);
        scheduler.addNode(2, 4, 4);
        scheduler.addNode(3, 4, 4);

        // one application
        scheduler.addApplication(1);
        scheduler.addApplication(2);

        // place a memslice from application 1 on node 2
        scheduler.updateAllocation(2, 1, 0, 1);

        // place a memslice from application 2 on node 1 for confusion
        scheduler.updateAllocation(1, 2, 0, 1);

        // create 2 pending allocations - will check to see if they are placed on node 2
        scheduler.generateRequest(null, 1, 0, 1);
        scheduler.generateRequest(null, 0, 1, 1);

        // run model and check result
        Result<? extends Record> results = model.solve("PENDING");

        // Should be two placements by the model
        assertEquals(2, results.size());
        results.forEach(r -> {
            // double check values in results are as expected
            assertEquals(1, (int) r.get("APPLICATION"));
            assertEquals(2, (int) r.get("CONTROLLABLE__NODE"));
            assertEquals("PENDING", r.get("STATUS"));
            assertEquals(1, (int) r.get("CORES") + (int) r.get("MEMSLICES"));
        });
    }

    private void capacityTestWithPlaced(List<String> constraints) throws ClassNotFoundException {
        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null, false);

        // Create and build model
        OrToolsSolver.Builder b = new OrToolsSolver.Builder()
                .setPrintDiagnostics(false)
                .setMaxTimeInSeconds(300);
        Model model = Model.build(conn, b.build(), constraints);

        // two nodes with 1 core, 1 memslice each
        scheduler.addNode(1, 2, 2);
        scheduler.addNode(2, 2, 2);

        // one application
        scheduler.addApplication(1);

        // placed work
        scheduler.updateAllocation(1, 1, 1, 1);
        scheduler.updateAllocation(2, 1, 1, 1);

        // create enough pending allocations to fill all capacity
        scheduler.generateRequest(null, 1, 0, 1);
        scheduler.generateRequest(null, 1, 0, 1);
        scheduler.generateRequest(null, 0, 1, 1);
        scheduler.generateRequest(null, 0, 1, 1);

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
            assertEquals("PENDING", r.get("STATUS"));
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
        scheduler.generateRequest(null, 0, 1, 1);

        // run model and check result
        try {
            model.solve("PENDING");
            fail("Model should fail");
        } catch (SolverException e) {
            assertTrue(e.toString().contains("INFEASIBLE"));
        }
    }

    private void capacityTestWithoutPlaced(List<String> constraints) throws ClassNotFoundException {
        // Create database
        DSLContext conn = DBUtils.getConn();
        Scheduler scheduler = new Scheduler(conn, null, false);

        // Create and build model
        OrToolsSolver.Builder b = new OrToolsSolver.Builder()
                .setPrintDiagnostics(false)
                .setMaxTimeInSeconds(300);
        Model model = Model.build(conn, b.build(), constraints);

        // two nodes with 1 core, 1 memslice each
        scheduler.addNode(1, 1, 1);
        scheduler.addNode(2, 1, 1);

        // one application
        scheduler.addApplication(1);

        // create enough pending allocations to fill all capacity
        scheduler.generateRequest(null, 1, 0, 1);
        scheduler.generateRequest(null, 1, 0, 1);
        scheduler.generateRequest(null, 0, 1, 1);
        scheduler.generateRequest(null, 0, 1, 1);

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
            assertEquals("PENDING", r.get("STATUS"));
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
        //conn.execute("insert into pending values(5, 1, 0, 1, 'PENDING', null, null)");
        scheduler.generateRequest(null, 0, 1, 1);

        // run model and check result
        try {
            model.solve("PENDING");
            fail("Model should fail");
        } catch (SolverException e) {
            assertTrue(e.toString().contains("INFEASIBLE"));
        }
    }

    private void loadBalanceTest(List<String> constraints) throws ClassNotFoundException {
        // Create database
        DSLContext conn = DBUtils.getConn();

        // Create and build model
        OrToolsSolver.Builder b = new OrToolsSolver.Builder()
                .setPrintDiagnostics(false)
                .setMaxTimeInSeconds(300);
        Model model = Model.build(conn, b.build(), constraints);

        // TODO: update these values
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
            assertEquals("PENDING", r.get("STATUS"));
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
                assertEquals("PENDING", r.get("STATUS"));
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
