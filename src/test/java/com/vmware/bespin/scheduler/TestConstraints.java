package com.vmware.bespin.scheduler;

import com.vmware.dcm.Model;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import static org.jooq.impl.DSL.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        // two nodes with 3 cores, 3 memslices
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
    public void testCapacity() {
        assertTrue(true);
    }
}
