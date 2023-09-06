/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.jooq.DSLContext;
import org.jooq.Result;

import com.vmware.bespin.scheduler.Solver;
import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.SolverException;
import com.vmware.dcm.backend.ortools.OrToolsSolver;

public class DiNOSSolver implements Solver {
    protected Logger LOG = LogManager.getLogger(DiNOSSolver.class);
    protected final Model model;

    /**
     * DCM is a wrapper object around a database connection and model for modelling
     * a cluster with
     * cores and memslices
     * @param conn The database connection.
     * @param useLocalityConstraints if true, use locality constraints
     * @param usePrintDiagnostics set DCM to output print diagnostics
     */
    public DiNOSSolver(final DSLContext conn, final boolean useLocalityConstraints, final boolean usePrintDiagnostics) {
        final List<String> constraints = new ArrayList<>();

        constraints.add(DiNOSConstraints.getCapacityFunctionCoreConstraint().sql());
        constraints.add(DiNOSConstraints.getCapacityFunctionMemsliceConstraint().sql());

        if (useLocalityConstraints) {
            constraints.add(DiNOSConstraints.getAppLocalityPlacedConstraint().sql());
            constraints.add(DiNOSConstraints.getAppLocalityPendingConstraint().sql());
        }

        constraints.add(DiNOSConstraints.getSymmetryBreakingConstraint().sql());

        final OrToolsSolver.Builder builder = new OrToolsSolver.Builder()
                .setPrintDiagnostics(usePrintDiagnostics)
                .setUseCapacityPresenceLiterals(false)
                .setMaxTimeInSeconds(10);

        this.model = Model.build(conn, builder.build(), constraints);
    }

    /**
     * Solve all outstanding requests in the pending table
     * 
     * @param conn database connection
     * @return records Records which were assigned by this call to solve.
     * @throws Exception this should never happen, but overriding subclasses may
     *                   throw errors.
     */
    public Result<? extends org.jooq.Record> solve(final DSLContext conn) throws SolverException {
        try {
            return (Result<? extends Record>) model.solve("PENDING");
        } catch (ModelException | SolverException err) {
            throw new SolverException("DCM Solver failed: " + err.getMessage());
        }
    }
}
