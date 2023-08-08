package com.vmware.bespin.scheduler;

import org.jooq.DSLContext;
import org.jooq.Result;

import com.vmware.dcm.SolverException;

public interface Solver {
    public Result<? extends Record> solve(final DSLContext conn) throws SolverException;
}