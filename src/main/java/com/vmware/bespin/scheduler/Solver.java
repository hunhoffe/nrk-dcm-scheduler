package com.vmware.bespin.scheduler;

import org.jooq.DSLContext;
import org.jooq.Result;

public interface Solver {
    public Result<? extends org.jooq.Record> solve(final DSLContext conn) throws SolverException;
}