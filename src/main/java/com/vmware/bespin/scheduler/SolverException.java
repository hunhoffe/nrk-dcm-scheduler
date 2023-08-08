package com.vmware.bespin.scheduler;

public class SolverException 
  extends Exception {
    public SolverException(final String errorMessage, final Throwable err) {
        super(errorMessage, err);
    }
}