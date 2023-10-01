/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos;

import org.jooq.DSLContext;

import com.vmware.bespin.scheduler.DBUtils;
import com.vmware.bespin.scheduler.Solver;
import com.vmware.bespin.simulation.FillCurrentSolver;
import com.vmware.bespin.simulation.RandomSolver;
import com.vmware.bespin.simulation.RoundRobinSolver;

import java.io.IOException;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class DiNOSRunner {
    private static final String MAX_REQUESTS_PER_SOLVE_OPTION = "maxReqsPerSolve";
    private static final int MAX_REQUESTS_PER_SOLVE_DEFAULT = 15;
    private static final String MAX_TIME_PER_SOLVE_OPTION = "maxTimePerSolve";
    private static final int MAX_TIME_PER_SOLVE_DEFAULT = 10; // in milliseconds
    private static final String POLL_INTERVAL_OPTION = "pollInterval";
    private static final int POLL_INTERVAL_DEFAULT = 500; // 1/2 second in milliseconds

    // Which scheduler to use
    private static final String SOLVER_OPTION = "solver";
    private static final String SOLVER_DEFAULT = "DCMloc";

    public static void main(final String[] args) throws ClassNotFoundException, InterruptedException, 
            SocketException, UnknownHostException, IOException {
        // These are the defaults for these parameters.
        // They should be overridden by commandline arguments.
        int maxReqsPerSolve = MAX_REQUESTS_PER_SOLVE_DEFAULT;
        long maxTimePerSolve = MAX_TIME_PER_SOLVE_DEFAULT;
        long pollInterval = POLL_INTERVAL_DEFAULT;
        String solver = SOLVER_DEFAULT;

        // create Options object
        final Options options = new Options();

        final Option helpOption = Option.builder("h")
                .longOpt("help").argName("h")
                .hasArg(false)
                .desc("print help message")
                .build();
        final Option maxReqsPerSolveOption = Option.builder("r")
                .longOpt(MAX_REQUESTS_PER_SOLVE_OPTION).argName(MAX_REQUESTS_PER_SOLVE_OPTION)
                .hasArg()
                .desc(String.format("max number of new allocations request per solver iteration.%nDefault: %d",
                        MAX_REQUESTS_PER_SOLVE_DEFAULT))
                .type(Integer.class)
                .build();
        final Option maxTimePerSolveOption = Option.builder("t")
                .longOpt(MAX_TIME_PER_SOLVE_OPTION).argName(MAX_TIME_PER_SOLVE_OPTION)
                .hasArg()
                .desc(String.format("max number of second between each solver iteration.%nDefault: %d",
                        MAX_REQUESTS_PER_SOLVE_DEFAULT))
                .type(Long.class)
                .build();
        final Option pollIntervalOption = Option.builder("p")
                .longOpt(POLL_INTERVAL_OPTION).argName(POLL_INTERVAL_OPTION)
                .hasArg()
                .desc(String.format("interval to check if the solver should run in milliseconds.%nDefault: %d",
                        POLL_INTERVAL_DEFAULT))
                .type(Long.class)
                .build();
        final Option solverOption = Option.builder("s")
            .longOpt(SOLVER_OPTION).argName(SOLVER_OPTION)
            .hasArg()
            .desc(String.format("solver (DCMloc | DCMcap | R | RR | FC).%nDefault: %s", 
                    SOLVER_DEFAULT))
            .type(String.class)
            .build();

        options.addOption(helpOption);
        options.addOption(maxReqsPerSolveOption);
        options.addOption(maxTimePerSolveOption);
        options.addOption(pollIntervalOption);
        options.addOption(solverOption);

        final CommandLineParser parser = new DefaultParser();
        try {
            final CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                // automatically generate the help statement
                final HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("java -jar -Dlog4j.configurationFile=src/main/resources/log4j2.xml " +
                                "target/scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar [options]",
                        options);
                return;
            }
            if (cmd.hasOption(MAX_REQUESTS_PER_SOLVE_OPTION)) {
                maxReqsPerSolve = Integer.parseInt(cmd.getOptionValue(MAX_REQUESTS_PER_SOLVE_OPTION));
            }
            if (cmd.hasOption(MAX_TIME_PER_SOLVE_OPTION)) {
                maxTimePerSolve = Integer.parseInt(cmd.getOptionValue(MAX_TIME_PER_SOLVE_OPTION));
            }
            if (cmd.hasOption(POLL_INTERVAL_OPTION)) {
                pollInterval = Long.parseLong(cmd.getOptionValue(POLL_INTERVAL_OPTION));
            }
            if (cmd.hasOption(SOLVER_OPTION)) {
                solver = cmd.getOptionValue(SOLVER_OPTION);
                if (!solver.equals("DCMloc") && !solver.equals("DCMcap") && 
                        !solver.equals("R") && !solver.equals("RR") && !solver.equals("FC")) {
                    System.out.println(
                        String.format(
                            "Solver must be (case sensitive) 'DCMloc'|'DCMcap'|'R'|'RR'|'FC' but is '%s'",
                            solver));
                    return;
                }
            }
        } catch (final ParseException ignored) {
            System.out.println("Failed to parse command line");
            System.exit(-1);
        }

        // Create an in-memory database and get a JOOQ connection to it
        final DSLContext conn = DBUtils.getConn();

        // Choose the scheduler
        Solver mySolver = null;
        if (solver.equals("DCMcap")) {
            System.out.println("Using DCMCap solver");
            mySolver = new DiNOSSolver(conn, false, false);
        } else if (solver.equals("DCMloc")) {
            System.out.println("Using DCMLoc solver");
            mySolver = new DiNOSSolver(conn, true, false);
        } else if (solver.equals("R")) {
            System.out.println("Using Random solver");
            mySolver = new RandomSolver();
        } else if (solver.equals("RR")) {
            System.out.println("Using RoundRobin solver");
            mySolver = new RoundRobinSolver();
        } else if (solver.equals("FC")) {
            System.out.println("Using FillCurrent solver");
            mySolver = new FillCurrentSolver();
        } else {
            System.out.println("Scheduler type not supported yet.");
            System.exit(-1);
        }

        final DiNOSScheduler scheduler = new DiNOSScheduler(conn, maxReqsPerSolve, maxTimePerSolve, pollInterval, 
                InetAddress.getByName("172.31.0.11"), 10100, 10101, mySolver);

        scheduler.run(false);
    }
}