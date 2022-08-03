/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler;

import com.vmware.dcm.Model;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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


public class SchedulerRunner extends DCMRunner {
    private static final Logger LOG = LogManager.getLogger(SchedulerRunner.class);
    private static final String USE_CAP_FUNCTION_OPTION = "useCapFunction";
    private static final boolean USE_CAP_FUNCTION_DEFAULT = true;
    private static final String MAX_REQUESTS_PER_SOLVE_OPTION = "maxReqsPerSolve";
    private static final int MAX_REQUESTS_PER_SOLVE_DEFAULT = 15;
    private static final String MAX_TIME_PER_SOLVE_OPTION = "maxTimePerSolve";
    private static final int MAX_TIME_PER_SOLVE_DEFAULT = 10; // in milliseconds
    private static final String POLL_INTERVAL_OPTION = "pollInterval";
    private static final int POLL_INTERVAL_DEFAULT = 500; // 1/2 second in milliseconds

    public static void main(final String[] args) throws ClassNotFoundException, InterruptedException, 
            SocketException, UnknownHostException, IOException {
        // These are the defaults for these parameters.
        // They should be overridden by commandline arguments.
        boolean useCapFunction = USE_CAP_FUNCTION_DEFAULT;
        int maxReqsPerSolve = MAX_REQUESTS_PER_SOLVE_DEFAULT;
        long maxTimePerSolve = MAX_TIME_PER_SOLVE_DEFAULT;
        long pollInterval = POLL_INTERVAL_DEFAULT;

        // create Options object
        final Options options = new Options();

        final Option helpOption = Option.builder("h")
                .longOpt("help").argName("h")
                .hasArg(false)
                .desc("print help message")
                .build();
        final Option useCapFunctionOption = Option.builder("f")
                .longOpt(USE_CAP_FUNCTION_OPTION).argName(USE_CAP_FUNCTION_OPTION)
                .hasArg()
                .desc(String.format("use capability function vs hand-written constraints.%nDefault: %b",
                        USE_CAP_FUNCTION_DEFAULT))
                .type(Boolean.class)
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

        options.addOption(helpOption);
        options.addOption(useCapFunctionOption);
        options.addOption(maxReqsPerSolveOption);
        options.addOption(maxTimePerSolveOption);
        options.addOption(pollIntervalOption);

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
            if (cmd.hasOption(USE_CAP_FUNCTION_OPTION)) {
                useCapFunction = Boolean.parseBoolean(cmd.getOptionValue(USE_CAP_FUNCTION_OPTION));
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
        } catch (final ParseException ignored) {
            LOG.error("Failed to parse command line");
            return;
        }

        // Create an in-memory database and get a JOOQ connection to it
        Class.forName("org.h2.Driver");
        final DSLContext conn = DSL.using("jdbc:h2:mem:");

        // initialize database with no resources
        initDB(conn, 0, 0, 0, 0, 0, false);

        LOG.info("Running solver with parameters: useCapFunction={}, maxReqsPerSolve={}, " +
                        "maxTimePerSolve={}, pollInterval={}",
                useCapFunction, maxReqsPerSolve, maxTimePerSolve,
                pollInterval);
        final Model model = createModel(conn, useCapFunction);
        final Scheduler scheduler = new Scheduler(model, conn, maxReqsPerSolve, maxTimePerSolve, pollInterval, 
            InetAddress.getByName("172.31.0.11"), 6971);
        scheduler.run();
    }
}
