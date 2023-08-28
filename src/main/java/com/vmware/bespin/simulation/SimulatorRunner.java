package com.vmware.bespin.simulation;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmware.bespin.scheduler.DBUtils;
import com.vmware.bespin.scheduler.Scheduler;
import com.vmware.bespin.scheduler.Solver;
import com.vmware.bespin.scheduler.dinos.DiNOSSolver;

public class SimulatorRunner {
    // Cluster Size
    private static final String NUM_NODES_OPTION = "numNodes";
    private static final int NUM_NODES_DEFAULT = 64;
    private static final String CORES_PER_NODE_OPTION = "coresPerNode";
    private static final int CORES_PER_NODE_DEFAULT = 128;
    private static final String MEMSLICES_PER_NODE_OPTION = "memslicesPerNode";
    private static final int MEMSLICES_PER_NODE_DEFAULT = 256;
    
    // Cluster utilization & fill method
    private static final String CLUSTER_UTILIZATION_OPTION = "clusterUtil";
    private static final int CLUSTER_UTILIZATION_DEFAULT = 50;
    
    // Which scheduler to use
    private static final String SCHEDULER_OPTION = "scheduler";
    private static final String SCHEDULER_DEFAULT = "DCMloc";
    
    // Number of processes
    private static final String NUM_APPS_OPTION = "numApps";
    private static final int NUM_APPS_DEFAULT = 20;
    
    // Random seed, used for debugging
    private static final String RANDOM_SEED_OPTION = "randomSeed";

    private static void print_help(final Options options) {
        final HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar target/scheduler-1.1.16-SNAPSHOT-jar-with-dependencies.jar [options]",
                options);
    }

    public static void main(final String[] args) throws Exception {

        // These are the defaults for these parameters.
        // They should be overridden by commandline arguments.
        int numNodes = NUM_NODES_DEFAULT;
        int coresPerNode = CORES_PER_NODE_DEFAULT;
        int memslicesPerNode = MEMSLICES_PER_NODE_DEFAULT;
        int clusterUtil = CLUSTER_UTILIZATION_DEFAULT;
        String scheduler = SCHEDULER_DEFAULT;
        int numApps = NUM_APPS_DEFAULT;
        Integer randomSeed = null;

        final Logger log = LogManager.getLogger(Simulation.class);

        // create Options object
        final Options options = new Options();

        final Option helpOption = Option.builder("h")
                .longOpt("help").argName("h")
                .hasArg(false)
                .desc("print help message")
                .build();

        // Set Cluster Size
        final Option numNodesOption = Option.builder("n")
                .longOpt(NUM_NODES_OPTION).argName(NUM_NODES_OPTION)
                .hasArg()
                .desc(String.format("number of nodes.%nDefault: %d", NUM_NODES_DEFAULT))
                .type(Integer.class)
                .build();
        final Option coresPerNodeOption = Option.builder("c")
                .longOpt(CORES_PER_NODE_OPTION).argName(CORES_PER_NODE_OPTION)
                .hasArg()
                .desc(String.format("cores per node.%nDefault: %d", CORES_PER_NODE_DEFAULT))
                .type(Integer.class)
                .build();
        final Option memslicesPerNodeOption = Option.builder("m")
                .longOpt(MEMSLICES_PER_NODE_OPTION).argName(MEMSLICES_PER_NODE_OPTION)
                .hasArg()
                .desc(String.format("number of 2 MB memory slices per node.%nDefault: %d", MEMSLICES_PER_NODE_DEFAULT))
                .type(Integer.class)
                .build();

        // Cluster utilization & fill method
        final Option clusterUtilOption = Option.builder("u")
            .longOpt(CLUSTER_UTILIZATION_OPTION).argName(CLUSTER_UTILIZATION_OPTION)
            .hasArg()
            .desc(String.format("cluster utilization as percentage.%nDefault: %d", CLUSTER_UTILIZATION_DEFAULT))
            .type(Integer.class)
            .build();
        
        final Option schedulerOption = Option.builder("s")
            .longOpt(SCHEDULER_OPTION).argName(SCHEDULER_OPTION)
            .hasArg()
            .desc(String.format("scheduler (DCMloc | DCMcap | R | RR).%nDefault: %s", 
                    SCHEDULER_DEFAULT))
            .type(String.class)
            .build();

        // Set number of applications (processes)
        final Option numAppsOption = Option.builder("p")
                .longOpt(NUM_APPS_OPTION).argName(NUM_APPS_OPTION)
                .hasArg()
                .desc(String.format("number of applications running on the cluster.%nDefault: %d", 
                        NUM_APPS_DEFAULT))
                .type(Integer.class)
                .build();
        // Set random seed
        final Option randomSeedOption = Option.builder("r")
                .longOpt(RANDOM_SEED_OPTION).argName(RANDOM_SEED_OPTION)
                .hasArg()
                .desc(String.format("Optional: seed for random."))
                .type(Integer.class)
                .build();

        options.addOption(helpOption);
        options.addOption(numNodesOption);
        options.addOption(coresPerNodeOption);
        options.addOption(memslicesPerNodeOption);
        options.addOption(clusterUtilOption);
        options.addOption(schedulerOption);
        options.addOption(numAppsOption);
        options.addOption(randomSeedOption);

        final CommandLineParser parser = new DefaultParser();
        try {
            final CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                // automatically generate the help statement
                print_help(options);
                return;
            }
            if (cmd.hasOption(NUM_NODES_OPTION)) {
                numNodes = Integer.parseInt(cmd.getOptionValue(NUM_NODES_OPTION));
                if (numNodes <= 0) {
                    log.error("Number of nodes must be > 0");
                    print_help(options);
                    return;
                }
            }
            if (cmd.hasOption(CORES_PER_NODE_OPTION)) {
                coresPerNode = Integer.parseInt(cmd.getOptionValue(CORES_PER_NODE_OPTION));                
                if (coresPerNode <= 0) {
                    log.error("Cores per node must be > 0");
                    print_help(options);
                    return;
                }
            }
            if (cmd.hasOption(MEMSLICES_PER_NODE_OPTION)) {
                memslicesPerNode = Integer.parseInt(cmd.getOptionValue(MEMSLICES_PER_NODE_OPTION));
                if (memslicesPerNode <= 0) {
                    log.error("Memslices per node must be > 0");
                    print_help(options);
                    return;
                }
            }
            if (cmd.hasOption(CLUSTER_UTILIZATION_OPTION)) {
                clusterUtil = Integer.parseInt(cmd.getOptionValue(CLUSTER_UTILIZATION_OPTION));
                if (clusterUtil < 0 || clusterUtil > 100) {
                    log.error("Cluster utilization must be in [0, 100] (a valid percentage)");
                    print_help(options);
                    return;
                }
            }
            if (cmd.hasOption(SCHEDULER_OPTION)) {
                scheduler = cmd.getOptionValue(SCHEDULER_OPTION);
                if (!scheduler.equals("DCMloc") && !scheduler.equals("DCMcap") && 
                        !scheduler.equals("R") && !scheduler.equals("RR")) {
                    log.error("Scheduler must be (case sensitive) 'DCMloc'|'DCMcap'|'R'|'RR' but is '{}'",
                        scheduler);
                    print_help(options);
                    return;
                }
            }
            if (cmd.hasOption(NUM_APPS_OPTION)) {
                numApps = Integer.parseInt(cmd.getOptionValue(NUM_APPS_OPTION));
                if (numApps <= 0) {
                    log.error("Number of applications must be > 0");
                    print_help(options);
                    return;
                }
            }
            if (cmd.hasOption(RANDOM_SEED_OPTION)) {
                randomSeed = Integer.parseInt(cmd.getOptionValue(RANDOM_SEED_OPTION));
            }
        } catch (final ParseException ignored) {
            log.error("Failed to parse command line");
            return;
        }

        // Create an in-memory database and get a JOOQ connection to it
        final DSLContext conn = DBUtils.getConn();

        // Choose the scheduler
        Solver solver = null;
        if (scheduler.equals("DCMcap")) {
            solver = new DiNOSSolver(conn, true, false, false);
        } else if (scheduler.equals("DCMloc")) {
            solver = new DiNOSSolver(conn, true, true, false);
        } else  {
            System.err.println("Scheudler type not supported yet.");
            System.exit(-1);
        }

        final Scheduler sched = new Scheduler(conn, solver);
        System.out.println(String.format("Simulation setup: scheduler=%s, nodes=%d, coresPerNode=%d, " + 
                "memSlicesPerNode=%d, numApps=%d, clusterUtil=%d, randomSeed=%d",
                scheduler, numNodes, coresPerNode, memslicesPerNode, numApps, clusterUtil, randomSeed));

        final Simulation simulation = new Simulation(conn, sched, randomSeed, numNodes, coresPerNode, memslicesPerNode, 
                numApps);

        final double coreMean = 0.05 * ((double) coresPerNode);
        final double memsliceMean = 0.05 * ((double) memslicesPerNode);

        // Populate the cluster
        simulation.fillPoisson(clusterUtil, coreMean, memsliceMean);
        simulation.stepPoisson(coreMean, true, memsliceMean, true, true);

        // Check for violations
        if (sched.checkForCapacityViolation()) {
            log.warn("Failed due to capacity violation??");
            System.exit(-1);
        }

        // Print final stats
        sched.printStats();
        log.info("Simulation complete");
    }
}
