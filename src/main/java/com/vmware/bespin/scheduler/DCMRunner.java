/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler;

import com.vmware.bespin.scheduler.generated.tables.Applications;
import com.vmware.bespin.scheduler.generated.tables.Nodes;
import com.vmware.bespin.scheduler.generated.tables.Pending;
import com.vmware.bespin.scheduler.generated.tables.Placed;
import com.vmware.dcm.Model;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public abstract class DCMRunner {
    static Logger LOG = LogManager.getLogger(DCMRunner.class);

    static void setupDb(final DSLContext conn) {
        final InputStream resourceAsStream = DCMRunner.class.getResourceAsStream("/bespin_tables.sql");
        try {
            assert resourceAsStream != null;
            try (final BufferedReader tables = new BufferedReader(new InputStreamReader(resourceAsStream,
                    StandardCharsets.UTF_8))) {
                // Create a fresh database
                final String schemaAsString = tables.lines()
                        .filter(line -> !line.startsWith("--")) // remove SQL comments
                        .collect(Collectors.joining("\n"));
                final String[] createStatements = schemaAsString.split(";");
                for (final String createStatement: createStatements) {
                    conn.execute(createStatement);
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        // View to see totals of placed resources at each node
        conn.execute("""
                create view allocated as
                select node, cast(sum(cores) as int) as cores, cast(sum(memslices) as int) as memslices
                from placed
                group by node
                """);

        // View to see total unallocated (unused) resources at each node
        conn.execute("""
                create view unallocated as
                select n.id as node, cast(n.cores - coalesce(sum(p.cores), 0) as int) as cores,
                    cast(n.memslices - coalesce(sum(p.memslices), 0) as int) as memslices
                from nodes n
                left join placed p
                    on n.id = p.node
                group by n.id
                """);

        // View to see the nodes each application is placed on
        conn.execute("""
                create view app_nodes as
                select application, node
                from placed
                group by application, node
                """);
    }

    static void initDB(final DSLContext conn, final int numNodes, final int coresPerNode, 
                final int memslicesPerNode, final int clusterUtil, final String clusterFill, 
                final int numApps, final Integer randomSeed, final boolean populate) {
        final Nodes nodeTable = Nodes.NODES;
        final Applications appTable = Applications.APPLICATIONS;
        final Placed placedTable = Placed.PLACED;
        final Pending pendingTable = Pending.PENDING;

        final Random rand;
        if (randomSeed != null) {
            LOG.info("Using random seed: {}", randomSeed);
            rand = new Random(randomSeed);
        } else {
            rand = new Random();
        }

        setupDb(conn);

        // Add nodes with specified cores
        for (int i = 1; i <= numNodes; i++) {
            conn.insertInto(nodeTable)
                    .set(nodeTable.ID, i)
                    .set(nodeTable.CORES, coresPerNode)
                    .set(nodeTable.MEMSLICES, memslicesPerNode)
                    .execute();
        }

        // Add initial applications
        for (int i = 1; i <= numApps; i++) {
            conn.insertInto(appTable)
                    .set(appTable.ID, i)
                    .execute();
        }

        if (populate) {
            // Determine the number of both resources to allocate based on clusterUtil
            int coreAllocs = numNodes * coresPerNode;
            coreAllocs = (int) Math.ceil(((float) (coreAllocs * clusterUtil)) / 100.0);
            int memsliceAllocs = numNodes * memslicesPerNode;
            memsliceAllocs = (int) Math.ceil(((float) (memsliceAllocs * clusterUtil)) / 100.0);

            // Format of key=application number, values=[num_cores, num_memslices]
            final HashMap<Integer, List<Integer>> appAllocMap = new HashMap<>();

            // Assign cores to applications
            for (int i = 0; i < coreAllocs; i++) {
                final int application = rand.nextInt(numApps) + 1;
                final List<Integer> key = appAllocMap.getOrDefault(application, List.of(0, 0));
                appAllocMap.put(application, List.of(key.get(0) + 1, key.get(1)));
            }

            // Assign memslices to applications
            for (int i = 0; i < memsliceAllocs; i++) {
                final int application = rand.nextInt(numApps) + 1;
                final List<Integer> key = appAllocMap.getOrDefault(application, List.of(0, 0));
                appAllocMap.put(application, List.of(key.get(0), key.get(1) + 1));
            }

            if (clusterFill.equals("random")) {
                // Assign application allocs to nodes
                for (final Map.Entry<Integer, List<Integer>> entry : appAllocMap.entrySet()) {
                    final int application = entry.getKey();
                    final int cores = entry.getValue().get(0);
                    final int memslices = entry.getValue().get(1);

                    for (int i = 0; i < cores; i++) {
                        boolean done = false;
                        while (!done) {
                            // Choose a random node
                            final int node = rand.nextInt(numNodes) + 1;

                            // Check if there are any cores available
                            final String sql = String.format("select cores from allocated where node = %d", node);
                            int coresUsed = 0;
                            final Result<Record> coreResults = conn.fetch(sql);
                            if (null != coreResults && coreResults.isNotEmpty()) {
                                coresUsed = (int) conn.fetch(sql).get(0).getValue(0);
                            }

                            // If there is a core available, allocate it.
                            if (coresPerNode - coresUsed >= 1) {
                                conn.insertInto(placedTable,
                                        placedTable.APPLICATION, placedTable.NODE, placedTable.CORES, 
                                        placedTable.MEMSLICES)
                                    .values(application, node, 1, 0)
                                    .onDuplicateKeyUpdate()
                                    .set(placedTable.CORES, placedTable.CORES.plus(1))
                                    .execute();
                                done = true;
                            }
                        }
                    }

                    for (int i = 0; i < memslices; i++) {
                        boolean done = false;
                        while (!done) {
                            // Choose a random node
                            final int node = rand.nextInt(numNodes) + 1;

                            // Check if there are any cores available
                            final String sql = String.format("select memslices from allocated where node = %d", node);
                            int memslicesUsed = 0;
                            final Result<Record> memsliceResults = conn.fetch(sql);
                            if (null != memsliceResults && memsliceResults.isNotEmpty()) {
                                memslicesUsed = (int) conn.fetch(sql).get(0).getValue(0);
                            }

                            // If there is a core available, allocate it.
                            if (memslicesPerNode - memslicesUsed >= 1) {
                                conn.insertInto(placedTable,
                                        placedTable.APPLICATION, placedTable.NODE, placedTable.CORES, 
                                        placedTable.MEMSLICES)
                                    .values(application, node, 0, 1)
                                    .onDuplicateKeyUpdate()
                                    .set(placedTable.MEMSLICES, placedTable.MEMSLICES.plus(1))
                                    .execute();
                                done = true;
                            }
                        }
                    }
                }

                // Double check correctness
                String sql = "select sum(cores) from allocated";
                final int totalCoresUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
                sql = "select sum(memslices) from allocated";
                final int totalMemslicesUsed = ((Long) conn.fetch(sql).get(0).getValue(0)).intValue();
                sql = "select max(cores) from allocated";
                final int maxCoresUsed = (Integer) conn.fetch(sql).get(0).getValue(0);
                sql = "select max(memslices) from allocated";
                final int maxMemslicesUsed = (Integer) conn.fetch(sql).get(0).getValue(0);
                assert (totalCoresUsed == coreAllocs);
                assert (maxCoresUsed <= coresPerNode);
                assert (totalMemslicesUsed == memsliceAllocs);
                assert (maxMemslicesUsed <= memslicesPerNode);

                LOG.info(String.format("Total cores used: %d/%d, Max cores per node: %d/%d",
                        totalCoresUsed, coreAllocs, maxCoresUsed, coresPerNode));
                LOG.info(String.format("Total memslices used: %d/%d, Max memslices per node: %d/%d",
                        totalMemslicesUsed, memsliceAllocs, maxMemslicesUsed, memslicesPerNode));
            } else {
                // Assume this is SingleStep fill. Chunk as much as possible (up to 1/2 node capabity)
                for (final Map.Entry<Integer, List<Integer>> entry : appAllocMap.entrySet()) {
                    final int application = entry.getKey();
                    final int cores = entry.getValue().get(0);
                    final int memslices = entry.getValue().get(1);

                    int coresToSubmit = cores;
                    while (coresToSubmit > 0) {
                        final int coresInSubmission;
                        if (coresToSubmit > coresPerNode / 2) {
                            coresInSubmission = coresPerNode / 2;
                        } else {
                            coresInSubmission = coresToSubmit;
                        }   
                        conn.insertInto(pendingTable)
                            .set(pendingTable.APPLICATION, application)
                            .set(pendingTable.CORES, coresInSubmission)
                            .set(pendingTable.MEMSLICES, 0)
                            .set(pendingTable.STATUS, "PENDING")
                            .set(pendingTable.CURRENT_NODE, -1)
                            .set(pendingTable.CONTROLLABLE__NODE, (Field<Integer>) null)
                            .execute();
                        coresToSubmit -= coresInSubmission;
                    }

                    int memslicesToSubmit = memslices;
                    while (memslicesToSubmit > 0) {
                        final int memslicesInSubmission;
                        if (memslicesToSubmit > memslicesPerNode / 2) {
                            memslicesInSubmission = memslicesPerNode / 2;
                        } else {
                            memslicesInSubmission = memslicesToSubmit;
                        }   
                        conn.insertInto(pendingTable)
                            .set(pendingTable.APPLICATION, application)
                            .set(pendingTable.CORES, 0)
                            .set(pendingTable.MEMSLICES, memslicesInSubmission)
                            .set(pendingTable.STATUS, "PENDING")
                            .set(pendingTable.CURRENT_NODE, -1)
                            .set(pendingTable.CONTROLLABLE__NODE, (Field<Integer>) null)
                            .execute();
                            memslicesToSubmit -= memslicesInSubmission;
                    }
                }
            }
        }
    }

    static Model createModel(final DSLContext conn, final boolean useCapFunc, final boolean usePrintDiagnostics) {
        final List<String> constraints = new ArrayList<>();
        constraints.add(Constraints.getPlacedConstraint().sql());

        if (useCapFunc) {
            // this will replace two above, and balance constraint below
            constraints.add(Constraints.getCapacityFunctionCoreConstraint().sql());
            constraints.add(Constraints.getCapacityFunctionMemsliceConstraint().sql());
        } else {
            constraints.add(Constraints.getSpareView().sql());
            constraints.add(Constraints.getCapacityConstraint().sql());

            // TODO: should scale because we're maximizing the sum cores_per_node ratio compared to memslices_per_node
            constraints.add(Constraints.getLoadBalanceCoreConstraint().sql());
            constraints.add(Constraints.getLoadBalanceMemsliceConstraint().sql());
        }
        constraints.add(Constraints.getAppLocalityPlacedConstraint().sql());
        constraints.add(Constraints.getAppLocalityPendingConstraint().sql());

        final OrToolsSolver.Builder builder = new OrToolsSolver.Builder()
                .setPrintDiagnostics(usePrintDiagnostics)
                .setUseCapacityPresenceLiterals(false)
                .setMaxTimeInSeconds(300);
        return Model.build(conn, builder.build(), constraints);
    }

    static void printStats(final DSLContext conn) {

        // print resource usage statistics by node
        System.out.println("Unallocated resources per node:");
        System.out.println(conn.fetch("select * from unallocated"));

        // print application statistics
        System.out.println("Application resources grouped by node: ");
        final String nodesPerAppView = """
                select application, sum(cores) as cores, sum(memslices) as memslices,
                    count(distinct node) as num_nodes
                from placed
                group by application
        """;
        final Result<? extends Record> appResults = conn.fetch(nodesPerAppView);
        appResults.forEach(r -> {
            LOG.info("FRAGMENTATION_PROCESS: app={}, num_nodes={}", (Integer) r.get("APPLICATION"), 
                    (Long) r.get("NUM_NODES"));
        });
        System.out.println(appResults);


        // print application statistics
        System.out.println("Node resources grouped by application");
        final String appsPerNodeView = """
                select node, sum(cores) as cores, sum(memslices) as memslices,
                    count(distinct application) as num_apps
                from placed
                group by node
        """;
        final Result<? extends Record> nodeResults = conn.fetch(appsPerNodeView);
        nodeResults.forEach(r -> {
            LOG.info("FRAGMENTATION_NODE: node={}, num_processes={}", (Integer) r.get("NODE"), 
                    (Long) r.get("NUM_APPS"));
        });
        System.out.println(nodeResults);
    }
}
