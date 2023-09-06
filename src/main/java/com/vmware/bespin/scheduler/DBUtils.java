package com.vmware.bespin.scheduler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

public class DBUtils {
  
    /**
     * Initialized the database connection by creating tables and populating nodes
     * and applications
     * @throws ClassNotFoundException
     */
    public static DSLContext getConn() throws ClassNotFoundException {
        // Create database
        Class.forName("org.h2.Driver");
        final DSLContext conn = DSL.using("jdbc:h2:mem:");
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/bespin_tables.sql");
        try {
            assert resourceAsStream != null;
            try (final BufferedReader tables = new BufferedReader(new InputStreamReader(resourceAsStream,
                    StandardCharsets.UTF_8))) {
                // Create a fresh database
                final String schemaAsString = tables.lines()
                        .filter(line -> !line.startsWith("--")) // remove SQL comments
                        .collect(Collectors.joining("\n"));
                final String[] createStatements = schemaAsString.split(";");
                for (final String createStatement : createStatements) {
                    conn.execute(createStatement);
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

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

        return conn;
    }
    
    private DBUtils() {
        // Private constructor
    }
}
