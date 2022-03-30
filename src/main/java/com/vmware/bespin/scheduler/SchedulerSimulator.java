package com.vmware.bespin.scheduler;

import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.bespin.scheduler.generated.tables.Cores;
import com.vmware.bespin.scheduler.generated.tables.Applications;

import java.util.List;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Update;
import org.jooq.impl.DSL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

public class SchedulerSimulator {

    private static void setupDb(final DSLContext conn) {
        final InputStream resourceAsStream = SchedulerSimulator.class.getResourceAsStream("/bespin_tables.sql");
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
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] argv) throws ClassNotFoundException {
        // Create an in-memory database and get a JOOQ connection to it
        Class.forName ("org.h2.Driver");
        final DSLContext conn = DSL.using("jdbc:h2:mem:");

        // Create tables in DB
        setupDb(conn);

        // Dummy constraint for testing
        final String placed_constraint = "create constraint placed_constraint as " +
                " select * from cores check current_node = controllable__node";

        Model.build(conn, List.of(placed_constraint));
    }
}
