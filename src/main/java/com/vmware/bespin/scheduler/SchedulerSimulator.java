package com.vmware.bespin.scheduler;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.Result;
import org.jooq.Record;
import org.jooq.Update;

public class SchedulerSimulator {

    public static void main(String[] argv) throws ClassNotFoundException {
        // Create an in-memory database and get a JOOQ connection to it
        Class.forName ("org.h2.Driver");
        final DSLContext conn = DSL.using("jdbc:h2:mem:");

        // node == physical machine / server
        conn.execute("create table nodes(" +
                "id integer)");

        System.out.println("this is the main method!");
    }
}
