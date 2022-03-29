package com.vmware.bespin.scheduler;

import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.bespin.scheduler.generated.tables.Nodes;
import com.vmware.bespin.scheduler.generated.tables.Applications;

import java.util.List;

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

        final Nodes t = Nodes.NODES;

        System.out.println("this is the main method!");

        Model.build(conn, List.of());
    }
}
