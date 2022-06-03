package com.vmware.bespin.scheduler;

import java.util.*;

public class Constraints {
    public static Constraint getPlacedConstraint() {
        // Only PENDING core requests are placed
        // All DCM solvers need to have this constraint
        Constraint placedConstraint = new Constraint(
                "placedConstraint",
                """
                            create constraint placed_constraint as
                            select * from pending
                            where status = 'PLACED'
                            check current_node = controllable__node
                        """);
        return placedConstraint;
    }
}

class Constraint {
    final String name;
    final String sql;

    private static Constraint singleton_instance = null;

    Constraint(String name, String sql) {
        this.name = name;
        this.sql = sql;
    }
}
