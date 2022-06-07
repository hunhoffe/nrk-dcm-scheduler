package com.vmware.bespin.scheduler;

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

    public static Constraint getSpareView() {
        // View of spare resources per node for both memslices and cores
        Constraint spareView = new Constraint(
                "spareView",
                """
                        create constraint spare_view as
                        select unallocated.node, unallocated.cores - sum(pending.cores) as cores,
                            unallocated.memslices - sum(pending.memslices) as memslices
                        from pending
                        join unallocated
                            on unallocated.node = pending.controllable__node
                        group by unallocated.node, unallocated.cores, unallocated.memslices
                """);
        return spareView;
    }

    public static Constraint getCapacityConstraint() {
        // Capacity core constraint (e.g., can only use what is available on each node)
        Constraint capacityConstraint = new Constraint(
                "capacityConstraint",
                """
                        create constraint capacity_constraint as
                        select * from spare_view
                        check cores >= 0 and memslices >= 0
                """);
        return capacityConstraint;
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
