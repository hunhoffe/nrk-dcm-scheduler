package com.vmware.bespin.scheduler;

public class Constraints {
    public static Constraint getPlacedConstraint() {
        // Only PENDING core requests are placed
        // All DCM solvers need to have this constraint
        return new Constraint(
                "placedConstraint",
                """
                        create constraint placed_constraint as
                        select * from pending
                        where status = 'PLACED'
                        check current_node = controllable__node
                 """);
    }

    public static Constraint getSpareView() {
        // View of spare resources per node for both memslices and cores
        return new Constraint(
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
    }

    public static Constraint getCapacityConstraint() {
        // Capacity core constraint (e.g., can only use what is available on each node)
        return new Constraint(
                "capacityConstraint",
                """
                        create constraint capacity_constraint as
                        select * from spare_view
                        check cores >= 0 and memslices >= 0
                """);
    }

    public static Constraint getCapacityFunctionCoreConstraint() {
        return new Constraint(
                "coreCapacityConstraint",
                """
                    create constraint core_cap as
                    select * from pending
                    join unallocated
                        on unallocated.node = pending.controllable__node
                    check capacity_constraint(pending.controllable__node, unallocated.node, pending.cores,
                        unallocated.cores) = true
                """);
    }

    public static Constraint getCapacityFunctionMemsliceConstraint() {
        return new Constraint(
                "coreCapacityConstraint",
                """
                    create constraint mem_cap as
                    select * from pending
                    join unallocated
                        on unallocated.node = pending.controllable__node
                    check capacity_constraint(pending.controllable__node, unallocated.node, pending.memslices, 
                        unallocated.memslices) = true
                """);
    }

    public static Constraint getLoadBalanceCoreConstraint() {
        return new Constraint(
                "loadBalanceCoreConstraint",
                """
                    create constraint balance_cores_constraint as
                    select cores from spare_view
                    maximize min(cores)
                """);
    }

    public static Constraint getLoadBalanceMemsliceConstraint() {
        return new Constraint(
                "loadBalanceMemsliceConstraint",
                """
                    create constraint balance_memslices_constraint as
                    select memslices from spare_view
                    maximize min(memslices)
                """);
    }

    public static Constraint getAppLocalitySingleConstraint() {
        // this is buggy because does not prioritize between placed/pending locality
        // coould maybe fix with a + instead of an or?
        return new Constraint(
                "appLocalityConstraint",
                """
                create constraint app_locality_constraint as
                select * from pending
                maximize
                    (pending.controllable__node in
                        (select b.controllable__node
                            from pending as b
                            where b.application = pending.application
                            and not b.id = pending.id
                        ))
                    + (pending.controllable__node in
                        (select node
                            from app_nodes
                            where app_nodes.application = pending.application
                        ))
                """);
    }

    public static Constraint getAppLocalityPendingConstraint() {
        // TODO: do we want to maximize the sum? e.g., concentrate per node
        return new Constraint(
                "appLocalityPendingConstraint",
                """
                create constraint app_locality_pending_constraint as
                select * from pending
                maximize
                    (pending.controllable__node in
                        (select b.controllable__node
                            from pending as b
                            where b.application = pending.application
                            and not b.id = pending.id
                        ))
                """);
    }

    public static Constraint getAppLocalityPlacedConstraint() {
        // TODO: do we want to maximize the sum? e.g., concentrate per node
        return new Constraint(
                "appLocalityPlacedConstraint",
                """
                create constraint app_locality_placed_constraint as
                select * from pending
                maximize (pending.controllable__node in
                        (select node
                            from app_nodes
                            where app_nodes.application = pending.application
                        ))
                """);
    }

    // We don't use this constraint right now, but might in the future.
    public static Constraint getSymmetryBreakingConstraint() {
        return new Constraint(
                "symmetryBreakingConstraint",
                """
                   create constraint constraint_symmetry_breaking as
                   select *
                   from pending
                   group by application, cores, memslices
                   check increasing(controllable__node) = true
                   """);
    }
}

record Constraint(String name, String sql) { }
