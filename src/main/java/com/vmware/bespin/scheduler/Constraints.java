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

    public static Constraint getCapacityFunctionCoreConstraint() {
        Constraint capacityFunctionCoreConstraint = new Constraint(
                "coreCapacityConstraint",
                """
                    create constraint core_cap as
                    select * from pending
                    join unallocated
                        on unallocated.node = pending.controllable__node
                    check capacity_constraint(pending.controllable__node, unallocated.node, pending.cores, unallocated.cores) = true
                """);
        return capacityFunctionCoreConstraint;
    }

    public static Constraint getCapacityFunctionMemsliceConstraint() {
        Constraint capacityFunctionMemsliceConstraint = new Constraint(
                "coreCapacityConstraint",
                """
                    create constraint mem_cap as
                    select * from pending
                    join unallocated
                        on unallocated.node = pending.controllable__node
                    check capacity_constraint(pending.controllable__node, unallocated.node, pending.memslices, unallocated.memslices) = true
                """);
        return capacityFunctionMemsliceConstraint;
    }

    public static Constraint getLoadBalanceCoreConstraint() {
        Constraint loadBalanceCoreConstraint = new Constraint(
                "loadBalanceCoreConstraint",
                """
                    create constraint balance_cores_constraint as
                    select cores from spare_view
                    maximize min(cores)
                """);
        return loadBalanceCoreConstraint;
    }

    public static Constraint getLoadBalanceMemsliceConstraint() {
        Constraint loadBalanceMemsliceConstraint = new Constraint(
                "loadBalanceMemsliceConstraint",
                """
                    create constraint balance_memslices_constraint as
                    select memslices from spare_view
                    maximize min(memslices)
                """);
        return loadBalanceMemsliceConstraint;
    }

    public static Constraint getAppLocalitySingleConstraint() {
        // this is buggy because does not prioritize between placed/pending locality
        // coould maybe fix with a + instead of an or?
        Constraint appLocalityConstraint = new Constraint(
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
        return appLocalityConstraint;
    }

    public static Constraint getAppLocalityPendingConstraint() {
        // TODO: do we want to maximize the sum? e.g., concentrate per node
        Constraint appLocalityConstraint = new Constraint(
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
        return appLocalityConstraint;
    }

    public static Constraint getAppLocalityPlacedConstraint() {
        // TODO: do we want to maximize the sum? e.g., concentrate per node
        Constraint appLocalityConstraint = new Constraint(
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
        return appLocalityConstraint;
    }

    public static Constraint getSymmetryBreakingConstraint() {
        Constraint symmetryBreakingConstraint = new Constraint(
                "symmetryBreakingConstraint",
                """
                   create constraint constraint_symmetry_breaking as
                   select *
                   from pending
                   group by application, cores, memslices
                   check increasing(controllable__node) = true
                   """);
        return symmetryBreakingConstraint;
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
