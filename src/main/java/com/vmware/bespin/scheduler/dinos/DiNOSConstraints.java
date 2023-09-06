/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos;

public class DiNOSConstraints {
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
                            from placed
                            where placed.application = pending.application
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
                    1024 * (pending.controllable__node in
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
                maximize 
                  1024 * (pending.controllable__node in
                        (select node
                            from placed
                            where placed.application = pending.application
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
