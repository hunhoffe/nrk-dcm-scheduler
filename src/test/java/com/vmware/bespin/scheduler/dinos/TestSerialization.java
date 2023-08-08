/*
 * Copyright 2022 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

package com.vmware.bespin.scheduler.dinos;

import org.junit.jupiter.api.Test;

public class TestSerialization {

    @Test
    public void testAssignment() {
        SchedulerAssignment assignment = new SchedulerAssignment(1, 2);
        byte[] b = assignment.toBytes();
        assert (b.length == SchedulerAssignment.BYTE_LEN);
        SchedulerAssignment assignment2 = new SchedulerAssignment(b);
        assert (assignment2.requestId == assignment.requestId);
        assert (assignment2.node == assignment.node);
    }
}