/*
 * Copyright 2023 University of Colorado, VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

 package com.vmware.bespin.scheduler.rpc;

 import com.vmware.bespin.rpc.Utils;
 
 public class AffinityRequest {
     public static final int BYTE_LEN = Long.BYTES * 3;
 
     final long nodeId;
     final long cores;
     final long memslices;
 
     public AffinityRequest(final byte nodeId, final byte cores, final byte memslices) {
         this.nodeId = nodeId;
         this.cores = cores;
         this.memslices = memslices;
     }
 
     public AffinityRequest(final byte[] data) {
         assert (data.length == AffinityRequest.BYTE_LEN);
 
         this.nodeId = Utils.bytesToLong(data, 0);
         this.cores = Utils.bytesToLong(data, Long.BYTES);
         this.memslices = Utils.bytesToLong(data, Long.BYTES * 2);
     }
 
     public byte[] toBytes() {
         final byte[] buff = new byte[AffinityRequest.BYTE_LEN];
         Utils.longToBytes(this.nodeId, buff, 0);
         Utils.longToBytes(this.cores, buff, Long.BYTES);
         Utils.longToBytes(this.memslices, buff, Long.BYTES * 2);
     return buff;
     }
 
     @Override
     public String toString() {
         return "AffinityAllocRequest(node=" + this.nodeId + ", cores=" + this.cores +
         ", memslices=" + this.memslices + ")";
     }
 }
 