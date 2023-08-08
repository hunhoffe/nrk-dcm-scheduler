/*
 * Copyright 2023 University of Colorado, VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2 OR MIT
 */

 package com.vmware.bespin.scheduler.dinos.rpc;
 
 public class AffinityResponse {
     public static final int BYTE_LEN = 1;
 
     final byte requestFulfilled;
 
     public AffinityResponse(final boolean requestFulfilled) {
         this.requestFulfilled = (byte) (requestFulfilled ? 1 : 0);
     }
 
     public AffinityResponse(final byte[] data) {
         assert (data.length == AffinityResponse.BYTE_LEN);
         this.requestFulfilled = data[0];
     }
 
     public byte[] toBytes() {
         final byte[] buff = new byte[AffinityResponse.BYTE_LEN];
         buff[0] = this.requestFulfilled;
         return buff;
     }
 }
 