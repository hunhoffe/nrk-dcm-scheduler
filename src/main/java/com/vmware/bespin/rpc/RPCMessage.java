package com.vmware.bespin.rpc;

public record RPCMessage(RPCHeader hdr, byte[] payload) { }
