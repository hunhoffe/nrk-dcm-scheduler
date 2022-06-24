package com.vmware.bespin.rpc;

public record RPCMsg(RPCHeader hdr, byte[] payload) { }
