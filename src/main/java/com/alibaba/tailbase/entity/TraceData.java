package com.alibaba.tailbase.entity;

public class TraceData {
	private String traceId;
	private String clientPort;
	
	public String getTraceId() {
		return traceId;
	}
	
	public void setTraceId(String traceId) {
		this.traceId = traceId;
	}
	
	public String getClientPort() {
		return clientPort;
	}

	public void setClientPort(String clientPort) {
		this.clientPort = clientPort;
	}

}
