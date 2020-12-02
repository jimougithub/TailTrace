package com.alibaba.tailbase.config;

import java.util.concurrent.Executor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class AsyncConfig implements AsyncConfigurer {
	@Value("${thread.client.send.wrongtraceid}")
	int CLIENT_SEND_WRONG_TRACEID_THREAD;
	
	@Value("${thread.backend.get.wrongtracedata}")
	int BACKEDN_GET_WRONG_TRACEDATA_THREAD;
	
	public int getClientSendWrongTraceIdCount() {
		return CLIENT_SEND_WRONG_TRACEID_THREAD;
	}
	
	public int getBackendGetWrongTraceThreadCount() {
		return BACKEDN_GET_WRONG_TRACEDATA_THREAD;
	}
	
	@Bean
	public Executor asyncClientSendWrongTraceIdExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(CLIENT_SEND_WRONG_TRACEID_THREAD);
		executor.setMaxPoolSize(CLIENT_SEND_WRONG_TRACEID_THREAD);
		executor.setQueueCapacity(CLIENT_SEND_WRONG_TRACEID_THREAD);
		executor.setThreadNamePrefix("send_wrong_trace_id_thread-");
		executor.initialize();
		return executor;
	}
	
	@Bean
	public Executor asyncBackendGetWrongTraceDataExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(BACKEDN_GET_WRONG_TRACEDATA_THREAD);
		executor.setMaxPoolSize(BACKEDN_GET_WRONG_TRACEDATA_THREAD);
		executor.setQueueCapacity(BACKEDN_GET_WRONG_TRACEDATA_THREAD);
		executor.setThreadNamePrefix("send_wrong_trace_id_thread");
		executor.initialize();
		return executor;
	}
	
}
