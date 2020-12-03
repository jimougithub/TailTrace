package com.alibaba.tailbase.clientprocess;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Global;

@Service
public class ClientDataPurge {
	private static final Logger LOGGER = LoggerFactory.getLogger(ClientDataPurge.class.getName());
	
	@Async("asyncClientDataPurgeExecutor")
	public void run() {
		LOGGER.warn("--------------ClientDataPurgeThread start--------------");
		while (true) {
			try {
				//poll wrong trade id from queue
				Long latestStartTime = Global.CLIENT_DATA_PURGE_QUEUE.poll(60, TimeUnit.SECONDS);
				if (latestStartTime != null && latestStartTime > 0) {
					purgeBatchTraceList(latestStartTime);
				}
			} catch (InterruptedException e) {
				LOGGER.error("Poll CLIENT_DATA_PURGE_QUEUE error", e);
			}
		}
	}
	
	//Purge those trace pos base on the start time
    private static void purgeBatchTraceList(long latestStartTime) {
		for (int i=0; i<Constants.BATCH_COUNT; i++) {
			boolean purge = true;
			//Check if wrong trace data being read
			for (Map.Entry<String, Integer> entry : Global.BATCH_WRONG_TRACE_ID_POS_MAP.entrySet()) {
			    if (entry.getValue() == i) {
			    	purge = false;
			    	break;
			    }
			}
			if (!purge) {
				continue;
			}
			
			Map<String, List<String>> traceMap  = Global.BATCH_TRACE_LIST.get(i);
			if (traceMap.isEmpty()) {
				continue;
			}
			
			Long posStartTime = Global.BATCH_TRACE_POS_TIME_MAP.get(i);
			if (posStartTime != null && posStartTime > 0 && posStartTime <= latestStartTime) {
				LOGGER.info("Purging client data: " + latestStartTime);
				traceMap.clear();
			}
		}
    }
}
