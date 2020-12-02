package com.alibaba.tailbase.clientprocess;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

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
				String lastStartTime = Global.CLIENT_DATA_PURGE_QUEUE.poll(60, TimeUnit.SECONDS);
				if (lastStartTime!=null && !lastStartTime.equals("")) {
					purgeBatchTraceList(lastStartTime);
				}
			} catch (InterruptedException e) {
				LOGGER.error("Poll CLIENT_DATA_PURGE_QUEUE error", e);
			}
		}
	}
	
	//Purge those trace list base on the start time
    private static void purgeBatchTraceList(String startTime) {
		for (Map<String, List<String>> traceMap : Global.BATCH_TRACE_LIST) {
			if (traceMap.isEmpty()) {
				continue;
			}
			
			Set<Entry<String, List<String>>> entrySet=traceMap.entrySet();
			Iterator<Entry<String, List<String>>> iteratorMap=entrySet.iterator();
			Map.Entry<String, List<String>> traceList = null;
			List<String> tailList = null;
			boolean purge = true;
			while(iteratorMap.hasNext() && purge){
				traceList = iteratorMap.next();
				tailList = traceList.getValue();
				for (String item : tailList) {
					String[] cols = item.split("\\|");
					if (Long.parseLong(cols[1].trim()) > Long.parseLong(startTime)) {
						purge = false;
						break;
					}
				}
			}
			if (purge) {
				LOGGER.warn("Purging client data: " + startTime);
				traceMap.clear();
			}
		}
    }
}
