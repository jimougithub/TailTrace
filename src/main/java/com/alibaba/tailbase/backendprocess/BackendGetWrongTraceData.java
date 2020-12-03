package com.alibaba.tailbase.backendprocess;

import static com.alibaba.tailbase.Constants.CLIENT_PROCESS_PORT1;
import static com.alibaba.tailbase.Constants.CLIENT_PROCESS_PORT2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.Global;
import com.alibaba.tailbase.Utils;

import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

@Service
public class BackendGetWrongTraceData {
	private static final Logger LOGGER = LoggerFactory.getLogger(BackendGetWrongTraceData.class.getName());
	String[] ports = new String[]{CLIENT_PROCESS_PORT1, CLIENT_PROCESS_PORT2};
	
	@Async("asyncBackendGetWrongTraceDataExecutor")
	public void run(BlockingQueue<String> BACKEND_WRONG_TRACE_QUEUE, String clientPort) {
		LOGGER.warn("--------------BackendGetWrongTraceDataThread: " + clientPort + " started--------------");
		
		List<String> badTraceIdList = new ArrayList<String>();
		String traceId = null;
		while (true) {
			try {
				//poll wrong trade id from queue
				traceId = BACKEND_WRONG_TRACE_QUEUE.poll(60, TimeUnit.SECONDS);
				while (traceId!=null) {
					badTraceIdList.add(traceId);
					traceId = BACKEND_WRONG_TRACE_QUEUE.poll();
				}
				
				//Get wrong trace data by batch
				if (!badTraceIdList.isEmpty()) {
					getWrongTraceData(badTraceIdList, clientPort);
		            badTraceIdList.clear();
				}
			} catch (InterruptedException e) {
				LOGGER.error("Poll wrong trace id wrong: ", e);
			}
		}
	}
	
	private void getWrongTraceData(List<String> traceList, String clientPort) {
		Map<String, Set<String>> map = new HashMap<>();
		for (String port : ports) {
            Map<String, List<String>> processMap = getWrongTraceDataFromClient(JSON.toJSONString(traceList), port, clientPort);
            if (processMap != null) {
                for (Map.Entry<String, List<String>> entry : processMap.entrySet()) {
                    String traceId = entry.getKey();
                    Set<String> spanSet = map.get(traceId);
                    if (spanSet == null) {
                        spanSet = new HashSet<>();
                        map.put(traceId, spanSet);
                    }
                    spanSet.addAll(entry.getValue());
                }
            }
        }
        LOGGER.info("getWrong traceIdsize:" + traceList.size() + ", result:" + map.size());
        
        //Generate Check Sum
        for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
            String traceId = entry.getKey();
            Set<String> spanSet = entry.getValue();
            // order span with startTime
            String spans = spanSet.stream().sorted(Comparator.comparing(BackendGetWrongTraceData::getStartTime)).collect(Collectors.joining("\n"));
            spans = spans + "\n";
            // output all span to check
            Global.TRACE_CHUCKSUM_MAP.put(traceId, Utils.MD5(spans));
        }
	}
     
     //call client process, to get all spans of wrong traces.
     private Map<String,List<String>> getWrongTraceDataFromClient(@RequestParam String traceIdList, String port, String clientPort) {
         try {
             RequestBody body = new FormBody.Builder().add("traceIdList", traceIdList).add("clientPort", clientPort + "").build();
             String url = String.format("http://localhost:%s/getWrongTrace", port);
             Request request = new Request.Builder().url(url).post(body).build();
             Response response = Utils.callHttp(request);
             Map<String,List<String>> resultMap = JSON.parseObject(response.body().string(), new TypeReference<Map<String, List<String>>>() {});
             response.close();
             return resultMap;
         } catch (Exception e) {
             LOGGER.warn("fail to getWrongTrace, json:" + traceIdList, e);
         }
         return null;
     }
     
     public static long getStartTime(String span) {
         if (span != null) {
             String[] cols = span.split("\\|");
             if (cols.length > 8) {
                 return Utils.toLong(cols[1], -1);
             }
         }
         return -1;
     }
}
