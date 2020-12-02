package com.alibaba.tailbase.clientprocess;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Global;
import com.alibaba.tailbase.Utils;
import com.alibaba.tailbase.entity.TraceData;

import okhttp3.Request;
import okhttp3.Response;


public class ClientProcessData implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());
    // an list of trace map, like ring buffer. key is traceId, value is spans
    private static List<Map<String,List<String>>> BATCH_TRACE_LIST = new ArrayList<>();
    // trace map for wrong trace data
    private static Map<String, List<String>> WRONG_TREACE_MAP = new ConcurrentHashMap<>();
    
    public static void init() {
        for (int i = 0; i < Constants.BATCH_COUNT; i++) {
            BATCH_TRACE_LIST.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
        }
    }

    public static void start() {
        Thread t = new Thread(new ClientProcessData(), "ProcessDataThread");
        t.start();
    }

    @Override
    public void run() {
        try {
            String path = Utils.getPath();
            URL url = new URL(path);
            LOGGER.info("data path:" + path);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            InputStream input = httpConnection.getInputStream();
            BufferedReader bf = new BufferedReader(new InputStreamReader(input));
            String line;
            long count = 0;
            int pos = 0;
            String wrongTraceID = "";
            Map<String, List<String>> traceMap = BATCH_TRACE_LIST.get(pos);
            
            //Start reading trace data from url
            while ((line = bf.readLine()) != null) {
                String[] cols = line.split("\\|");
                if (cols != null && cols.length > 1 ) {
                	//Add into trace map
                    String traceId = cols[0];
                    List<String> spanList = traceMap.get(traceId);
                    if (spanList == null) {
                    	count++;
                        spanList = new ArrayList<>();
                        traceMap.put(traceId, spanList);
                    }
                    spanList.add(line);
                    
                    //Report wrong trace ID
                    if (!wrongTraceID.equals("") && !traceId.equals(wrongTraceID)) {
                    	reportWrongTrace(wrongTraceID, pos);
                    	wrongTraceID = "";
                    }
                    
                    //Check wrong trace spans
                    if (cols.length > 8 && cols[8] != null) {
                        String tags = cols[8];
                        if (tags.contains("error=1")) {
                        	wrongTraceID = traceId;
                        } else if (tags.contains("http.status_code=") && tags.indexOf("http.status_code=200") < 0) {
                        	wrongTraceID = traceId;
                        }
                    }
                }
                
                //change to another trace list when full
                if (count % Constants.BATCH_SIZE == 0) {
                    pos++;
                    // loop cycle
                    if (pos >= Constants.BATCH_COUNT) {
                        pos = 0;
                    }
                    traceMap = BATCH_TRACE_LIST.get(pos);
                    // donot produce data, wait backend to consume data
                    if (traceMap.size() > 0) {
                        while (true) {
                        	LOGGER.warn("sleeping 10 million seconds");
                            Thread.sleep(10);
                            if (traceMap.size() == 0) {
                                break;
                            }
                        }
                    }
                }
            }
            
            //Report wrong trace ID
            if (!wrongTraceID.equals("")) {
            	reportWrongTrace(wrongTraceID, pos);
            	wrongTraceID = "";
            }
            
            bf.close();
            input.close();
            callFinish();
        } catch (Exception e) {
            LOGGER.error("fail to process data", e);
        }
    }
    
    
    //store and report wrong trace 
    private void reportWrongTrace(String wrongTraceId, int pos) throws InterruptedException {
    	//store wrong trace data
    	List<String> wrongSpanList = BATCH_TRACE_LIST.get(pos).get(wrongTraceId);
    	if (wrongSpanList==null) {
    		int previous = pos - 1;
    		if (previous < 0) {
    			previous = Constants.BATCH_COUNT-1;
    		}
    		wrongSpanList = BATCH_TRACE_LIST.get(previous).get(wrongTraceId);
    	}
    	if (wrongSpanList != null) {
    		WRONG_TREACE_MAP.put(wrongTraceId, wrongSpanList);
    	} else {
    		LOGGER.error("Cannot locate wrong trace data: "+ wrongTraceId);
    	}
    	
    	//report wrong trace
    	TraceData traceData = new TraceData();
    	traceData.setTraceId(wrongTraceId);
    	Global.CLIENT_WRONG_TRACE_QUEUE.put(traceData);
    }

    
    // notify backend process when client process has finished.
    private void callFinish() {
        try {
            Request request = new Request.Builder().url("http://localhost:8002/finish").build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            LOGGER.warn("fail to callFinish");
        }
    }
    
    
    public static String getWrongTracing(String wrongTraceIdList, String clientPort) {
        LOGGER.info(String.format("getWrongTracing, clientPort:%s, wrongTraceIdList:\n %s", clientPort, wrongTraceIdList));
        List<String> traceIdList = JSON.parseObject(wrongTraceIdList, new TypeReference<List<String>>(){});
        Map<String,List<String>> wrongTraceMap = new HashMap<>();
        
        //Search wrong trace data
        if (clientPort.equals(Utils.getPort())) {
        	getSelfWrongTraceWithBatch(traceIdList, wrongTraceMap);
        } else {
        	long lastStartTime = getWrongTraceWithBatch(traceIdList, wrongTraceMap);
            //Purge records if another client already poll the data
            if (!wrongTraceMap.isEmpty() && lastStartTime>0) {
            	purgeBatchTraceList(lastStartTime);
            }
        }
        
        return JSON.toJSONString(wrongTraceMap);
    }
    
    //get wrong trace data report by itself
    private static void getSelfWrongTraceWithBatch(List<String> traceIdList, Map<String,List<String>> wrongTraceMap) {
		if (!WRONG_TREACE_MAP.isEmpty()) {
	        for (String traceId : traceIdList) {
	            List<String> spanList = WRONG_TREACE_MAP.get(traceId);
	            if (spanList != null) {
	            	wrongTraceMap.put(traceId, spanList);
	            	WRONG_TREACE_MAP.remove(traceId);
	            } else {
	            	LOGGER.error("xxxxx getSelfWrongTraceWithBatch cannot get wrong trace data: " + traceId);
	            }
	        }
		}
    }
    
    //get wrong trace data report by another client
    private static long getWrongTraceWithBatch(List<String> traceIdList, Map<String,List<String>> wrongTraceMap) {
    	long lastStartTime = 0;
		for (Map<String, List<String>> traceMap : BATCH_TRACE_LIST) {
			if (traceMap.isEmpty()) {
				continue;
			}
	        for (String traceId : traceIdList) {
	            List<String> spanList = traceMap.get(traceId);
	            if (spanList != null) {
	            	wrongTraceMap.put(traceId, spanList);
	                
	                // log down the latest start time
	                String[] cols = spanList.get(spanList.size()-1).split("\\|");
	                lastStartTime = Long.parseLong(cols[1].trim());
	            }
	        }
		}
		return lastStartTime;
    }
    
    //Purge those trace list base on the start time
    private static void purgeBatchTraceList(long startTime) {
		for (Map<String, List<String>> traceMap : BATCH_TRACE_LIST) {
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
					if (Long.parseLong(cols[1]) > startTime) {
						purge = false;
						break;
					}
				}
			}
			if (purge) {
				traceMap.clear();
			}
		}
    }

}
