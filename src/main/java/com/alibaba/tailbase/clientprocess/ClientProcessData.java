package com.alibaba.tailbase.clientprocess;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Global;
import com.alibaba.tailbase.Utils;

import okhttp3.Request;
import okhttp3.Response;

public class ClientProcessData implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());
    
    public static void init() {
        for (int i = 0; i < Constants.BATCH_COUNT; i++) {
        	Global.BATCH_TRACE_LIST.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
        }
    }

    public static void start() {
        Thread t = new Thread(new ClientProcessData(), "ProcessDataThread");
        t.start();
    }

    @Override
    public void run() {
    	LOGGER.warn("--------------ClientProcessDataThread start--------------");
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
            int currentPos = 0;
            Integer locatePos = 0;
            Map<String, List<String>> traceMap = Global.BATCH_TRACE_LIST.get(pos);
            
            //Start reading trace data from url
            while ((line = bf.readLine()) != null) {
                String[] cols = line.split("\\|");
                if (cols != null && cols.length > 1 ) {
                    String traceId = cols[0];
                    //Check which pos stored the traceId
                    locatePos = Global.BATCH_TRACE_ID_POS_MAP.get(traceId);
                	if (locatePos == null) {
                		if (currentPos != pos) {
                			traceMap = Global.BATCH_TRACE_LIST.get(pos);
                    		currentPos = pos;
                		}
                		locatePos = pos;
                		Global.BATCH_TRACE_ID_POS_MAP.put(traceId, pos);
                	} else {
                		if (locatePos != pos) {
                    		traceMap = Global.BATCH_TRACE_LIST.get(locatePos);
                    		currentPos = locatePos;
                    	}
                	}
                	//Add into trace map
                    List<String> spanList = traceMap.get(traceId);
                    if (spanList == null) {
                    	count++;
                        spanList = new ArrayList<>();
                        traceMap.put(traceId, spanList);
                    }
                    spanList.add(line);
                    
                    //Check wrong trace spans and report wrong traceId to backend
                    if (cols.length > 8 && cols[8] != null) {
                        String tags = cols[8];
                        if (tags.contains("error=1")) {
                        	reportWrongTraceToBacked(traceId, locatePos);
                        } else if (tags.contains("http.status_code=") && tags.indexOf("http.status_code=200") < 0) {
                        	reportWrongTraceToBacked(traceId, locatePos);
                        }
                    }
                
	                //change to another trace list when full
	                if (count % Constants.BATCH_SIZE == 0) {
	                	//Mark down the last startTime for each pos
	                	try {
	                		Global.BATCH_TRACE_POS_TIME_MAP.put(pos, Long.parseLong(cols[1]));
	                	} catch (NumberFormatException e) {
	                		LOGGER.error("NumberFormatException fail to pase startTime", cols[1]);
	                	}
	                    pos++;
	                    // loop cycle
	                    if (pos >= Constants.BATCH_COUNT) {
	                        pos = 0;
	                    }
	                    traceMap = Global.BATCH_TRACE_LIST.get(pos);
	                    Global.BATCH_TRACE_POS_TIME_MAP.put(pos, (long) 0);
	                    //check if the pos being released
	                    if (traceMap.size() > 0) {
	                    	LOGGER.warn("fffff force to purge data.....");
	                        Thread.sleep(200);
	                        traceMap.clear();
	                    }
	                }
                }
            }
            
            bf.close();
            input.close();
            callFinish();
		} catch (IOException e) {
			LOGGER.error("IOException fail to process data", e);
		} catch (InterruptedException e) {
			LOGGER.error("InterruptedException fail to process data", e);
		}
    }
    
    // report wrong trace id to backend
    private void reportWrongTraceToBacked(String wrongTraceId, int pos) throws InterruptedException {
    	Global.BATCH_WRONG_TRACE_ID_POS_MAP.put(wrongTraceId, pos);
    	Global.CLIENT_WRONG_TRACE_REPORT_QUEUE.put(wrongTraceId);
    }
    
    // notify backend process when client process has finished.
    private void callFinish() {
        try {
            Request request = new Request.Builder().url("http://localhost:8002/finish").build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            LOGGER.warn("xxxx fail to callFinish");
        }
    }
    
    // backend getting wrong trace data
    public static String getWrongTracing(String wrongTraceIdList, String clientPort) {
        LOGGER.info(String.format("getWrongTracing, clientPort:%s, wrongTraceIdList:\n %s", clientPort, wrongTraceIdList));
        List<String> traceIdList = JSON.parseObject(wrongTraceIdList, new TypeReference<List<String>>(){});
        Map<String,List<String>> wrongTraceMap = new HashMap<>();
        
        //Search wrong trace data
        long latestStartTime = getWrongTraceWithBatch(traceIdList, wrongTraceMap);
        //Purge records if another client already poll the data
        if (!clientPort.equals(Utils.getPort()) && !wrongTraceMap.isEmpty() && latestStartTime!=0) {
        	try {
				Global.CLIENT_DATA_PURGE_QUEUE.put(latestStartTime);
			} catch (InterruptedException e) {
				LOGGER.error("xxxxx Send data to CLIENT_DATA_PURGE_QUEUE failed", e);
			}
        }
        
        return JSON.toJSONString(wrongTraceMap);
    }
    
    //get wrong trace data report by another client
    private static long getWrongTraceWithBatch(List<String> traceIdList, Map<String,List<String>> wrongTraceMap) {
    	long latestStartTime = 0;
    	long newStartTime = 0;
		int currentPos = 0;
		Map<String, List<String>> traceMap = null;
        for (String traceId : traceIdList) {
        	// locate the pos by traceId
        	Integer locatePos = Global.BATCH_TRACE_ID_POS_MAP.get(traceId);
        	if (locatePos != null) {
        		Global.BATCH_WRONG_TRACE_ID_POS_MAP.remove(traceId);
        		if (traceMap == null || locatePos != currentPos) {
        			currentPos = locatePos;
        			traceMap = Global.BATCH_TRACE_LIST.get(currentPos);
        		}
        		List<String> spanList = traceMap.get(traceId);
                if (spanList != null) {
                	wrongTraceMap.put(traceId, spanList);
                    
                    // log down the latest start time
                    String[] cols = spanList.get(0).split("\\|");
                    newStartTime = Long.parseLong(cols[1]);
                    if (newStartTime > latestStartTime) {
                    	latestStartTime = newStartTime;
                    }
                } else {
                	LOGGER.error("rrrrr data being purged. cannot retreive spanList: " + traceId);
                }
        	} else {
        		LOGGER.info("getWrongTraceWithBatch cannot locate traceId: " + traceId);
        	}
        }
		return latestStartTime;
    }

}
