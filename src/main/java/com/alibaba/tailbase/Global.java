package com.alibaba.tailbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Global {
    public static String CURRENT_PORT = "8000";
    public static boolean SYSTEM_READY = false;
    
    public static List<Map<String,List<String>>> BATCH_TRACE_LIST = new ArrayList<>();							//An list of trace map, like ring buffer. key is traceId, value is spans
    public static Map<String, String> TRACE_CHUCKSUM_MAP= new ConcurrentHashMap<>();							//Final check sump result map
    public static Map<String, Integer> BATCH_TRACE_ID_POS_MAP = new ConcurrentHashMap<>();						//Store traceId and pos mapping
    public static Map<String, Integer> BATCH_WRONG_TRACE_ID_POS_MAP = new ConcurrentHashMap<>();				//Store wrong traceId vs pos mapping
    public static Map<Integer, Long> BATCH_TRACE_POS_TIME_MAP = new ConcurrentHashMap<>();						//Store the max start time of each pos mapping
    
    public static BlockingQueue<String> CLIENT_WRONG_TRACE_REPORT_QUEUE = new LinkedBlockingQueue<String>();
    public static BlockingQueue<String> BACKEND_WRONG_TRACE_QUEUE1 = new LinkedBlockingQueue<String>();
    public static BlockingQueue<String> BACKEND_WRONG_TRACE_QUEUE2 = new LinkedBlockingQueue<String>();
    public static BlockingQueue<Long> CLIENT_DATA_PURGE_QUEUE = new LinkedBlockingQueue<Long>();
}
