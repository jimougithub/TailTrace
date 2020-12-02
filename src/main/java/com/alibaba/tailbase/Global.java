package com.alibaba.tailbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.tailbase.entity.TraceData;

public class Global {
    public static String CURRENT_PORT = "8000";
    public static List<Map<String,List<String>>> BATCH_TRACE_LIST = new ArrayList<>();
    public static Map<String, String> TRACE_CHUCKSUM_MAP= new ConcurrentHashMap<>();
    public static BlockingQueue<TraceData> CLIENT_WRONG_TRACE_QUEUE = new LinkedBlockingQueue<TraceData>();
    public static BlockingQueue<TraceData> BACKEND_WRONG_TRACE_QUEUE1 = new LinkedBlockingQueue<TraceData>();
    public static BlockingQueue<TraceData> BACKEND_WRONG_TRACE_QUEUE2 = new LinkedBlockingQueue<TraceData>();
    public static BlockingQueue<String> CLIENT_DATA_PURGE_QUEUE = new LinkedBlockingQueue<String>();
}
