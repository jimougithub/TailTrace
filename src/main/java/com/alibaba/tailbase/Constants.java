package com.alibaba.tailbase;

public class Constants {

    public static final String CLIENT_PROCESS_PORT1 = "8000";
    public static final String CLIENT_PROCESS_PORT2 = "8001";
    public static final String BACKEND_PROCESS_PORT1 = "8002";
    public static final String BACKEND_PROCESS_PORT2 = "8003";
    public static final String BACKEND_PROCESS_PORT3 = "8004";
    
    public static final int BATCH_COUNT = 5000;						//number of bucket to cache trace Data
    public static final int BATCH_SIZE = 1000;						//maximum number of trace store into a tree map
    public static final int PROCESS_COUNT = 2;
    
}
