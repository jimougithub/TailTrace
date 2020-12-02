package com.alibaba.tailbase.clientprocess;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.tailbase.Global;
import com.alibaba.tailbase.Utils;
import com.alibaba.tailbase.entity.TraceData;

import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

@Service
public class ClientSendWrongTraceID {
	private static final Logger LOGGER = LoggerFactory.getLogger(ClientSendWrongTraceID.class.getName());
	
	@Async("asyncClientSendWrongTraceIdExecutor")
	public void run() {
		LOGGER.warn("--------------ClientSendWrongTraceIdThread start--------------");
		Set<String> badTraceIdList = new HashSet<>();
		while (true) {
			try {
				//Poll wrong trade id from queue
				TraceData traceData = null;
				traceData = Global.CLIENT_WRONG_TRACE_QUEUE.poll(60, TimeUnit.SECONDS);
				while (traceData!=null) {
					badTraceIdList.add(traceData.getTraceId());
					traceData = null;
					traceData = Global.CLIENT_WRONG_TRACE_QUEUE.poll();
				}
				
				//Send wrong trace id list to backend
				if (!badTraceIdList.isEmpty()) {
					sendWrongTraceIdToBackend(badTraceIdList);
		            badTraceIdList.clear();
				}
			} catch (InterruptedException e) {
				LOGGER.error("Poll wrong trace id wrong: ", e);
			}
		}
	}
	
	// call backend controller to update wrong tradeId list.
    private void sendWrongTraceIdToBackend(Set<String> badTraceIdList) {
        String json = JSON.toJSONString(badTraceIdList);
        try {
            //LOGGER.info("sendWrongTraceIdToBackend, json:" + json);
            RequestBody body = new FormBody.Builder().add("traceIdListJson", json).add("clientPort", Utils.getPort() + "").build();
            Request request = new Request.Builder().url("http://localhost:8002/setWrongTraceId").post(body).build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            LOGGER.error("fail to sendWrongTraceIdToBackend, json:" + json);
        }
    }
	
}
