package com.alibaba.tailbase.backendprocess;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Global;

@RestController
public class BackendController {
	private static final Logger LOGGER = LoggerFactory.getLogger(BackendController.class.getName());
	private static volatile Integer FINISH_PROCESS_COUNT = 0;

	public static void init() {
		
	}

	@RequestMapping("/setWrongTraceId")
	public String setWrongTraceId(@RequestParam String traceIdListJson, @RequestParam String clientPort) {
		List<String> traceIdList = JSON.parseObject(traceIdListJson, new TypeReference<List<String>>() {});
		for (String traceId : traceIdList) {
			try {
				if (clientPort.equals(Constants.CLIENT_PROCESS_PORT1)) {
					Global.BACKEND_WRONG_TRACE_QUEUE1.put(traceId);
				} else {
					Global.BACKEND_WRONG_TRACE_QUEUE2.put(traceId);
				}
			} catch (InterruptedException e) {
				LOGGER.error("write BACKEND_WRONG_TRACE_QUEUE error", e);
			}
		}
		LOGGER.info(String.format("setWrongTraceId, clientPort:%s traceIdList:%s", clientPort, traceIdListJson));
		return "suc";
	}

	@RequestMapping("/finish")
	public String finish() {
		FINISH_PROCESS_COUNT++;
		LOGGER.warn("receive call 'finish', count:" + FINISH_PROCESS_COUNT);
		return "suc";
	}

	// trace batch will be finished, when client process has finished.(FINISH_PROCESS_COUNT == PROCESS_COUNT)
	public static boolean isFinished() {
		if (!Global.BACKEND_WRONG_TRACE_QUEUE1.isEmpty() || !Global.BACKEND_WRONG_TRACE_QUEUE2.isEmpty()) {
			return false;
		}
		if (FINISH_PROCESS_COUNT < Constants.PROCESS_COUNT) {
			return false;
		}
		return true;
	}
}
