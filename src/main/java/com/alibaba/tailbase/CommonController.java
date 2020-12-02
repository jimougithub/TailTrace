package com.alibaba.tailbase;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.tailbase.backendprocess.BackendGetWrongTraceData;
import com.alibaba.tailbase.clientprocess.ClientDataPurge;
import com.alibaba.tailbase.clientprocess.ClientProcessData;
import com.alibaba.tailbase.clientprocess.ClientSendWrongTraceID;
import com.alibaba.tailbase.config.AsyncConfig;


@RestController
public class CommonController {

  private static Integer DATA_SOURCE_PORT = 0;
  
  @Autowired
  ClientSendWrongTraceID clientSendWrongTraceID;
  
  @Autowired
  BackendGetWrongTraceData backendGetWrongTraceData;
  
  @Autowired
  ClientDataPurge clientDataPurge;
  
  @Autowired
  AsyncConfig asyncConfig;

  public static Integer getDataSourcePort() {
    return DATA_SOURCE_PORT;
  }

  @RequestMapping("/ready")
  public String ready() {
	if (Utils.isClientProcess()) {
		//Start send wrong trace id thread
		for (int i=1; i<=asyncConfig.getClientSendWrongTraceIdCount(); i++) {
			clientSendWrongTraceID.run();
		}
		
		//Start client data purge thread
		for (int i=1; i<=asyncConfig.getClientDataPurgeThreadCount(); i++) {
			clientDataPurge.run();
		}
	}
  	
  	if (Utils.isBackendProcess()) {
  		//Start get wrong trace thread
  		for (int i=1; i<=asyncConfig.getBackendGetWrongTraceThreadCount(); i++) {
  			if ((i % 2) != 0) {
  				backendGetWrongTraceData.run(Global.BACKEND_WRONG_TRACE_QUEUE1, Constants.CLIENT_PROCESS_PORT1);
  			} else {
  				backendGetWrongTraceData.run(Global.BACKEND_WRONG_TRACE_QUEUE2, Constants.CLIENT_PROCESS_PORT2);
  			}
  		}
  	}
	
    return "suc";
  }

  @RequestMapping("/setParameter")
  public String setParamter(@RequestParam Integer port) {
    DATA_SOURCE_PORT = port;
    if (Utils.isClientProcess()) {
      ClientProcessData.start();
    }
    return "suc";
  }

  @RequestMapping("/start")
  public String start() {
    return "suc";
  }



}
