package com.alibaba.tailbase;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.tailbase.backendprocess.BackendGetWrongTraceData;
import com.alibaba.tailbase.clientprocess.ClientDataPurge;
import com.alibaba.tailbase.clientprocess.ClientSendWrongTraceID;
import com.alibaba.tailbase.config.AsyncConfig;

@Service
public class ControlService {
	@Autowired
	BackendGetWrongTraceData backendGetWrongTraceData;

	@Autowired
	ClientSendWrongTraceID clientSendWrongTraceID;

	@Autowired
	ClientDataPurge clientDataPurge;

	@Autowired
	AsyncConfig asyncConfig;

	public void startUp(){
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
	}
}
