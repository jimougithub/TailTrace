package com.alibaba.tailbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

import com.alibaba.tailbase.backendprocess.BackendController;
import com.alibaba.tailbase.backendprocess.CheckSumService;
import com.alibaba.tailbase.clientprocess.ClientProcessData;

@EnableAutoConfiguration
@ComponentScan(basePackages = "com.alibaba.tailbase")
@EnableAsync
public class MultiEntry {

    public static void main(String[] args) {
    	//Retrieve port from parameter. special logic for calling from windows
    	Global.CURRENT_PORT = System.getProperty("server.port", "8080");
    	if (args.length>0 && args[0].contains("--server.port")) {
    		Global.CURRENT_PORT = args[0].trim().substring(14);
    	}
    	System.out.println("current port: " + Global.CURRENT_PORT);
    	
        if (Utils.isBackendProcess()) {
            BackendController.init();
            CheckSumService.start();
        }
        
        if (Utils.isClientProcess()) {
            ClientProcessData.init();
        }
        
        ConfigurableApplicationContext context = SpringApplication.run(MultiEntry.class, "--server.port=" + Global.CURRENT_PORT);
        context.getBean(ControlService.class).startUp();
    }

}
