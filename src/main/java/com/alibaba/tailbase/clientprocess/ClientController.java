package com.alibaba.tailbase.clientprocess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class ClientController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    @RequestMapping("/getWrongTrace")
    public String getWrongTrace(@RequestParam String traceIdList, @RequestParam String clientPort) {
        String json = ClientProcessData.getWrongTracing(traceIdList, clientPort);
        LOGGER.info("suc to getWrongTrace, clientPort:" + clientPort);
        return json;
    }
}
