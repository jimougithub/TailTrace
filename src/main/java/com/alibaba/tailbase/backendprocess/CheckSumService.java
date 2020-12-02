package com.alibaba.tailbase.backendprocess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Global;
import com.alibaba.tailbase.Utils;
import com.alibaba.tailbase.clientprocess.ClientProcessData;

import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class CheckSumService implements Runnable{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    public static void start() {
        Thread t = new Thread(new CheckSumService(), "CheckSumServiceThread");
        t.start();
    }

    @Override
    public void run() {
        while (true) {
            try {
            	// send checksum when client process has all finished.
                if (BackendController.isFinished()) {
                    if (sendCheckSum()) {
                        break;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("CheckSumService Error", e);
            } finally {
            	try {
                    Thread.sleep(100);
                } catch (Throwable e) {
                    // quiet
                }
            }
        }
    }
    

    private boolean sendCheckSum() {
        try {
            String result = JSON.toJSONString(Global.TRACE_CHUCKSUM_MAP);
            RequestBody body = new FormBody.Builder().add("result", result).build();
            String url = String.format("http://localhost:%s/api/finished", CommonController.getDataSourcePort());
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            if (response.isSuccessful()) {
                response.close();
                LOGGER.warn("suc to sendCheckSum, result:" + result);
                return true;
            }
            LOGGER.warn("fail to sendCheckSum:(" + url + ")" + response.message());
            response.close();
            return false;
        } catch (Exception e) {
            LOGGER.warn("fail to call finish", e);
        }
        return false;
    }
}
