package com.ksc.wordcount.thrift.impl;

import com.ksc.wordcount.thrift.UrlTopNAppRequest;
import com.ksc.wordcount.thrift.UrlTopNAppResponse;
import com.ksc.wordcount.thrift.UrlTopNResult;
import com.ksc.wordcount.thrift.UrlTopNService;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

public class UrlTopNServiceImpl implements UrlTopNService.Iface {
    @Override
    public UrlTopNAppResponse submitApp(UrlTopNAppRequest urlTopNAppRequest) throws TException {
        // 实现 submitApp 方法的逻辑，处理传入的请求，返回相应的响应
        UrlTopNAppResponse response = new UrlTopNAppResponse();
        // ...
        return response;
    }

    @Override
    public UrlTopNAppResponse getAppStatus(String applicationId) throws TException {
        // 实现 getAppStatus 方法的逻辑，处理传入的请求，返回相应的响应
        UrlTopNAppResponse response = new UrlTopNAppResponse();
        // ...
        return response;
    }

    @Override
    public List<UrlTopNResult> getTopNAppResult(String applicationId) throws TException {
        // 实现 getTopNAppResult 方法的逻辑，处理传入的请求，返回相应的响应
        List<UrlTopNResult> results = new ArrayList<>();
        // ...
        return results;
    }
}
