package com.ksc.wordcount.thrift.impl;

import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.driver.TaskManager;
import com.ksc.wordcount.driver.TaskScheduler;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.thrift.UrlTopNAppRequest;
import com.ksc.wordcount.thrift.UrlTopNAppResponse;
import com.ksc.wordcount.thrift.UrlTopNResult;
import com.ksc.wordcount.thrift.UrlTopNService;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.ksc.wordcount.driver.WordCountDriver.startProcess;

public class UrlTopNServiceImpl implements UrlTopNService.Iface {
    @Override
    public UrlTopNAppResponse submitApp(UrlTopNAppRequest urlTopNAppRequest) throws TException {
        // 实现 submitApp 方法的逻辑，处理传入的请求，返回相应的响应
        UrlTopNAppResponse response = new UrlTopNAppResponse();
        new Thread(() -> {
            startProcess(urlTopNAppRequest);
        }).start();
        response.setAppStatus(0);
        response.setApplicationId(urlTopNAppRequest.getApplicationId());
        return response;
    }

    @Override
    public UrlTopNAppResponse getAppStatus(String applicationId) throws TException {

        UrlTopNAppResponse response = new UrlTopNAppResponse();
        response.setAppStatus(DriverEnv.applicationManager.getAppStatus(applicationId) == null ? 0 : DriverEnv.applicationManager.getAppStatus(applicationId));
        response.setApplicationId(applicationId);
        return response;
    }

    @Override
    public List<UrlTopNResult> getTopNAppResult(String applicationId) throws TException {
        List<UrlTopNResult> results = new ArrayList<>();
        String outPath = DriverEnv.applicationManager.getOut(applicationId);
        List<KeyValue<String, Integer>> out = read(outPath);
        out.forEach(e -> {
            UrlTopNResult urlTopNResult = new UrlTopNResult();
            urlTopNResult.setUrl(e.getKey());
            urlTopNResult.setCount(e.getValue());
            results.add(urlTopNResult);
        });
        return results;
    }

    public static List<KeyValue<String, Integer>> read(String avroFilePath) {
        List<KeyValue<String, Integer>> keyValueList = new ArrayList<>();

        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(avroFilePath), new GenericDatumReader<>())) {

            for (GenericRecord record : dataFileReader) {
                String key = record.get("key").toString();
                Integer value = (Integer) record.get("value");
                keyValueList.add(new KeyValue(key, value));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return keyValueList;
    }
}
