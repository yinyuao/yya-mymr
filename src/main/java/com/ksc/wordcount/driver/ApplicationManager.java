package com.ksc.wordcount.driver;

import com.ksc.wordcount.task.KeyValue;

import java.util.HashMap;
import java.util.Map;

public class ApplicationManager {

    String currentApplication = null;

    /**
     * applicationId和状态的映射
     */
    Map<String, Integer> appStatus = new HashMap<>();

    /**
     * applicationId和输出地址的映射
     */
    Map<String, String> out = new HashMap<>();

    public void setCurrentApplication(String currentApplication) {
        this.currentApplication = currentApplication;
    }

    public String getCurrentApplication() {
        return currentApplication;
    }

    public void setOut(String applicationId, String path) {
        out.put(applicationId, path);
    }

    public String getOut(String applicationId) {
        return out.get(applicationId);
    }

    public void setAppStatus(String applicationId, Integer status) {
        appStatus.put(applicationId, status);
    }

    public Integer getAppStatus(String applicationId) {
        return appStatus.get(applicationId);
    }
}
