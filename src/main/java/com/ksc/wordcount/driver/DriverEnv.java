package com.ksc.wordcount.driver;

public class DriverEnv {
    public static String host;
    public static int port;
    public static int thriftPort;

    public static TaskManager taskManager = new TaskManager();

    public static ExecutorManager executorManager = new ExecutorManager();

    public static TaskScheduler taskScheduler = new TaskScheduler(taskManager,executorManager);
}
