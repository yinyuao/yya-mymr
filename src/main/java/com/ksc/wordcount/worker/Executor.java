package com.ksc.wordcount.worker;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.rpc.Executor.ExecutorActor;
import com.ksc.wordcount.rpc.Executor.ExecutorRpc;
import com.ksc.wordcount.rpc.Executor.ExecutorSystem;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.shuffle.nettyimpl.server.ShuffleService;

import java.io.FileInputStream;
import java.util.Properties;

public class Executor {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        try {
            // 加载配置文件
            properties.load(new FileInputStream("src/main/conf/executor/application.properties"));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        // 获取配置参数
        // 设置 ExecutorEnv 中的配置信息
        ExecutorEnv.host = properties.getProperty("executor.host");
        ExecutorEnv.port = Integer.parseInt(properties.getProperty("executor.port"));
        ExecutorEnv.memory = properties.getProperty("executor.memory");
        ExecutorEnv.driverUrl = properties.getProperty("executor.driverUrl");
        ExecutorEnv.core = Integer.parseInt(properties.getProperty("executor.core"));
        ExecutorEnv.executorUrl = properties.getProperty("executor.executorUrl")
                .replace("{host}", ExecutorEnv.host)
                .replace("{port}", String.valueOf(ExecutorEnv.port));
        ExecutorEnv.shufflePort = Integer.parseInt(properties.getProperty("executor.shufflePort"));

        // 启动一个线程来启动 ShuffleService 服务器
        new Thread(() -> {
            try {
                new ShuffleService(ExecutorEnv.shufflePort).start(); // 启动数据洗牌服务
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        // 获取 ExecutorSystem 实例并创建 ExecutorActor
        ActorSystem executorSystem = ExecutorSystem.getExecutorSystem(); // 获取 ExecutorSystem 单例
        ActorRef clientActorRef = executorSystem.actorOf(Props.create(ExecutorActor.class), "executorActor"); // 创建 ExecutorActor
        System.out.println("ServerActor started at: " + clientActorRef.path().toString());

        // 注册 Executor 信息
        ExecutorRpc.register(new ExecutorRegister(ExecutorEnv.executorUrl, ExecutorEnv.memory, ExecutorEnv.core)); // 注册 Executor
    }
}
