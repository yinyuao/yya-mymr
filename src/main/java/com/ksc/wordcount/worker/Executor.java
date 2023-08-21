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

        ExecutorEnv.host = "127.0.0.1";
        ExecutorEnv.port = Integer.parseInt(args[1]);
        ExecutorEnv.shufflePort = Integer.parseInt(args[2]);
        ExecutorEnv.memory = args[3];
        ExecutorEnv.core = Integer.parseInt(args[4]);
        ExecutorEnv.driverUrl = "akka.tcp://DriverSystem@" + args[5] + ":" + Integer.parseInt(args[6]) + "/user/driverActor";
        ExecutorEnv.executorUrl = "akka.tcp://ExecutorSystem@" + ExecutorEnv.host + ":" + ExecutorEnv.port + "/user/executorActor";


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

        // 注册
        while (true) {
            try {
                ExecutorRpc.register(new ExecutorRegister(ExecutorEnv.executorUrl, ExecutorEnv.memory, ExecutorEnv.core));
                break;
            } catch (Exception e) {
                Thread.sleep(1000);
            }
        }

    }
}
