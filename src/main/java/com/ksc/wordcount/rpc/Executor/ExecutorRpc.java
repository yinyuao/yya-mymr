package com.ksc.wordcount.rpc.Executor;

import akka.actor.ActorRef;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;
import com.ksc.wordcount.task.reduce.ReduceStatus;
import com.ksc.wordcount.worker.ExecutorEnv;

public class ExecutorRpc {

    public static void updateTaskMapStatue(TaskStatus taskStatus){
        if (taskStatus instanceof MapStatus && ((MapStatus) taskStatus).getTaskStatus() == TaskStatusEnum.FINISHED){
            ((MapStatus) taskStatus).setShuffleBlockHostAndPort(ExecutorEnv.host,ExecutorEnv.shufflePort);
        }
        if (taskStatus instanceof ReduceStatus && ((ReduceStatus) taskStatus).getTaskStatus() == TaskStatusEnum.FINISHED){
            if (((ReduceStatus) taskStatus).getShuffleBlockIds() != null) {
                ((ReduceStatus) taskStatus).setShuffleBlockHostAndPort(ExecutorEnv.host,ExecutorEnv.shufflePort);
            }
        }
        ExecutorSystem.getDriverRef().tell(taskStatus, ActorRef.noSender());
    }

    public static void register(ExecutorRegister executorRegister){
        ExecutorSystem.getDriverRef().tell(executorRegister, ActorRef.noSender());
    }
}
