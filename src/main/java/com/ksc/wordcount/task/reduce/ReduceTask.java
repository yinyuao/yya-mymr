package com.ksc.wordcount.task.reduce;

import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.rpc.Executor.ExecutorRpc;
import com.ksc.wordcount.shuffle.DirectShuffleWriter;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.nettyimpl.client.ShuffleClient;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.Task;
import com.ksc.wordcount.task.TaskStatusEnum;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;

public class ReduceTask extends Task {

    ShuffleBlockId[] shuffleBlockId;
    //String destDir;
    ReduceFunction reduceFunction;
    PartionWriter partionWriter;
    int reduceTaskNum;



    public ReduceTask(ReduceTaskContext reduceTaskContext) {
        super(reduceTaskContext);
        this.shuffleBlockId = reduceTaskContext.getShuffleBlockId();
        //this.destDir = reduceTaskContext.getDestDir();
        this.reduceFunction = reduceTaskContext.getReduceFunction();
        this.partionWriter = reduceTaskContext.getPartionWriter();
        this.reduceTaskNum = reduceTaskContext.getReduceTaskNum();
    }




    public ReduceStatus runTask() throws Exception {
            Stream<KeyValue> stream=Stream.empty();

            for(ShuffleBlockId shuffleBlockId:shuffleBlockId){
                stream=Stream.concat(stream,new ShuffleClient().fetchShuffleData(shuffleBlockId));
            }
            Stream reduceStream = reduceFunction.reduce(stream);
            if (partionWriter != null) {
                String path = partionWriter.write(reduceStream);
                ExecutorRpc.subOutPath(new KeyValue<>(applicationId, path));
            } else {
                String shuffleId= UUID.randomUUID().toString();
                //将task执行结果写入shuffle文件中
                System.out.println();
                DirectShuffleWriter shuffleWriter = new DirectShuffleWriter(AppConfig.shuffleTempDir, shuffleId, super.stageId, applicationId, partionId, reduceTaskNum);
                shuffleWriter.write(reduceStream);
                shuffleWriter.commit();
                return shuffleWriter.getReduceStatus(super.taskId);
            }
            return new ReduceStatus(super.taskId, TaskStatusEnum.FINISHED);
        }
}
