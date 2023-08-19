package com.ksc.wordcount.driver;

import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.TaskContext;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;
import com.ksc.wordcount.task.reduce.ReduceStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class TaskManager {

    /**
     * stageId和task队列的映射
     */
    private Map<Integer, BlockingQueue<TaskContext>> stageIdToBlockingQueueMap = new HashMap<>();

    /**
     * stageId和taskId的映射
     */
    private Map<Integer, List<Integer>> stageMap = new HashMap<>();

    /**
     * taskId和task状态的映射
     */
    Map<Integer, TaskStatus> taskStatusMap = new HashMap<>();


    public BlockingQueue<TaskContext> getBlockingQueue(int stageId) {
        return stageIdToBlockingQueueMap.get(stageId);
    }

    public void registerBlockingQueue(int stageId, BlockingQueue blockingQueue) {
        stageIdToBlockingQueueMap.put(stageId, blockingQueue);
    }

    public void addTaskContext(int stageId, TaskContext taskContext) {
        //建立stageId和任务的映射
        getBlockingQueue(stageId).offer(taskContext);
        if (stageMap.get(stageId) == null) {
            stageMap.put(stageId, new ArrayList());
        }
        //建立stageId和任务Id的映射
        stageMap.get(stageId).add(taskContext.getTaskId());
    }


    public StageStatusEnum getStageTaskStatus(int stageId) {
        //todo 学生实现 获取指定stage的执行状态，如果该stage下的所有task均执行成功，返回FINISHED
        for (int taskId : stageMap.get(stageId)) {
            if (taskStatusMap.get(taskId) == null) {
                return StageStatusEnum.RUNNING;
            }
            if (taskStatusMap.get(taskId).getTaskStatus() == TaskStatusEnum.FAILED) {
                return StageStatusEnum.FAILED;
            }
            if (taskStatusMap.get(taskId).getTaskStatus() == TaskStatusEnum.RUNNING) {
                return StageStatusEnum.RUNNING;
            }
        }
        return StageStatusEnum.FINISHED;
    }

    public ShuffleBlockId[] getStageShuffleIdByReduceId(int stageId, int reduceId) {
        List<ShuffleBlockId> shuffleBlockIds = new ArrayList<>();
        for (int taskId : stageMap.get(stageId)) {
            ShuffleBlockId shuffleBlockId = ((MapStatus) taskStatusMap.get(taskId)).getShuffleBlockIds()[reduceId];
            shuffleBlockIds.add(shuffleBlockId);
        }
        return shuffleBlockIds.toArray(new ShuffleBlockId[shuffleBlockIds.size()]);
    }

    public ShuffleBlockId[] getReduceStageShuffleIdByReduceId(int stageId, int reduceId) {
        List<ShuffleBlockId> shuffleBlockIdList = new ArrayList<>();
        int taskId = stageMap.get(stageId).get(reduceId);
        ShuffleBlockId[] shuffleBlockIds = ((ReduceStatus) taskStatusMap.get(taskId)).getShuffleBlockIds();
        for (ShuffleBlockId shuffleBlockId : shuffleBlockIds) {
            shuffleBlockIdList.add(shuffleBlockId);
        }
        return shuffleBlockIdList.toArray(new ShuffleBlockId[shuffleBlockIdList.size()]);
    }

    public ShuffleBlockId[] getAllReduceStageShuffleId(int stageId) {
        List<ShuffleBlockId> shuffleBlockIdList = new ArrayList<>();
        for (int taskId : stageMap.get(stageId)) {
            ShuffleBlockId[] shuffleBlockIds = ((ReduceStatus) taskStatusMap.get(taskId)).getShuffleBlockIds();
            for (ShuffleBlockId shuffleBlockId : shuffleBlockIds) {
                shuffleBlockIdList.add(shuffleBlockId);
            }
        }
        return shuffleBlockIdList.toArray(new ShuffleBlockId[shuffleBlockIdList.size()]);
    }

    public void updateTaskStatus(TaskStatus taskStatus) {
        taskStatusMap.put(taskStatus.getTaskId(), taskStatus);
    }


    private int maxTaskId = 0;

    public int generateTaskId() {
        return maxTaskId++;
    }


}
