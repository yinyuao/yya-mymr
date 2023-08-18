package com.ksc.wordcount.driver;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.datasourceapi.FileFormat;
import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.datasourceapi.UnsplitFileFormat;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.*;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.reduce.ReduceFunction;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WordCountDriver {

    public static void main(String[] args) {
        // 从命令行参数中读取配置信息
        DriverEnv.host = args[0];
        DriverEnv.port = Integer.parseInt(args[1]);
        DriverEnv.thriftPort = Integer.parseInt(args[2]);
        String applicationId = args[4];
        String inputPath = args[5];
        String outputPath = args[6];
        int topN = Integer.parseInt(args[7]);
        int reduceTaskNum = Integer.parseInt(args[8]);
        int splitSize = Integer.parseInt(args[9]);

        // 使用指定的文件格式获取文件切片信息
        FileFormat fileFormat = new UnsplitFileFormat();
        PartionFile[] partionFiles = fileFormat.getSplits(inputPath, splitSize);

        // 获取任务调度器
        TaskManager taskScheduler = DriverEnv.taskManager;

        // 获取Executor系统和DriverActor
        ActorSystem executorSystem = DriverSystem.getExecutorSystem();
        ActorRef driverActorRef = executorSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());

        // 处理Map阶段
        int mapStageId = 0;
        taskScheduler.registerBlockingQueue(mapStageId, new LinkedBlockingQueue());
        for (PartionFile partionFile : partionFiles) {
            // 定义MapFunction，处理每行数据生成KeyValue对
            MapFunction<String, KeyValue> wordCountMapFunction = new MapFunction<String, KeyValue>() {
                @Override
                public Stream<KeyValue> map(Stream<String> stream) {
                    return stream
                            .flatMap(line -> {
                                String regex = "(https?://[\\w./]+)";
                                Pattern pattern = Pattern.compile(regex);
                                Matcher matcher = pattern.matcher(line);
                                List<String> urls = new ArrayList<>();

                                while (matcher.find()) {
                                    urls.add(matcher.group(1));
                                }

                                return urls.stream();
                            })
                            .map(url -> new KeyValue(url, 1));
                }
            };

            // 创建Map任务上下文并添加到任务调度器
            MapTaskContext mapTaskContext = new MapTaskContext(applicationId, "stage_" + mapStageId, taskScheduler.generateTaskId(), partionFile.getPartionId(), partionFile,
                    fileFormat.createReader(), reduceTaskNum, wordCountMapFunction);
            taskScheduler.addTaskContext(mapStageId, mapTaskContext);
        }

        // 提交并等待Map阶段任务完成
        DriverEnv.taskScheduler.submitTask(mapStageId);
        DriverEnv.taskScheduler.waitStageFinish(mapStageId);


        // 处理Reduce阶段
        int reduceStageId = 1;
        taskScheduler.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        for (int i = 0; i < reduceTaskNum; i++) {
            // 获取与Reduce任务相关的ShuffleBlockId
            ShuffleBlockId[] stageShuffleIds = taskScheduler.getStageShuffleIdByReduceId(mapStageId, i);

            // 定义ReduceFunction，按单词聚合次数
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {
                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                    HashMap<String, Integer> wordCounts = new HashMap<>();

                    // 遍历流中的KeyValue对，对单词次数进行聚合
                    stream.forEach(kv -> {
                        String key = kv.getKey();
                        Integer value = kv.getValue();
                        wordCounts.put(key, wordCounts.getOrDefault(key, 0) + value);
                    });

                    // 将聚合结果转化为KeyValue对
                    return wordCounts.entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue()));
                }
            };

            // 创建Reduce任务上下文并添加到任务调度器
            PartionWriter partionWriter = null;
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + reduceStageId, taskScheduler.generateTaskId(), i, reduceTaskNum, stageShuffleIds, reduceFunction, partionWriter);
            taskScheduler.addTaskContext(reduceStageId, reduceTaskContext);
        }

        // 提交并等待Reduce阶段任务完成
        DriverEnv.taskScheduler.submitTask(reduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(reduceStageId);

        // 处理Reduce阶段
        reduceStageId++;
        taskScheduler.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        for (int i = 0; i < 1; i++) {
            // 获取与Reduce任务相关的ShuffleBlockId
            ShuffleBlockId[] stageShuffleIds = taskScheduler.getAllReduceStageShuffleId(reduceStageId - 1);
            // 定义ReduceFunction，按单词聚合次数
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {
                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                    HashMap<String, Integer> wordCounts = new HashMap<>();

                    // 遍历流中的KeyValue对，对单词再次次数进行聚合
                    stream.forEach(kv -> {
                        String key = kv.getKey();
                        Integer value = kv.getValue();
                        wordCounts.put(key, wordCounts.getOrDefault(key, 0) + value);
                    });

                    // 将聚合结果转化为KeyValue对
                    return wordCounts.entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue()));
                }
            };

            // 创建Reduce任务上下文并添加到任务调度器
            PartionWriter partionWriter = null;
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + reduceStageId, taskScheduler.generateTaskId(), i, 1, stageShuffleIds, reduceFunction, partionWriter);
            taskScheduler.addTaskContext(reduceStageId, reduceTaskContext);
        }

        // 提交并等待Reduce阶段任务完成
        DriverEnv.taskScheduler.submitTask(reduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(reduceStageId);

        // 处理Reduce阶段
        reduceStageId++;
        taskScheduler.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        for (int i = 0; i < 1; i++) {
            // 获取与Reduce任务相关的ShuffleBlockId
            ShuffleBlockId[] stageShuffleIds = taskScheduler.getAllReduceStageShuffleId(reduceStageId - 1);
            // 定义ReduceFunction，按单词聚合次数
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {
                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {

                    // 将Stream转换为List，并根据值进行降序排序
                    List<KeyValue<String, Integer>> keyValueList = stream.collect(Collectors.toList());
                    keyValueList.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue()));

                    // 将排序后的List转回为Stream
                    return keyValueList.stream();
                }
            };

            // 创建Reduce任务上下文并添加到任务调度器
            PartionWriter partionWriter = null;
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + reduceStageId, taskScheduler.generateTaskId(), i, 1, stageShuffleIds, reduceFunction, partionWriter);
            taskScheduler.addTaskContext(reduceStageId, reduceTaskContext);
        }

        // 提交并等待Reduce阶段任务完成
        DriverEnv.taskScheduler.submitTask(reduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(reduceStageId);


        // 处理Reduce阶段
        reduceStageId++;
        taskScheduler.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        // 获取与Reduce任务相关的ShuffleBlockId
        ShuffleBlockId[] stageShuffleIds = taskScheduler.getAllReduceStageShuffleId(reduceStageId - 1);

        // 定义ReduceFunction，按单词聚合次数
        ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {
            @Override
            public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {

                // 获取前top位的键值对
                List<KeyValue<String, Integer>> topEntries = new ArrayList<>();
                HashSet<Integer> set = new HashSet<>();
                for (KeyValue<String, Integer> entry : stream.collect(Collectors.toList())) {
                    topEntries.add(new KeyValue<>(entry.getKey(), entry.getValue()));
                    set.add(entry.getValue());
                    if (set.size() >= topN) break;
                }

                // 将聚合结果转化为KeyValue对
                return topEntries.stream();
            }
        };

        // 创建Reduce任务上下文并添加到任务调度器
        PartionWriter partionWriter = fileFormat.createWriter(outputPath, 0);
        ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + reduceStageId, taskScheduler.generateTaskId(), 0, 1, stageShuffleIds, reduceFunction, partionWriter);
        taskScheduler.addTaskContext(reduceStageId, reduceTaskContext);


        // 提交并等待Reduce阶段任务完成
        DriverEnv.taskScheduler.submitTask(reduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(reduceStageId);

        System.out.println("Job finished");
    }
}
