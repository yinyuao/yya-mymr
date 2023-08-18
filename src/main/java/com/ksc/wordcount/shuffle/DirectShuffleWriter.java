package com.ksc.wordcount.shuffle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.reduce.ReduceStatus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.stream.Stream;

public class DirectShuffleWriter implements ShuffleWriter<KeyValue> {

    String baseDir;

    int reduceTaskNum;

    Output[] fileWriters;

    ShuffleBlockId[] shuffleBlockIds;

    public DirectShuffleWriter(String baseDir, String shuffleId, String stageId, String applicationId, int mapId, int reduceTaskNum) {
        this.baseDir = baseDir;
        this.reduceTaskNum = reduceTaskNum;
        fileWriters = new Output[reduceTaskNum];
        shuffleBlockIds = new ShuffleBlockId[reduceTaskNum];
        for (int i = 0; i < reduceTaskNum; i++) {
            try {
                shuffleBlockIds[i] = new ShuffleBlockId(baseDir, applicationId, shuffleId, stageId, mapId, i);
                new File(shuffleBlockIds[i].getShuffleParentPath()).mkdirs();
                fileWriters[i] = new Output(new FileOutputStream(shuffleBlockIds[i].getShufflePath()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //todo 学生实现 将maptask的处理结果写入shuffle文件中
    @Override
    public void write(Stream<KeyValue> entryStream) throws IOException {
        Iterator<KeyValue> iterator = entryStream.iterator();
        while (iterator.hasNext()) {
            KeyValue next = iterator.next();
            Kryo kryo = new Kryo();
            kryo.register(KeyValue.class);
            int reduceTaskIndex = next.getKey().hashCode() % reduceTaskNum;
            kryo.writeClassAndObject(fileWriters[reduceTaskIndex], next);
        }
    }

    @Override
    public void commit() {
        for (int i = 0; i < reduceTaskNum; i++) {
            fileWriters[i].close();
        }
    }

    public MapStatus getMapStatus(int mapTaskId) {
        return new MapStatus(mapTaskId, shuffleBlockIds);
    }

    public ReduceStatus getReduceStatus(int taskId) {
        return new ReduceStatus(taskId, shuffleBlockIds);
    }
}
