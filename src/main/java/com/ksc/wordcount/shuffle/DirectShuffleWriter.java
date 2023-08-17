package com.ksc.wordcount.shuffle;

import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapStatus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.stream.Stream;

public class DirectShuffleWriter implements ShuffleWriter<KeyValue> {

    String baseDir;

    int reduceTaskNum;

    ObjectOutputStream[] fileWriters;

    ShuffleBlockId[] shuffleBlockIds ;

    public DirectShuffleWriter(String baseDir, String shuffleId, String applicationId, int mapId, int reduceTaskNum) {
        this.baseDir = baseDir;  // 设置基本目录
        this.reduceTaskNum = reduceTaskNum;  // 设置 reduce 任务数量
        fileWriters = new ObjectOutputStream[reduceTaskNum];  // 初始化一个 ObjectOutputStream 数组
        shuffleBlockIds = new ShuffleBlockId[reduceTaskNum];  // 初始化一个 ShuffleBlockId 数组

        // 循环为每个 reduce 任务创建对应的文件写入流和 ShuffleBlockId
        for (int i = 0; i < reduceTaskNum; i++) {
            try {
                // 创建 ShuffleBlockId，用于标识每个 reduce 任务的数据块
                shuffleBlockIds[i] = new ShuffleBlockId(baseDir, applicationId, shuffleId, mapId, i);

                // 创建目录以存储 Shuffle 数据
                new File(shuffleBlockIds[i].getShuffleParentPath()).mkdirs();

                // 创建 ObjectOutputStream 以将数据写入对应的 Shuffle 文件
                fileWriters[i] = new ObjectOutputStream(new FileOutputStream(shuffleBlockIds[i].getShufflePath()));
            } catch (IOException e) {
                e.printStackTrace();  // 打印异常信息
            }
        }
    }

    //todo 学生实现 将maptask的处理结果写入shuffle文件中
    @Override
    public void write(Stream<KeyValue> entryStream) throws IOException {
        Iterator<KeyValue> iterator = entryStream.iterator();
        while (iterator.hasNext()) {
            KeyValue next = iterator.next();
            fileWriters[next.getKey().hashCode() % reduceTaskNum].writeObject(next);
        }
    }

    @Override
    public void commit() {
        for (int i = 0; i < reduceTaskNum; i++) {
            try {
                fileWriters[i].close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public  MapStatus getMapStatus(int mapTaskId) {
        return new MapStatus(mapTaskId,shuffleBlockIds);
    }


}
