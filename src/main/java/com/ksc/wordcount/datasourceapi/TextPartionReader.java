package com.ksc.wordcount.datasourceapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class TextPartionReader implements PartionReader<String>, Serializable {


    @Override
    public Stream<String> toStream(PartionFile partionFile) throws IOException {
        Stream<String> allStream = Stream.empty();
        //todo 学生实现 maptask读取原始数据文件的内容
        FileSplit[] fileSplits = partionFile.getFileSplits();
        for (FileSplit fileSplit : fileSplits) {
            BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(fileSplit.getFileName()));
            bufferedReader.skip(fileSplit.getStart());
            Stream<String> lineStream = bufferedReader.lines().limit(fileSplit.getLength());
            allStream = Stream.concat(allStream, lineStream);
        }
        return allStream;
    }
}
