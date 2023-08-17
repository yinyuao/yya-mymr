package com.ksc.wordcount.datasourceapi;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class UnsplitFileFormat implements FileFormat {

    @Override
    public boolean isSplitable(String filePath) {
        return true;
    }

    @Override
    public PartionFile[] getSplits(String filePath, long size) {
        List<PartionFile> partiongFileList = new ArrayList<>();
        //todo 学生实现 driver端切分split的逻辑
        int pantionId = 0;
        File[] files = new File(filePath).listFiles();
        for (File file : files) {
            List<FileSplit> fileSplits = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
                String line;
                long offset = 0;
                long currentSize = 0;
                // 记录行数
                long count = 0;
                while ((line = reader.readLine()) != null) {
                    long lineSize = line.getBytes().length + System.lineSeparator().getBytes().length; // 估计行的字节大小
                    currentSize += lineSize;
                    count++;
                    if (currentSize >= size) {
                        // 创建一个新分片，将之前累积的行放入其中
                        fileSplits.add(new FileSplit(file.getAbsolutePath(), offset, count));
                        offset += currentSize;
                        currentSize = 0;
                        count = 0;
                    }
                }

                // 为剩余的行创建一个分片
                if (currentSize != 0) {
                    fileSplits.add(new FileSplit(file.getAbsolutePath(), offset, count));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            partiongFileList.add(new PartionFile(pantionId, fileSplits.toArray(fileSplits.toArray(new FileSplit[fileSplits.size()]))));
            pantionId++;
        }
        return partiongFileList.toArray(new PartionFile[partiongFileList.size()]);
    }

    @Override
    public PartionReader createReader() {
        return new TextPartionReader();
    }

    @Override
    public PartionWriter createWriter(String destPath, int partionId) {
        return new TextPartionWriter(destPath, partionId);
    }


}
