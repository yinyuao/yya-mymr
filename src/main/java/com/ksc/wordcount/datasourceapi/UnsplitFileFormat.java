package com.ksc.wordcount.datasourceapi;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class UnsplitFileFormat implements FileFormat {

    @Override
    public boolean isSplitable(String filePath) {
        return true;
    }

    @Override
    public PartionFile[] getSplits(String filePath, long size) {
        List<PartionFile> partiongFileList=new ArrayList<>();
        //todo 学生实现 driver端切分split的逻辑
        int pantionId = 0;
        File[] files = new File(filePath).listFiles();
        for (File file : files) {
            FileSplit[] fileSplits = {new FileSplit(file.getAbsolutePath(), 0, file.length())};
            partiongFileList.add(new PartionFile(pantionId, fileSplits));
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
