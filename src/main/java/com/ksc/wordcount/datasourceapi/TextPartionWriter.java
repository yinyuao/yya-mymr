package com.ksc.wordcount.datasourceapi;

import com.ksc.wordcount.task.KeyValue;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.http.util.ByteArrayBuffer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class TextPartionWriter implements PartionWriter<KeyValue>, Serializable {

    private String destDest;
    private int partionId;

    public TextPartionWriter(String destDest,int partionId){
         this.destDest = destDest;
         this.partionId = partionId;
    }

    //把partionId 前面补0，补成length位
    public String padLeft(int partionId,int length){
        String partionIdStr = String.valueOf(partionId);
        int len = partionIdStr.length();
        if(len<length){
            for(int i=0;i<length-len;i++){
                partionIdStr = "0"+partionIdStr;
            }
        }
        return partionIdStr;
    }

    //todo 学生实现 将reducetask的计算结果写入结果文件中
    @Override
    public void write(Stream<KeyValue> stream) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/main/schema/KeyValue.avsc"));
        File file = new File(destDest + File.separator + "result.avro");
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, file);
            stream.forEach(keyValue -> {
                try {
                    GenericRecord keyValueRecord = new GenericData.Record(schema);
                    keyValueRecord.put("key", keyValue.getKey());
                    keyValueRecord.put("value", keyValue.getValue());
                    dataFileWriter.append(keyValueRecord);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

}
