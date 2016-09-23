package com.zipeiyi;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by zhuhui on 2016/9/22.
 */
public class ReadBinlog2Hdfs {
    public ReadBinlog2Hdfs() throws IOException {
    }

    public void process() throws IOException {
        File binlogFile = new File("src/test/resources/mysql-bin.001239");
        BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile);
        try {
            for (Event event; (event = reader.readEvent()) != null; ) {
                System.out.println(event.getHeader());
                System.out.println(event.getData());
                System.out.println(event.toString());
            }
        } finally {
            reader.close();
        }
    }

    public void getInsertData() throws IOException {
        EventDeserializer eventDeserializer = new EventDeserializer();
        // 反序列化，对不必要的事件类型不去系列化，以此增强程序性能。
        eventDeserializer.setEventDataDeserializer(EventType.QUERY, new NullEventDataDeserializer());
        File binlogFile = new File("src/test/resources/mysql-bin.001239");
        BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, eventDeserializer);
        try {
            for (Event event; (event = reader.readEvent()) != null; ) {
                EventType eventType = event.getHeader().getEventType();
                System.out.println(event.getHeader());
                System.out.println(event.toString());
                if (eventType == EventType.WRITE_ROWS) {
                    WriteRowsEventData wrtiteRowsEvent = (WriteRowsEventData) event.getData();
                    List<Serializable[]> rows = wrtiteRowsEvent.getRows();
                    System.out.println(wrtiteRowsEvent.toString());
                }
                if (eventType == EventType.QUERY) {
                    QueryEventData queryEventData = (QueryEventData) event.getData();
                    String querySql = queryEventData.getSql();
                    System.out.println(querySql);
                }
                if (eventType == EventType.DELETE_ROWS) {
                    DeleteRowsEventData deleteRowsEventData = (DeleteRowsEventData) event.getData();
                    List<Serializable[]> rows = deleteRowsEventData.getRows();
                    System.out.println(deleteRowsEventData.toString());
                }
                if (eventType == EventType.UPDATE_ROWS) {
                    UpdateRowsEventData updateRowsEventData = (UpdateRowsEventData) event.getData();
                    List<Map.Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
                    System.out.println(updateRowsEventData.toString());
                }
                if (eventType == EventType.INTVAR) {
                    IntVarEventData intVarEventData = (IntVarEventData) event.getData();
                    intVarEventData.getValue();
                    System.out.println(intVarEventData.toString());
                }
            }
        } finally {
            reader.close();
        }
    }

}
