package com.zipeiyi;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created by Administrator on 2016/9/23.
 */
public class ReadBinlogClient {

    public void readFromServer() throws IOException {
        BinaryLogClient client = new BinaryLogClient("10.0.140.43", 3306, "root", "1qaz@WSX?");
        client.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                EventType eventType = event.getHeader().getEventType();
                System.out.println(event.getHeader());
                System.out.println(event.toString());
            }
        });
        client.connect();
    }

    public void readFromFilePosition() throws IOException, TimeoutException {
        BinaryLogClient client = new BinaryLogClient("10.0.140.43", 3306, "root", "1qaz@WSX?");
        // 可设计断点续传
        client.setBinlogFilename("mysql-bin.000004");
        client.setBinlogPosition(107);
        client.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                EventType eventType = event.getHeader().getEventType();
                if (eventType == EventType.WRITE_ROWS) {
                    WriteRowsEventData wrtiteRowsEvent = (WriteRowsEventData) event.getData();
                    List<Serializable[]> rows = wrtiteRowsEvent.getRows();
                    System.out.println(wrtiteRowsEvent.toString());
                } else if (eventType == EventType.QUERY) {
                    QueryEventData queryEventData = (QueryEventData) event.getData();
                    String querySql = queryEventData.getSql();
                    // 分析insert 语句抽取 字段值
                } else if (eventType == EventType.DELETE_ROWS) {
                    DeleteRowsEventData deleteRowsEventData = (DeleteRowsEventData) event.getData();
                    List<Serializable[]> rows = deleteRowsEventData.getRows();
                    System.out.println(deleteRowsEventData.toString());
                } else if (eventType == EventType.UPDATE_ROWS) {
                    UpdateRowsEventData updateRowsEventData = (UpdateRowsEventData) event.getData();
                    List<Map.Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
                    System.out.println(updateRowsEventData.toString());
                } else if (eventType == EventType.INTVAR) {
                    IntVarEventData intVarEventData = (IntVarEventData) event.getData();
                    intVarEventData.getValue();
                    System.out.println(intVarEventData.toString());
                }
                System.out.println(event.getHeader());
                System.out.println(event.toString());
            }
        });
        client.connect(1000000);
    }
}
