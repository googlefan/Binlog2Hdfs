package com.zipeiyi;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        BinaryLogClient client = new BinaryLogClient("10.0.140.22", 3306, "root", "1qaz@WSX?");
        // 可设计断点续传
        client.setBinlogFilename("mysql-bin.001238");
        //client.setBinlogPosition(107);
        client.registerEventListener(new BinaryLogClient.EventListener() {
            String patternStr = "'.*'";
            Pattern reg = Pattern.compile(patternStr);

            @Override
            public void onEvent(Event event) {
                EventType eventType = event.getHeader().getEventType();
                if (eventType == EventType.WRITE_ROWS) {
                    WriteRowsEventData wrtiteRowsEvent = (WriteRowsEventData) event.getData();
                    BitSet bs = wrtiteRowsEvent.getIncludedColumns();
                    List<Serializable[]> rows = wrtiteRowsEvent.getRows();
                    for (Serializable[] row : rows) {
                        for (Serializable s : row) {
                            System.out.println(s.toString());
                        }
                    }
                } else if (eventType == EventType.QUERY) {
                    QueryEventData queryEventData = (QueryEventData) event.getData();
                    // 分析insert 语句抽取 字段值
                    String querySql = queryEventData.getSql();
                    if (querySql.startsWith("INSERT")) {
                        Matcher m = reg.matcher(querySql);
                        while (m.find()) {
                            for (String str : m.group().split(",")) {
                                System.out.println(str);
                            }
                        }
                    }

                } else if (eventType == EventType.DELETE_ROWS) {
                    DeleteRowsEventData deleteRowsEventData = (DeleteRowsEventData) event.getData();
                    List<Serializable[]> rows = deleteRowsEventData.getRows();
                    for (Serializable[] row : rows) {
                        for (Serializable s : row) {
                            System.out.println(s.toString());
                        }
                    }
                } else if (eventType == EventType.UPDATE_ROWS) {
                    UpdateRowsEventData updateRowsEventData = (UpdateRowsEventData) event.getData();
                    List<Map.Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
                    for (Map.Entry<Serializable[], Serializable[]> row : rows) {
                        //  获取更新后的表的数据
                        Serializable[] slb = row.getValue();
                        for (Serializable s : slb) {
                            System.out.println(s.toString());
                        }
                    }
                }
                System.out.println(event.getHeader());
                System.out.println(event.toString());
            }
        });
        client.connect();
    }
}
