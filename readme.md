Binlog2Hdfs

读取并解析binlog日志使用如下工具包 
<dependency>
    <groupId>com.github.shyiko</groupId>
    <artifactId>mysql-binlog-connector-java</artifactId>
    <version>0.4.2</version>
</dependency>

解析结果存放到hadoop系统下的hdfs文件中

初步：
    单线程去读写
未来：
    使用ExcutorService Future 多线程算法去实现