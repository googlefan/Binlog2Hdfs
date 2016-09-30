package com.zipeiyi.hdfs.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class HDFSWriter {

    public void createBufferedWriter() {
        try {
            //Path pt = new Path("hdfs://10.0.140.22:9001/user/yinyu/test/tail2hdfs/");
            //FileSystem fs = FileSystem.get(new Configuration());
            Configuration configuration = new Configuration();
//            Path filePath = new Path("/user/yinyu/test/tail2hdfs/FlumeData.1474276600991");
            Path filePath = new Path("/user/yinyu/test/tail2hdfs/dsadsa.txt");
            configuration.addResource(filePath);
            FileSystem fs = FileSystem.get(new URI("hdfs://10.0.140.22:9001"), configuration);
            FSDataOutputStream fos = fs.create(filePath, true);
            fos.writeChars("dsadsadffff");
            fos.flush();
            fos.close();
//            FSDataInputStream fsDataInputStream = fs.open(filePath);
//            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(filePath, true)));
//            // TO append data to a file, use fs.append(Path f)
//            String line;
//            line = "Disha Dishu Daasha";
//            System.out.println(line);
//            br.write(line);
//            br.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("File not found");
        }
    }

}
