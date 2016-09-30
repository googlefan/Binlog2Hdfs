package com.zipeiyi.hdfs.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class TestHdfs {

	public void WriteFile(String hdfs) throws IOException {
		 
		  System.setProperty("hadoop.home.dir", "D://hadoop-2.6.4");
		  //System.setProperty("hadoop.home.dir", "D://hadoop-2.6.0");
		  Configuration conf = new Configuration();
		  FileSystem fs = FileSystem.get(URI.create(hdfs),conf);
		  FSDataOutputStream hdfsOutStream = fs.create(new Path(hdfs));
		  hdfsOutStream.writeChars("1234r455555");
		  hdfsOutStream.close();
		  fs.close();
		 }
		 
		 public void ReadFile(String hdfs) throws IOException {
			 
		  Configuration conf = new Configuration();
		  FileSystem fs = FileSystem.get(URI.create(hdfs),conf);
		  FSDataInputStream hdfsInStream = fs.open(new Path(hdfs));
		  
		  byte[] ioBuffer = new byte[1024];
		  int readLen = hdfsInStream.read(ioBuffer);
		  while(readLen!=-1) {
			  readLen = hdfsInStream.read(ioBuffer);
		  }
		  hdfsInStream.close();
		  fs.close(); 
		 }
		  
		 public static void main(String[] args) throws IOException {  
		  String hdfs = "hdfs://10.0.140.22:9001/user/yinyu/test/tail2hdfs/dongyejun123.txt";
		  TestHdfs t = new TestHdfs();  
		  t.WriteFile(hdfs);
		  t.ReadFile(hdfs);
		 }
	}
