package com.zipeiyi;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ReadBinlog2Hdfs Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>???? 22, 2016</pre>
 */
public class ReadBinlog2HdfsTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: process()
     */
    @Test
    public void testProcess() throws Exception {
        byte[] MAGIC_HEADER = new byte[]{-2, 98, 105, 110};
        System.out.println(MAGIC_HEADER.length);
        ReadBinlog2Hdfs reader = new ReadBinlog2Hdfs();
        reader.process();
}

    @Test
    public void getInsertData() throws Exception {
        ReadBinlog2Hdfs reader = new ReadBinlog2Hdfs();
        reader.getInsertData();
    }
} 
