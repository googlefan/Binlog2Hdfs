package com.zipeiyi;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ReadBinlogClient Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>���� 23, 2016</pre>
 */
public class ReadBinlogClientTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: readFromServer()
     */
    @Test
    public void testReadFromServer() throws Exception {
        ReadBinlogClient client = new ReadBinlogClient();
        client.readFromServer();
    }
    @Test
    public void testReadFromFilePosition() throws Exception {
        ReadBinlogClient client = new ReadBinlogClient();
        client.readFromFilePosition();
    }
} 
