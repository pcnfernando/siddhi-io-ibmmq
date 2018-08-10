/*
 *  Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.wso2.extension.siddhi.io.ibmmq.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import javax.jms.JMSException;

public class IBMMQSinkTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(IBMMQSinkTestCase.class);
    private volatile int count;

    @BeforeMethod
    public void init() {
        count = 0;
    }

    @Test
    public void sinkTestCase1() throws InterruptedException, JMSException {
        LOG.info("IBM MQ Sink Test case 1 - Mandatory field test case with username and password");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime;

        String inStreamDefinition = "@sink(type='ibmmq',\n" +
                "        destination.name='Queue1',\n" +
                "        host='192.168.56.3',\n" +
                "        port='1414',\n" +
                "        channel='Channel1',\n" +
                "        queue.manager = 'ESBQManager',\n" +
                "        username = 'mqm',\n" +
                "        password = '1920',\n" +
                "        @map(type='xml'))\n" +
                "define stream SweetProductionStream(name string);";

        String query = "@info(name='query1')" +
                "from SweetProductionStream select * insert into outStream;";

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        InputHandler inputStream = siddhiAppRuntime.getInputHandler("SweetProductionStream");
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                count = inEvents.length;
                EventPrinter.print(timestamp, inEvents, removeEvents);
                AssertJUnit.assertEquals(1, count);
            }
        });
        siddhiAppRuntime.start();

        inputStream.send(new Object[]{"event1"});
        AssertJUnit.assertEquals(1, count);
        siddhiManager.shutdown();
    }
}
