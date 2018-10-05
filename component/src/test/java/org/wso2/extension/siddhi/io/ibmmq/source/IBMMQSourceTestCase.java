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

package org.wso2.extension.siddhi.io.ibmmq.source;

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.concurrent.atomic.AtomicInteger;

public class IBMMQSourceTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(IBMMQSourceTestCase.class);
    private static AtomicInteger actualEventCount = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        actualEventCount.set(0);
    }

    @Test
    public void sourceTestCase1() throws InterruptedException {
        LOG.info("IBM MQ Source Test case 1 - Mandatory field test case with username and password");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;

        String inStreamDefinition = "@source(type='ibmmq',\n" +
                "        destination.name='Queue1',\n" +
                "        host='192.168.56.3',\n" +
                "        port='1414',\n" +
                "        channel='Channel1',\n" +
                "        queue.manager = 'ESBQManager',\n" +
                "        username = 'mqm',\n" +
                "        password = '1920',\n" +
                "        @map(type='xml'))\n" +
                "define stream SweetProductionStream(name string);\n";
        String outStream = "define stream outStream(name string); \n";

        String query = "@info(name='query2') \n" +
                "from SweetProductionStream select * insert into outStream;";

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + outStream + query);
        InputHandler inputStream = siddhiAppRuntime.getInputHandler("SweetProductionStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                actualEventCount.incrementAndGet();
                EventPrinter.print(timestamp, inEvents, removeEvents);
            }
        });
        siddhiAppRuntime.start();
        inputStream.send(new Object[]{"event1"});
        waitTillVariableCountMatches(1, Duration.ONE_MINUTE);
        siddhiManager.shutdown();
    }

    @Test
    public void sourceTestCase2() throws InterruptedException {
        LOG.info("IBM MQ Source Test case 2 - Multiple worker test case");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;
        String siddhiApp = "@App:name(\"IBMMessageQueueSample\")\n";
        String ibmmqInStreamDefinition = "@source(type='ibmmq',\n" +
                "        destination.name='Queue1',\n" +
                "        host='192.168.56.3',\n" +
                "        port='1414',\n" +
                "        channel='Channel1',\n" +
                "        queue.manager = 'ESBQManager',\n" +
                "        username = 'mqm',\n" +
                "        password = '1920',\n" +
                "        worker.count = '5',\n" +
                "        @map(type='xml'))\n" +
                "define stream SweetProductionStreamIn(name string);\n";
        String ibmmqOutStreamDefinition = "@sink(type='ibmmq',\n" +
                "        destination.name='Queue1',\n" +
                "        host='192.168.56.3',\n" +
                "        port='1414',\n" +
                "        channel='Channel1',\n" +
                "        queue.manager = 'ESBQManager',\n" +
                "        username = 'mqm',\n" +
                "        password = '1920',\n" +
                "        @map(type='xml'))\n" +
                "define stream SweetProductionStreamOut(name string);\n";
        String inStreamDefinition = "define stream InStreamDefinition(name string);\n";
        String outStreamDefinition = "define stream OutStreamDefinition(name string); \n";

        String query = "@info(name='query1') \n" +
                "from InStreamDefinition select * insert into SweetProductionStreamOut;" +
                "@info(name='query2') \n" +
                "from SweetProductionStreamIn select * insert into OutStreamDefinition;";
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp + ibmmqInStreamDefinition +
                ibmmqOutStreamDefinition + inStreamDefinition + outStreamDefinition + query);
        InputHandler inputStream = siddhiAppRuntime.getInputHandler("InStreamDefinition");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        actualEventCount.incrementAndGet();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        inputStream.send(new Object[]{"event1"});
        inputStream.send(new Object[]{"event2"});
        inputStream.send(new Object[]{"event3"});
        inputStream.send(new Object[]{"event4"});
        inputStream.send(new Object[]{"event5"});
        waitTillVariableCountMatches(5, Duration.ONE_MINUTE);
        siddhiManager.shutdown();
    }

    private static void waitTillVariableCountMatches(long expected, Duration duration) {
        Awaitility.await().atMost(duration).until(() -> {
            return actualEventCount.get() == expected;
        });
    }
}
