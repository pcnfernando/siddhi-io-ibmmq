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

import com.ibm.mq.jms.MQConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.io.ibmmq.source.exception.IBMMQInputAdaptorRuntimeException;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

/**
 * IBM MQ message consumer class which retrieve messages from the queue.
 **/
public class IBMMessageConsumer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(IBMMessageConsumer.class);
    private SourceEventListener sourceEventListener;
    private MQConnection connection;
    private MessageConsumer messageConsumer;
    private Boolean isPaused = false;
    private ScheduledFuture scheduledFuture;


    public IBMMessageConsumer(SourceEventListener sourceEventListener, MQConnection connection,
                              MessageConsumer messageConsumer) throws JMSException {
        this.sourceEventListener = sourceEventListener;
        this.connection = connection;
        this.messageConsumer = messageConsumer;
        this.connection.start();
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (!isPaused) {
                    Message message = messageConsumer.receive();
                    if (message instanceof MapMessage) {
                        Map<String, Object> event = new HashMap<>();
                        MapMessage mapEvent = (MapMessage) message;
                        Enumeration<String> mapNames = mapEvent.getMapNames();
                        while (mapNames.hasMoreElements()) {
                            String key = mapNames.nextElement();
                            event.put(key, mapEvent.getObject(key));
                        }
                        sourceEventListener.onEvent(event, null);
                    } else if (message instanceof TextMessage) {
                        String event = ((TextMessage) message).getText();
                        sourceEventListener.onEvent(event, null);
                    } else if (message instanceof ByteBuffer) {
                        sourceEventListener.onEvent(message, null);
                    }
                } else {
                    scheduledFuture.wait();
                    return;
                }
            } catch (JMSException e) {
                throw new IBMMQInputAdaptorRuntimeException("Exception has occurred during consuming messages " +
                        "from the queue: " + e.getMessage(), e);
            } catch (InterruptedException e) {
                LOGGER.error("Error while pausing the consuming messages: "
                        + e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
        }
    }


    void pause() {
        isPaused = true;
    }

    void resume() {
        isPaused = false;

    }

    void kill() {
        scheduledFuture.cancel(true);
    }

    void consume(IBMMessageConsumer consumer, ScheduledExecutorService scheduledExecutorService) {
        scheduledFuture = scheduledExecutorService.schedule(consumer, 0, TimeUnit.MILLISECONDS);
    }


}
