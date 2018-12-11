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
import com.ibm.mq.jms.MQQueueConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.io.ibmmq.source.exception.IBMMQSourceAdaptorRuntimeException;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * IBM MQ message consumer class which retrieve messages from the queue.
 **/
public class IBMMessageConsumerThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(IBMMessageConsumerThread.class);
    private SourceEventListener sourceEventListener;
    private MQConnection connection;
    private MessageConsumer messageConsumer;
    private volatile boolean paused;
    private volatile boolean inactive;
    private ReentrantLock lock;
    private Condition condition;
    private String queueName;
    private int maxRetryCount = 10;
    private long retryInterval = 1000;
    private IBMMessageConsumerBean ibmMessageConsumerBean;
    private MQQueueConnectionFactory mqQueueConnectionFactory;
    private IBMMQConnectionRetryHandler ibmmqConnectionRetryHandler;

    public IBMMessageConsumerThread(SourceEventListener sourceEventListener,
                                    IBMMessageConsumerBean ibmMessageConsumerBean,
                                    MQQueueConnectionFactory mqQueueConnectionFactory) throws JMSException {
        this.ibmMessageConsumerBean = ibmMessageConsumerBean;
        this.mqQueueConnectionFactory = mqQueueConnectionFactory;
        this.sourceEventListener = sourceEventListener;
        this.queueName = ibmMessageConsumerBean.getQueueName();
        this.ibmmqConnectionRetryHandler = new IBMMQConnectionRetryHandler(this,
                retryInterval, maxRetryCount);
        lock = new ReentrantLock();
        condition = lock.newCondition();
        connect();
    }

    @Override
    public void run() {
        while (!inactive) {
            try {
                if (paused) {
                    lock.lock();
                    try {
                        condition.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } finally {
                        lock.unlock();
                    }
                }
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
            } catch (JMSException e) {
                throw new IBMMQSourceAdaptorRuntimeException("Exception occurred during consuming messages " +
                        "from the queue: " + e.getMessage(), e);
            } catch (Throwable t) {
                logger.error("Exception occurred during consuming messages: " + t.getMessage(), t);
            }
        }
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void shutdownConsumer() {
        inactive = true;
        try {
            if (Objects.nonNull(messageConsumer)) {
                messageConsumer.close();
            }
        } catch (JMSException e) {
            logger.error("Error occurred while closing the consumer for the queue: " + queueName + ". ", e);

        }
        try {
            if (Objects.nonNull(connection)) {
                connection.close();
            }
        } catch (JMSException e) {
            logger.error("Error occurred while closing the IBM MQ connection for the queue: " + queueName + ". ", e);
        }
    }

    public void connect() throws JMSException {
        if (ibmMessageConsumerBean.isSecured()) {
            connection = (MQConnection) mqQueueConnectionFactory.createConnection(
                    ibmMessageConsumerBean.getUserName(), ibmMessageConsumerBean.getPassword());
        } else {
            connection = (MQConnection) mqQueueConnectionFactory.createConnection();
        }
        ExceptionListener exceptionListener = new ExceptionListener() {
            @Override
            public void onException(JMSException e) {
                logger.error("Exception was thrown from the IBMMQ connection.", e);
                if (!ibmmqConnectionRetryHandler.retry()) {
                    logger.error("Connection to the MQ provider failed after retrying for "
                            + ibmmqConnectionRetryHandler.getRetryCount() + " times");
                }
            }
        };
        connection.setExceptionListener(exceptionListener);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(ibmMessageConsumerBean.getDestinationName());
        this.messageConsumer = session.createConsumer(queue);
        this.connection.start();
    }
}
