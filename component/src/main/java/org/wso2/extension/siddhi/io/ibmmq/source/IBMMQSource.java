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
import com.ibm.msg.client.wmq.WMQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.io.ibmmq.source.exception.IBMMQInputAdaptorRuntimeException;
import org.wso2.extension.siddhi.io.ibmmq.util.IBMMQConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

/**
 * IBM MQ Source implementation.
 */
@Extension(
        name = "ibmmq",
        namespace = "source",
        description = "IBM MQ Source allows users to subscribe to a IBM message queue and receive messages. It has the "
                + "ability to receive Map messages and Text messages.",
        parameters = {
                @Parameter(name = IBMMQConstants.DESTINATION_NAME,
                        description = "Queue name which IBM MQ Source should subscribe to",
                        type = DataType.STRING),
                @Parameter(name = IBMMQConstants.HOST,
                        description = "Host address of the IBM MQ server",
                        type = DataType.STRING),
                @Parameter(name = IBMMQConstants.PORT,
                        description = "Port of the IBM MQ server",
                        type = DataType.STRING),
                @Parameter(name = IBMMQConstants.CHANNEL,
                        description = "Channel of the IBM MQ server which should use to connect",
                        type = DataType.STRING),
                @Parameter(name = IBMMQConstants.QUEUE_MANAGER_NAME,
                        description = "Name of the Queue Manager",
                        type = DataType.STRING),
                @Parameter(name = IBMMQConstants.USER_NAME,
                        description = "User name of the server",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "will try to connect without username"),
                @Parameter(name = IBMMQConstants.PASSWORD,
                        description = "Password of the server",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "will try to connect without username"),

        },
        examples = {
                @Example(description = "This example shows how to connect to an IBM message queue and "
                        + "receive messages.",
                        syntax = "@source(type='ibmmq',"
                                + "destination.name='Queue1',"
                                + "host='192.168.56.3',"
                                + "port='1414',"
                                + "channel='Channel1',"
                                + "queue.manager = 'ESBQManager',"
                                + "password='1920',"
                                + "username='mqm',"
                                + "@map(type='text'))"
                                + "define stream SweetProductionStream(name string, amount double);"),
        }
)

public class IBMMQSource extends Source {
    private static final Logger LOG = LoggerFactory.getLogger(IBMMQSource.class);
    private SourceEventListener sourceEventListener;
    private OptionHolder optionHolder;
    private MQConnection connection;
    private MQQueueConnectionFactory connectionFactory;
    private Session session;
    private Queue queue;
    private MessageConsumer consumer;
    private IBMMessageConsumer ibmMessageConsumer;
    private ScheduledExecutorService scheduledExecutorService;
    private String userName;
    private String password;
    private boolean isSecured = false;


    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                     String[] requestedTransportPropertyNames, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.optionHolder = optionHolder;
        connectionFactory = new MQQueueConnectionFactory();
        try {

            if (!"null".equalsIgnoreCase(optionHolder.validateAndGetStaticValue(IBMMQConstants.USER_NAME, "null")) &&
                    !"null".equalsIgnoreCase(optionHolder.validateAndGetStaticValue(IBMMQConstants.PASSWORD, "null"))) {
                isSecured = true;
                userName = optionHolder.validateAndGetOption(IBMMQConstants.USER_NAME).getValue();
                password = optionHolder.validateAndGetOption(IBMMQConstants.PASSWORD).getValue();
            }
            connectionFactory.setChannel(optionHolder.validateAndGetOption(IBMMQConstants.CHANNEL).getValue());
            connectionFactory.setHostName(optionHolder.validateAndGetOption(IBMMQConstants.HOST).getValue());
            connectionFactory.setPort(Integer.parseInt(optionHolder.
                    validateAndGetOption(IBMMQConstants.PORT).getValue()));
            connectionFactory.setQueueManager(optionHolder.validateAndGetOption(IBMMQConstants.QUEUE_MANAGER_NAME)
                    .getValue());
            connectionFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            scheduledExecutorService = siddhiAppContext.getScheduledExecutorService();
        } catch (JMSException e) {
            throw new IBMMQInputAdaptorRuntimeException("Error while initializing IBM MQ source: " + optionHolder.
                    validateAndGetOption(IBMMQConstants.DESTINATION_NAME).getValue() + ", " + e.getMessage(), e);
        }

    }

    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        //ConnectionCallback is not used as re-connection is handled by carbon transport.
        try {
            if (isSecured) {
                connection = (MQConnection) connectionFactory.createConnection(userName, password);
            } else {
                connection = (MQConnection) connectionFactory.createConnection();
            }
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            queue = session.createQueue(optionHolder.validateAndGetOption(IBMMQConstants.DESTINATION_NAME)
                    .getValue());
            consumer = session.createConsumer(queue);
            ibmMessageConsumer = new IBMMessageConsumer(sourceEventListener, connection, consumer);
            ibmMessageConsumer.consume(ibmMessageConsumer, scheduledExecutorService);

        } catch (JMSException e) {
            throw new ConnectionUnavailableException("Exception in starting the IBM MQ receiver for stream: "
                    + sourceEventListener.getStreamDefinition().getId(), e);
        }
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, Map.class};
    }

    @Override
    public void disconnect() {
        try {
            if (Objects.nonNull(connection)) {
                connection.close();
                ibmMessageConsumer.kill();
            }
            if (Objects.nonNull(consumer)) {
                consumer.close();
            }
        } catch (JMSException e) {
            LOG.error("Error disconnecting the IBM MQ connection ", e);
        }
    }

    @Override
    public void destroy() {
        // disconnect() gets called before destroy() which does the cleanup destroy() needs
    }

    @Override
    public void pause() {
        ibmMessageConsumer.pause();
    }

    @Override
    public void resume() {
        ibmMessageConsumer.resume();
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        // no state to restore
    }
}
