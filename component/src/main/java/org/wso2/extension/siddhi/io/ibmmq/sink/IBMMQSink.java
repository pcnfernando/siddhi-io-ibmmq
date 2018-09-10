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

import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.io.ibmmq.sink.exception.IBMMQSinkAdaptorRuntimeException;
import org.wso2.extension.siddhi.io.ibmmq.util.IBMMQConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;

/**
 * IBM MQ Sink implementation
 **/

@Extension(
        name = "ibmmq",
        namespace = "sink",
        description = "IBM MQ Sink allows users to publish messages to an IBM MQ broker",
        parameters = {
                @Parameter(name = IBMMQConstants.DESTINATION_NAME,
                        description = "Queue name which IBM MQ sink should send events to",
                        type = DataType.STRING),
                @Parameter(name = IBMMQConstants.HOST,
                        description = "Host address of the MQ server",
                        type = DataType.STRING),
                @Parameter(name = IBMMQConstants.PORT,
                        description = "Port of the MQ server",
                        type = DataType.STRING),
                @Parameter(name = IBMMQConstants.CHANNEL,
                        description = "Channel used to connect to the MQ server",
                        type = DataType.STRING),
                @Parameter(name = IBMMQConstants.QUEUE_MANAGER_NAME,
                        description = "Name of the Queue Manager",
                        type = DataType.STRING),
                @Parameter(name = IBMMQConstants.USER_NAME,
                        description = "User name of the server. If this is not provided, " +
                                "will try to connect without both username and password",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "null"),
                @Parameter(name = IBMMQConstants.PASSWORD,
                        description = "Password of the server. If this is not provided, will try to connect without " +
                                "both username and password",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "null"),
        },
        examples = {
                @Example(description = "This example shows how to connect to an IBM MQ queue and "
                        + "send messages.",
                        syntax = "@sink(type='ibmmq',"
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

public class IBMMQSink extends Sink {
    private static final Logger LOG = LoggerFactory.getLogger(IBMMQSink.class);
    private OptionHolder optionHolder;
    private QueueConnection connection;
    private MQQueueConnectionFactory connectionFactory;
    private StreamDefinition outputStreamDefinition;
    private QueueSession session;
    private Queue queue;
    private QueueSender messageSender;
    private MessageConsumer consumer;
    private String userName;
    private String password;
    private String queueName;
    private boolean isSecured = false;
    private SiddhiAppContext siddhiAppContext;

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, Map.class, ByteBuffer.class};
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    @Override
    protected void init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder, ConfigReader
            sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.optionHolder = optionHolder;
        this.connectionFactory = new MQQueueConnectionFactory();
        this.queueName = optionHolder.validateAndGetStaticValue(IBMMQConstants.DESTINATION_NAME);
        this.userName = optionHolder.validateAndGetStaticValue(IBMMQConstants.USER_NAME,
                sinkConfigReader.readConfig(IBMMQConstants.USER_NAME, null));
        this.password = optionHolder.validateAndGetStaticValue(IBMMQConstants.PASSWORD,
                sinkConfigReader.readConfig(IBMMQConstants.PASSWORD, null));

        if (Objects.nonNull(userName) && Objects.nonNull(password)) {
            isSecured = true;
        }
        try {
            connectionFactory.setQueueManager(optionHolder.validateAndGetOption(IBMMQConstants.QUEUE_MANAGER_NAME).
                    getValue());
            connectionFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            connectionFactory.setPort(Integer.parseInt(optionHolder.validateAndGetOption(IBMMQConstants.PORT)
                    .getValue()));
            connectionFactory.setHostName(optionHolder.validateAndGetOption(IBMMQConstants.HOST).getValue());
            connectionFactory.setChannel(optionHolder.validateAndGetOption(IBMMQConstants.CHANNEL).getValue());
        } catch (JMSException e) {
            throw new IBMMQSinkAdaptorRuntimeException("Error while initializing IBM MQ sink: " + optionHolder.
                    validateAndGetOption(IBMMQConstants.DESTINATION_NAME).getValue() +
                    ", " + e.getMessage(), e);
        }

    }

    @Override
    public void publish(Object payload, DynamicOptions transportOptions) throws ConnectionUnavailableException {
        try {
            if (payload instanceof String) {
                Message message = session.createTextMessage(payload.toString());
                messageSender.send(message);

            } else if (payload instanceof Map) {
                MapMessage mapMessage = session.createMapMessage();
                ((Map) payload).forEach((key, value) -> {
                    try {
                        mapMessage.setString((String) key, (String) value);
                    } catch (JMSException e) {
                        throw new IBMMQSinkAdaptorRuntimeException("Exception occurred while publishing payload: " +
                                "key - '" + key + "', value - '" + value + "' from stream: '"
                                + outputStreamDefinition.getId() + "'. ", e);
                    }
                });
                messageSender.send(mapMessage);
            } else if (payload instanceof ByteBuffer) {
                byte[] data = ((ByteBuffer) payload).array();
                BytesMessage bytesMessage = session.createBytesMessage();
                bytesMessage.writeBytes(data);
                messageSender.send(bytesMessage);
            }
        } catch (JMSException e) {
            throw new IBMMQSinkAdaptorRuntimeException("Exception occurred while publishing payload: " +
                    payload.toString() + " , ", e);
        }
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        try {
            if (isSecured) {
                connection = (QueueConnection) connectionFactory.createConnection(userName, password);
            } else {
                connection = (QueueConnection) connectionFactory.createConnection();
            }
            session = (QueueSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            queue = session.createQueue(optionHolder.validateAndGetOption(IBMMQConstants.DESTINATION_NAME)
                    .getValue());
            consumer = session.createConsumer(queue);
            messageSender = session.createSender(queue);

        } catch (JMSException e) {
            throw new ConnectionUnavailableException("Exception occurred while connecting to the IBM MQ for queue: '"
                    + queueName + "' in siddhi app: '" + siddhiAppContext.getName() + "'. ", e);
        }
    }

    @Override
    public void disconnect() {
        try {
            if (Objects.nonNull(messageSender)) {
                messageSender.close();
            }
            if (Objects.nonNull(consumer)) {
                consumer.close();
            }
        } catch (JMSException e) {
            LOG.error("Error occurred while closing the consumer for the queue: " + queueName + ". ", e);

        }
        try {
            if (Objects.nonNull(connection)) {
                connection.close();
            }
        } catch (JMSException e) {
            LOG.error("Error occurred while closing the IBM MQ connection for the queue: " + queueName + ". ", e);
        }
    }

    @Override
    public void destroy() {
        // disconnect() gets called before destroy() which does the cleanup destroy() needs
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        //not available
    }
}
