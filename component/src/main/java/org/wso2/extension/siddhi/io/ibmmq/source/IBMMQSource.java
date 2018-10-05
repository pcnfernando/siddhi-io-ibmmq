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

import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.io.ibmmq.source.exception.IBMMQSourceAdaptorRuntimeException;
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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import javax.jms.JMSException;

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
                @Parameter(name = IBMMQConstants.WORKER_COUNT,
                        description = "Number of worker threads listening on the given queue.",
                        type = DataType.INT,
                        optional = true,
                        defaultValue = "1"),
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
    private static final Logger logger = LoggerFactory.getLogger(IBMMQSource.class);
    private SourceEventListener sourceEventListener;
    private MQQueueConnectionFactory connectionFactory;
    private IBMMessageConsumerGroup ibmMessageConsumerGroup;
    private ScheduledExecutorService scheduledExecutorService;
    private IBMMessageConsumerBean ibmMessageConsumerBean = new IBMMessageConsumerBean();


    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                     String[] requestedTransportPropertyNames, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.connectionFactory = new MQQueueConnectionFactory();
        ibmMessageConsumerBean.setQueueName(optionHolder.validateAndGetStaticValue(IBMMQConstants.DESTINATION_NAME));
        ibmMessageConsumerBean.setUserName(optionHolder.validateAndGetStaticValue(IBMMQConstants.USER_NAME,
                configReader.readConfig(IBMMQConstants.USER_NAME, null)));
        ibmMessageConsumerBean.setPassword(optionHolder.validateAndGetStaticValue(IBMMQConstants.PASSWORD,
                configReader.readConfig(IBMMQConstants.PASSWORD, null)));
        ibmMessageConsumerBean.setWorkerCount(Integer.parseInt(optionHolder.validateAndGetStaticValue(
                IBMMQConstants.WORKER_COUNT, "1")));
        ibmMessageConsumerBean.setDestinationName(optionHolder.validateAndGetOption(IBMMQConstants.DESTINATION_NAME)
                .getValue());
        if (Objects.nonNull(ibmMessageConsumerBean.getPassword()) &&
                Objects.nonNull(ibmMessageConsumerBean.getUserName())) {
            ibmMessageConsumerBean.setSecured(true);
        }
        try {
            connectionFactory.setChannel(optionHolder.validateAndGetOption(IBMMQConstants.CHANNEL).getValue());
            connectionFactory.setHostName(optionHolder.validateAndGetOption(IBMMQConstants.HOST).getValue());
            connectionFactory.setPort(Integer.parseInt(optionHolder.
                    validateAndGetOption(IBMMQConstants.PORT).getValue()));
            connectionFactory.setQueueManager(optionHolder.validateAndGetOption(IBMMQConstants.QUEUE_MANAGER_NAME)
                    .getValue());
            connectionFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            scheduledExecutorService = siddhiAppContext.getScheduledExecutorService();
        } catch (JMSException e) {
            throw new IBMMQSourceAdaptorRuntimeException("Error while initializing IBM MQ source: " + optionHolder.
                    validateAndGetOption(IBMMQConstants.DESTINATION_NAME).getValue() + ", " + e.getMessage(), e);
        }

    }

    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        //ConnectionCallback is not used as re-connection is handled by IBM MQ client.
        ibmMessageConsumerGroup = new IBMMessageConsumerGroup(scheduledExecutorService,
                connectionFactory, ibmMessageConsumerBean);
        ibmMessageConsumerGroup.run(sourceEventListener);
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, Map.class, ByteBuffer.class};
    }

    @Override
    public void disconnect() {
        if (ibmMessageConsumerGroup != null) {
            ibmMessageConsumerGroup.shutdown();
            logger.info("IBM MQ source disconnected for queue '" + ibmMessageConsumerBean.getQueueName() + "'");
        }
    }

    @Override
    public void destroy() {
        ibmMessageConsumerGroup = null;
        scheduledExecutorService.shutdown();
    }

    @Override
    public void pause() {
        if (ibmMessageConsumerGroup != null) {
            ibmMessageConsumerGroup.pause();
            logger.info("IBM MQ source paused for queue '" + ibmMessageConsumerBean.getQueueName() + "'");
        }
    }

    @Override
    public void resume() {
        if (ibmMessageConsumerGroup != null) {
            ibmMessageConsumerGroup.resume();
            if (logger.isDebugEnabled()) {
                logger.debug("Kafka Adapter resumed for queue '" + ibmMessageConsumerBean.getQueueName() + "'");
            }
        }
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
