siddhi-io-ibmmq
======================================

The **siddhi-io-ibmmq extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that used to receive and publish events to ibm Message queue.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-ibmmq">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-ibmmq/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-ibmmq/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-ibmmq/api/1.0.1">1.0.1</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

***Prerequisites for using the feature***

- Using this with WSO2 Stream Processor 

    - Download and install IBM MQ Websphere v7.5.0 or higher
    - Create a queue manager, queue, and a channel. 
    - Download [com.ibm.mq.allclient_9.0.5.0_1.0.0.jar](http://central.maven.org/maven2/com/ibm/mq/com.ibm.mq.allclient/9.0.5.0/com.ibm.mq.allclient-9.0.5.0.jar) and [javax.jms-api-2.0.1.jar] (http://central.maven.org/maven2/javax/jms/javax.jms-api/2.0.1/javax.jms-api-2.0.1.jar).
    and copy to the `<SP_HOME>/lib` directory
                
 
- You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

- This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-ibmmq/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.io.ibmmq</groupId>
        <artifactId>siddhi-io-ibmmq</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-ibmmq/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-ibmmq/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-ibmmq/api/1.0.1/#ibmmq-sink">ibmmq</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>*<br><div style="padding-left: 1em;"><p>IBM MQ Sink allows users to publish messages to an IBM MQ broker</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-ibmmq/api/1.0.1/#ibmmq-source">ibmmq</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*<br><div style="padding-left: 1em;"><p>IBM MQ Source allows users to subscribe to a IBM message queue and receive messages. It has the ability to receive Map messages and Text messages.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-ibmmq/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-ibmmq/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
