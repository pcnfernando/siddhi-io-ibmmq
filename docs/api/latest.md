# API Docs - v1.0.7-SNAPSHOT

## Sink

### ibmmq *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>*

<p style="word-wrap: break-word">IBM MQ sink allows you to publish messages to an IBM MQ broker.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@sink(type="ibmmq", destination.name="<STRING>", host="<STRING>", port="<STRING>", channel="<STRING>", queue.manager="<STRING>", username="<STRING>", password="<STRING>", batch.properties="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">destination.name</td>
        <td style="vertical-align: top; word-wrap: break-word">The name of the queue to which the IBM MQ sink should send events.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">host</td>
        <td style="vertical-align: top; word-wrap: break-word">The host address of the MQ server.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">port</td>
        <td style="vertical-align: top; word-wrap: break-word">The port of the MQ server.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">channel</td>
        <td style="vertical-align: top; word-wrap: break-word">The channel used to connect to the MQ server.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">queue.manager</td>
        <td style="vertical-align: top; word-wrap: break-word">The name of the queue manager.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word">The username to connect to the server. If this is not provided, the connection is attempted without both the username and the password.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">password</td>
        <td style="vertical-align: top; word-wrap: break-word">The password to connect to the server. If this is not provided, the connection is attempted without both the username and the password.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">batch.properties</td>
        <td style="vertical-align: top; word-wrap: break-word">IBM MQ properties which are supported by the client can be provided as key value pairs which is separated by ",". as an example batch.properties = 'XMSC_WMQ_CLIENT_RECONNECT_OPTIONS:1600,WMQ_CLIENT_RECONNECT:5005'.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type='ibmmq',destination.name='Queue1',host='192.168.56.3',port='1414',channel='Channel1',queue.manager = 'ESBQManager',password='1920',username='mqm',batch.properties = 'XMSC_WMQ_CLIENT_RECONNECT_OPTIONS:1600,WMQ_CLIENT_RECONNECT:5005',@map(type='text'))define stream SweetProductionStream(name string, amount double);
```
<p style="word-wrap: break-word">This example shows how to connect to an IBM MQ queue and send messages.</p>

## Source

### ibmmq *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*

<p style="word-wrap: break-word">IBM MQ source allows you to subscribe to an IBM message queue and receive messages. It has the ability to receive messages of the 'map' and 'text' message formats.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(type="ibmmq", destination.name="<STRING>", host="<STRING>", port="<STRING>", channel="<STRING>", queue.manager="<STRING>", username="<STRING>", password="<STRING>", worker.count="<INT>", batch.properties="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">destination.name</td>
        <td style="vertical-align: top; word-wrap: break-word">The name of the queue name to which the IBM MQ source should subscribe.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">host</td>
        <td style="vertical-align: top; word-wrap: break-word">The host address of the IBM MQ server.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">port</td>
        <td style="vertical-align: top; word-wrap: break-word">The port of the IBM MQ server.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">channel</td>
        <td style="vertical-align: top; word-wrap: break-word">The channel used to connect to the MQ server.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">queue.manager</td>
        <td style="vertical-align: top; word-wrap: break-word">The name of the queue manager.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word">The username to connect to the server. If this is not provided, the connection is attempted without both the username and the password.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">password</td>
        <td style="vertical-align: top; word-wrap: break-word">The password to connect to the server. If this is not provided, the connection is attempted without both the username and the password.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">worker.count</td>
        <td style="vertical-align: top; word-wrap: break-word">Number of worker threads listening on the given queue. When the multiple workers are enabled event ordering is not preserved.</td>
        <td style="vertical-align: top">1</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">batch.properties</td>
        <td style="vertical-align: top; word-wrap: break-word">IBM MQ properties which are supported by the client can be provided as key value pairs which is separated by ",". as an example batch.properties = 'XMSC_WMQ_CLIENT_RECONNECT_OPTIONS:1600,WMQ_CLIENT_RECONNECT:5005' </td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type='ibmmq',destination.name='Queue1',host='192.168.56.3',port='1414',channel='Channel1',queue.manager = 'ESBQManager',password='1920',username='mqm',batch.properties = 'XMSC_WMQ_CLIENT_RECONNECT_OPTIONS:1600,WMQ_CLIENT_RECONNECT:5005',@map(type='text'))define stream SweetProductionStream(name string, amount double);
```
<p style="word-wrap: break-word">This exampe shows how to connect to an IBM message queue and receive messages.</p>

