# 代码介绍
1.使用MQTT协议完成了publisher和subscriber订阅和接受消息的过程。该代码用于讨论在publisher数量，QoS等级和消息发送间隔对于通信质量的影响。 

2.该代码会遍历180种QoS，publisher数量和延迟的组合。

3.先运行analyzer再运行publisher。analyzer会在特定的topic下面发布控制信息，publisher会根据这些控制信息来进行消息的发布。最后analyzer再从另外的topic下面接受消息，并进行本地保存，以便于最后进行统计和分析。