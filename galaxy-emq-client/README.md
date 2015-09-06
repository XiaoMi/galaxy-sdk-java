# 小米消息队列（EMQ）用户指南

## 小米消息队列（EMQ）简介

### EMQ综述

小米消息队列（Elastic Message Queue，简称EMQ），是小米生态云所提供的众多云服务其中一项。EMQ向小米生态云的使用者提供了高效、稳定、可靠、全面托管的分布式消息队列服务，并配以简洁易用的SDK，使得开发者可以方便快捷地在其上构建自己的应用，同时获得良好的可扩展性。

### EMQ特性

#### 分布式可扩展
使用EMQ的Message Sender和Message Receiver在逻辑上完全解耦。开发者完全可以通过动态地添加Sender或Receiver来提高某一队列的吞吐量

#### 高可用高可靠
EMQ自身采用分布式架构，自动进行故障迁移与负载均衡。对于已成功放入队列的消息，EMQ保证至少成功送达Receiver一次

#### 完善的认证授权机制
支持多种身份认证机制，提供细粒度的用户授权功能，实现不同用户之间的安全隔离

####多语言SDK支持
提供多种语言的SDK

####注
请使用JDK 1.6或者JDK 1.7进行编译

## EMQ的基本概念与操作

#### Queue
Queue是Message的传输通道，开发者创建并使用不同的Queue来传递不同逻辑类型的Message。开发者创建的所有Queue都位于自身的名字空间下。Queue有若干属性（attribute），开发者可以通过设置这些属性来更好地适合不同的Message传输需求。Queue同时也是用户授权的对象，开发者可以通过设置这些授权类型来向其他开发者共享队列中的数据。在任意时刻，开发者都可以通过SDK接口查询Queue中各种状态的Message的大致数量，了解Queue的运行状态。

#### Message
开发者通过Send/Receive操作来实现Queue的消息传递。每一条Message都包含String类型的Message Body，并可以设置自身的属性。如果开发者未设置Message的属性，则默认使用Queue中对应的属性。

#### Message Id
如果Sender向Queue中send Message成功，EMQ将为每条成功的Message返回一个全局（指这个Queue）唯一的id，作为这条消息的标识。

#### Receipt Handle
每条成功接收的Message中，除Message Body与Message Id外，还将包含一个全局唯一的Receipt Handle。Receiver正确处理这条消息后，需使用Receipt Handle从Queue中删除这条Message。否则，这条消息将在一段时间后，重新变成可读状态，并被Receiver再次接收。如果这种情况发生，再次接收到的Message的Receipt Handle将与前一次不同。

#### Invisibility Time
Queue中的一条Message被Receiver接收后，将在一段时间内变为不可见，并等待Receiver的删除确认，这段时间被称为Invisibility Time。如果在这段时间EMQ一直未收到确认，这条消息将重新变成可读，被最终被Receiver再次接收。收到Message的Receiver如果发现在这段时间内无法完成消息的处理，可以通过SDK延长Invisibility Time；延长的时间从当前时刻起算。如果Receiver不愿处理这条Message，可以通过SDK将Invisibility Time设为0，使Message立刻可见，被最终被再次读取。

#### Long Polling
默认状态下，EMQ收到Receive请求后，将查询分布式系统的一部分，并将查询到的这部分Message立即返回给Receiver。如果被查询的部分中暂时没有Message，将返回空。Receiver如果想减少空应答，可以通过SDK将Receive操作的Delay Time设为大于0的值，即Long Polling。这时EMQ将阻塞Receive Request，并查询整个分布式系统。当查询到Queue中的Message后，将立即返回给Receiver。如果Queue中一直没有可读的Message， EMQ将在Delay Time超时后，向用户返回空。

#### Batch Operation
为了减少网络交互次数，提高吞吐量，用户可以在一个Request中包含多条Message，使用Batch Operation来收发Message，并进行确认或改变Message可见时间。使用Batch Operation时，EMQ将返回一个List，分别标识每条Message是否被成功处理。

## 示例

      SendMessageRequest sendMessageRequest =
          new SendMessageRequest(queueName, messageBody);
      SendMessageResponse sendMessageResponse =
          messageClient.sendMessage(sendMessageRequest);

      ReceiveMessageRequest receiveMessageRequest =
          new ReceiveMessageRequest(queueName);
      List<ReceiveMessageResponse> receiveMessageResponse =
          messageClient.receiveMessage(receiveMessageRequest);

      // process receiveMessageResponse

      if (!receiveMessageResponse.isEmpty()) {
        DeleteMessageBatchRequest deleteMessageBatchRequest =
            new DeleteMessageBatchRequest();
        deleteMessageBatchRequest.setQueueName(queueName);
        for (ReceiveMessageResponse response : receiveMessageResponse) {
          deleteMessageBatchRequest.addToDeleteMessageBatchRequestEntryList(
              new DeleteMessageBatchRequestEntry(response.getReceiptHandle()));
        }
        messageClient.deleteMessageBatch(deleteMessageBatchRequest);
        System.out.print("Delete Messages.\n\n");
      }

完整示例参见com.xiaomi.infra.galaxy.emq.example.EMQExample
