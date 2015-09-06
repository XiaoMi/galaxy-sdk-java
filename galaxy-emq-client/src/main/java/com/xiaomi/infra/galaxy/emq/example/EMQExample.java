package com.xiaomi.infra.galaxy.emq.example;

import java.util.List;

import libthrift091.TException;

import com.xiaomi.infra.galaxy.emq.client.EMQClientFactory;
import com.xiaomi.infra.galaxy.emq.thrift.ChangeMessageVisibilityRequest;
import com.xiaomi.infra.galaxy.emq.thrift.CreateQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.CreateQueueResponse;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteMessageBatchRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteMessageBatchRequestEntry;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.MessageService;
import com.xiaomi.infra.galaxy.emq.thrift.QueueService;
import com.xiaomi.infra.galaxy.emq.thrift.ReceiveMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ReceiveMessageResponse;
import com.xiaomi.infra.galaxy.emq.thrift.SendMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SendMessageResponse;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

public class EMQExample {
  private static String secretKeyId = ""; // Set your AppKey
  private static String secretKey = ""; // Set your AppSecret
  private static String name = "testClient";

  public static void main(String[] args) {
    Credential credential = new Credential().setSecretKeyId(secretKeyId).
        setSecretKey(secretKey).setType(UserType.APP_SECRET);
    EMQClientFactory clientFactory = new EMQClientFactory(credential);
    QueueService.Iface queueClient = clientFactory.newQueueClient();
    MessageService.Iface messageClient = clientFactory.newMessageClient();

    try {
      CreateQueueRequest createQueueRequest = new CreateQueueRequest(name);
      CreateQueueResponse createQueueResponse = queueClient.createQueue(
          createQueueRequest);
      String queueName = createQueueResponse.getQueueName();

      String messageBody = "EMQExample";
      SendMessageRequest sendMessageRequest =
          new SendMessageRequest(queueName, messageBody);
      SendMessageResponse sendMessageResponse =
          messageClient.sendMessage(sendMessageRequest);
      System.out.printf("Send:\n  MessageBody: %s  MessageId: %s\n\n",
          messageBody, sendMessageResponse.getMessageID());

      ReceiveMessageRequest receiveMessageRequest =
          new ReceiveMessageRequest(queueName);
      List<ReceiveMessageResponse> receiveMessageResponse =
          messageClient.receiveMessage(receiveMessageRequest);
      for (ReceiveMessageResponse response : receiveMessageResponse) {
        System.out.printf(
            "Receive:\n  MessageBody: %s  MessageId: %s ReceiptHandle: %s\n\n",
            response.getMessageBody(), response.getMessageID(),
            response.getReceiptHandle());
      }

      if (!receiveMessageResponse.isEmpty()) {
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest =
            new ChangeMessageVisibilityRequest(queueName,
                receiveMessageResponse.get(0).getReceiptHandle(), 0);
        messageClient.changeMessageVisibilitySeconds(changeMessageVisibilityRequest);
        System.out.printf("Change Visibility\n  ReceiptHandle: %s " +
                "Time: %s seconds\n\n",
            changeMessageVisibilityRequest.getReceiptHandle(),
            changeMessageVisibilityRequest.getInvisibilitySeconds());
      }

      receiveMessageRequest.setMaxReceiveMessageWaitSeconds(5);
      receiveMessageResponse = messageClient.receiveMessage(receiveMessageRequest);
      for (ReceiveMessageResponse response : receiveMessageResponse) {
        System.out.printf(
            "Receive:\n  MessageBody: %s  MessageId: %s ReceiptHandle: %s\n\n",
            response.getMessageBody(), response.getMessageID(),
            response.getReceiptHandle());
      }

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

      DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(queueName);
      queueClient.deleteQueue(deleteQueueRequest);
    } catch (TException e) {
      System.out.printf("Failed." + e.getMessage() + "\n\n");
    }
  }
}
