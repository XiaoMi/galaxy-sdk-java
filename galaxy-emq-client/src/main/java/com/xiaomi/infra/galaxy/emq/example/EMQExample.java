package com.xiaomi.infra.galaxy.emq.example;

import java.util.ArrayList;
import java.util.List;

import com.xiaomi.infra.galaxy.emq.client.EMQClientFactory;
import com.xiaomi.infra.galaxy.emq.thrift.ChangeMessageVisibilityBatchRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ChangeMessageVisibilityBatchRequestEntry;
import com.xiaomi.infra.galaxy.emq.thrift.CreateQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.CreateQueueResponse;
import com.xiaomi.infra.galaxy.emq.thrift.CreateTagRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteMessageBatchRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteMessageBatchRequestEntry;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteTagRequest;
import com.xiaomi.infra.galaxy.emq.thrift.GalaxyEmqServiceException;
import com.xiaomi.infra.galaxy.emq.thrift.MessageService;
import com.xiaomi.infra.galaxy.emq.thrift.QueueService;
import com.xiaomi.infra.galaxy.emq.thrift.ReceiveMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ReceiveMessageResponse;
import com.xiaomi.infra.galaxy.emq.thrift.SendMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SendMessageResponse;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;

import static com.xiaomi.infra.galaxy.emq.client.EMQClientFactory.generateHttpClient;
import libthrift091.TException;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

/* This JAVA SDK support Java6 and Java7; Java8 is not included yet */

public class EMQExample {
  private static String secretKeyId = ""; // Set your AppKey, like "5521728123579"

  private static String secretKey = ""; // Set your AppSecret, like "K7czwCuHttwrZD49DD/qKz=="
  private static String name = "testClient";

  public static void main(String[] args) {
    Credential credential = new Credential().setSecretKeyId(secretKeyId).
        setSecretKey(secretKey).setType(UserType.APP_SECRET);
    EMQClientFactory clientFactory = new EMQClientFactory(credential,
        generateHttpClient(10, 10));
    QueueService.Iface queueClient = clientFactory.newQueueClient(
        "http://awsbj0.emq.api.xiaomi.com");
    MessageService.Iface messageClient = clientFactory.newMessageClient(
        "http://awsbj0.emq.api.xiaomi.com");

    try {
      CreateQueueRequest createQueueRequest = new CreateQueueRequest(name);
      CreateQueueResponse createQueueResponse = queueClient.createQueue(
          createQueueRequest);
      String queueName = createQueueResponse.getQueueName();

      String tagName = "tagTest";
      CreateTagRequest createTagRequest = new CreateTagRequest(queueName, tagName);
      queueClient.createTag(createTagRequest);

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
          new ArrayList<ReceiveMessageResponse>();
      while (receiveMessageResponse.isEmpty()) {
        receiveMessageResponse =
            messageClient.receiveMessage(receiveMessageRequest);
      }
      DeleteMessageBatchRequest deleteMessageBatchRequest =
          new DeleteMessageBatchRequest();
      deleteMessageBatchRequest.setQueueName(queueName);
      for (ReceiveMessageResponse response : receiveMessageResponse) {
        System.out.printf(
            "Receive from default:\n  MessageBody: %s  MessageId: %s " +
                "ReceiptHandle: %s\n\n",
            response.getMessageBody(), response.getMessageID(),
            response.getReceiptHandle());
        deleteMessageBatchRequest.addToDeleteMessageBatchRequestEntryList(
            new DeleteMessageBatchRequestEntry(response.getReceiptHandle()));
      }
      messageClient.deleteMessageBatch(deleteMessageBatchRequest);

      receiveMessageRequest = new ReceiveMessageRequest(queueName);
      receiveMessageRequest.setTagName(tagName);
      receiveMessageResponse.clear();
      while (receiveMessageResponse.isEmpty()) {
        receiveMessageResponse =
            messageClient.receiveMessage(receiveMessageRequest);
      }
      ChangeMessageVisibilityBatchRequest changeRequest =
          new ChangeMessageVisibilityBatchRequest();
      changeRequest.setQueueName(queueName);
      for (ReceiveMessageResponse response : receiveMessageResponse) {
        System.out.printf(
            "Receive from tag:\n  MessageBody: %s  MessageId: %s " +
                "ReceiptHandle: %s\n\n",
            response.getMessageBody(), response.getMessageID(),
            response.getReceiptHandle());
        changeRequest.addToChangeMessageVisibilityRequestEntryList(
            new ChangeMessageVisibilityBatchRequestEntry(
                response.getReceiptHandle(), 0));
      }

      messageClient.changeMessageVisibilitySecondsBatch(changeRequest);
      System.out.printf("Change Visibility.\n\n");

      receiveMessageRequest.setMaxReceiveMessageWaitSeconds(5);
      receiveMessageResponse = messageClient.receiveMessage(receiveMessageRequest);
      for (ReceiveMessageResponse response : receiveMessageResponse) {
        System.out.printf(
            "Receive from tag:\n  MessageBody: %s  MessageId: %s ReceiptHandle: %s\n\n",
            response.getMessageBody(), response.getMessageID(),
            response.getReceiptHandle());
      }

      if (!receiveMessageResponse.isEmpty()) {
        deleteMessageBatchRequest = new DeleteMessageBatchRequest();
        deleteMessageBatchRequest.setQueueName(queueName);
        for (ReceiveMessageResponse response : receiveMessageResponse) {
          deleteMessageBatchRequest.addToDeleteMessageBatchRequestEntryList(
              new DeleteMessageBatchRequestEntry(response.getReceiptHandle()));
        }
        messageClient.deleteMessageBatch(deleteMessageBatchRequest);
        System.out.print("Delete Messages.\n\n");
      }

      DeleteTagRequest deleteTagRequest = new DeleteTagRequest(queueName, tagName);
      queueClient.deleteTag(deleteTagRequest);

      DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(queueName);
      queueClient.deleteQueue(deleteQueueRequest);
    } catch (TException e) {
      if (e instanceof GalaxyEmqServiceException) {
        GalaxyEmqServiceException ex = (GalaxyEmqServiceException) e;
        System.out.printf("Failed. Reason:" + ex.getErrMsg() + "\n" +
            ex.getDetails() + " requestId=" + ex.getRequestId() + "\n\n");
      } else {
        System.out.printf("Failed." + e.getMessage() + "\n\n");
      }
    }
  }
}
