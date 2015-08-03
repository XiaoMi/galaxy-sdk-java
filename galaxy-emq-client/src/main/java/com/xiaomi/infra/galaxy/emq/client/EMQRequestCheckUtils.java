package com.xiaomi.infra.galaxy.emq.client;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.xiaomi.infra.galaxy.emq.thrift.ChangeMessageVisibilityBatchRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ChangeMessageVisibilityBatchRequestEntry;
import com.xiaomi.infra.galaxy.emq.thrift.ChangeMessageVisibilityRequest;
import com.xiaomi.infra.galaxy.emq.thrift.CreateQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteMessageBatchRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteMessageBatchRequestEntry;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.GalaxyEmqServiceException;
import com.xiaomi.infra.galaxy.emq.thrift.GetQueueInfoRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ListPermissionsRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ListQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.MessageAttribute;
import com.xiaomi.infra.galaxy.emq.thrift.PurgeQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.QueryPermissionForIdRequest;
import com.xiaomi.infra.galaxy.emq.thrift.QueryPermissionRequest;
import com.xiaomi.infra.galaxy.emq.thrift.QueueAttribute;
import com.xiaomi.infra.galaxy.emq.thrift.RangeConstants;
import com.xiaomi.infra.galaxy.emq.thrift.ReceiveMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.RevokePermissionRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SendMessageBatchRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SendMessageBatchRequestEntry;
import com.xiaomi.infra.galaxy.emq.thrift.SendMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SetPermissionRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SetQueueAttributesRequest;
import com.xiaomi.infra.galaxy.emq.thrift.Version;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

public class EMQRequestCheckUtils {
  public static void checkRequest(Object[] objects)
      throws GalaxyEmqServiceException {
    if (objects.length == 1) {
      if (objects[0] instanceof CreateQueueRequest) {
        check((CreateQueueRequest) objects[0]);
      } else if (objects[0] instanceof DeleteQueueRequest) {
        check((DeleteQueueRequest) objects[0]);
      } else if (objects[0] instanceof PurgeQueueRequest) {
        check((PurgeQueueRequest) objects[0]);
      } else if (objects[0] instanceof SetQueueAttributesRequest) {
        check((SetQueueAttributesRequest) objects[0]);
      } else if (objects[0] instanceof GetQueueInfoRequest) {
        check((GetQueueInfoRequest) objects[0]);
      } else if (objects[0] instanceof ListQueueRequest) {
        check((ListQueueRequest) objects[0]);
      } else if (objects[0] instanceof SetPermissionRequest) {
        check((SetPermissionRequest) objects[0]);
      } else if (objects[0] instanceof RevokePermissionRequest) {
        check((RevokePermissionRequest) objects[0]);
      } else if (objects[0] instanceof QueryPermissionForIdRequest) {
        check((QueryPermissionForIdRequest) objects[0]);
      } else if (objects[0] instanceof QueryPermissionRequest) {
        check((QueryPermissionRequest) objects[0]);
      } else if (objects[0] instanceof ListPermissionsRequest) {
        check((ListPermissionsRequest) objects[0]);
      } else if (objects[0] instanceof SendMessageRequest) {
        check((SendMessageRequest) objects[0]);
      } else if (objects[0] instanceof ReceiveMessageRequest) {
        check((ReceiveMessageRequest) objects[0]);
      } else if (objects[0] instanceof ChangeMessageVisibilityRequest) {
        check((ChangeMessageVisibilityRequest) objects[0]);
      } else if (objects[0] instanceof DeleteMessageRequest) {
        check((DeleteMessageRequest) objects[0]);
      } else if (objects[0] instanceof SendMessageBatchRequest) {
        check((SendMessageBatchRequest) objects[0]);
      } else if (objects[0] instanceof DeleteMessageBatchRequest) {
        check((DeleteMessageBatchRequest) objects[0]);
      } else if (objects[0] instanceof ChangeMessageVisibilityBatchRequest) {
        check((ChangeMessageVisibilityBatchRequest) objects[0]);
      } else if (!(objects[0] instanceof Version)) {
        throw new GalaxyEmqServiceException().setErrMsg("Unknown Request");
      }
    } else if (objects.length > 1) {
      throw new GalaxyEmqServiceException().setErrMsg("Unknown Request");
    }
  }

  public static void check(CreateQueueRequest request)
      throws GalaxyEmqServiceException {
    checkNotEmpty(request.getQueueName(), "queue name");
    for (char c : request.getQueueName().toCharArray()) {
      if (!Character.isJavaIdentifierPart(c)) {
        throw new GalaxyEmqServiceException().setErrMsg("Invalid Queue Name").
            setDetails("invalid characters in queue name");
      }
    }
    if (request.getQueueAttribute() != null) {
      validateQueueAttribute(request.getQueueAttribute());
    }
  }

  public static void check(DeleteQueueRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(PurgeQueueRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(SetQueueAttributesRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    validateQueueAttribute(request.getQueueAttribute());
  }

  public static void check(GetQueueInfoRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(ListQueueRequest request)
      throws GalaxyEmqServiceException {
    validateQueueNamePrefix(request.getQueueNamePrefix());
  }

  public static void check(SetPermissionRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    checkNotEmpty(request.getDeveloperId(), "developerId");
  }

  public static void check(RevokePermissionRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    checkNotEmpty(request.getDeveloperId(), "developerId");
  }

  public static void check(QueryPermissionForIdRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    checkNotEmpty(request.getDeveloperId(), "developerId");
  }

  public static void check(QueryPermissionRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(ListPermissionsRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(SendMessageBatchRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    List<SendMessageBatchRequestEntry> entryList =
        request.getSendMessageBatchRequestEntryList();
    checkNotEmpty(entryList);
    Set<String> entryIdSet = new HashSet<String>(entryList.size());
    for (SendMessageBatchRequestEntry entry : entryList) {
      String entryId = entry.getEntryId();
      checkNotEmpty(entryId, "entityId");
      if (!entryIdSet.add(entryId)) {
        throw new GalaxyEmqServiceException().setErrMsg("Not Unique EntityId").
            setDetails("Duplicate entryId:" + entryId);
      }
      check(entry);
    }
  }

  public static void check(SendMessageBatchRequestEntry request)
      throws GalaxyEmqServiceException {
    checkNotEmpty(request.getMessageBody(), "message body");
    if (request.isSetDelaySeconds()) {
      checkParameterRange("delaySeconds", request.getDelaySeconds(),
          RangeConstants.GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MINIMAL,
          RangeConstants.GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MAXIMAL);
    }
    if (request.isSetInvisibilitySeconds()) {
      checkParameterRange("invisibilitySeconds", request.getInvisibilitySeconds(),
          RangeConstants.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MINIMAL,
          RangeConstants.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL);
    }
    if (request.isSetMessageAttributes()) {
      check(request.getMessageAttributes());
    }
  }

  public static void check(SendMessageRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    checkNotEmpty(request.getMessageBody(), "message body");
    if (request.isSetDelaySeconds()) {
      checkParameterRange("delaySeconds", request.getDelaySeconds(),
          RangeConstants.GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MINIMAL,
          RangeConstants.GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MAXIMAL);
    }
    if (request.isSetInvisibilitySeconds()) {
      checkParameterRange("invisibilitySeconds", request.getInvisibilitySeconds(),
          RangeConstants.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MINIMAL,
          RangeConstants.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL);
    }
    if (request.isSetMessageAttributes()) {
      check(request.getMessageAttributes());
    }
  }

  public static void check(List<MessageAttribute> attributeList)
      throws GalaxyEmqServiceException {
    if (attributeList != null) {
      Set<String> nameSet = new HashSet<String>(attributeList.size());
      for (MessageAttribute attribute : attributeList) {
        if (attribute.getType().toLowerCase().startsWith("string")) {
          if (attribute.getStringValue() == null) {
            throw new GalaxyEmqServiceException()
                .setErrMsg("Invalid user-defined attributes")
                .setDetails("stringValue cannot be null when type is STRING");
          }
        } else if (attribute.getType().toLowerCase().startsWith("binary")) {
          if (attribute.getBinaryValue() == null) {
            throw new GalaxyEmqServiceException()
                .setErrMsg("Invalid user-defined attributes")
                .setDetails("binaryValue cannot be null when type is BINARY");
          }
        } else {
          throw new GalaxyEmqServiceException()
              .setErrMsg("Invalid user-defined attributes")
              .setDetails("Attribute type must start with \"STRING\" or \"BINARY\"");
        }
        for (char c : attribute.getType().toCharArray()) {
          if (!Character.isLetter(c) && !Character.isDigit(c) && c != '.') {
            throw new GalaxyEmqServiceException()
                .setErrMsg("Invalid user-defined attributes")
                .setDetails("Invalid character \'" + c + "\' in attribute type");
          }
        }
        if (attribute.getName() == null || attribute.getName().isEmpty()) {
          throw new GalaxyEmqServiceException()
              .setErrMsg("Invalid user-defined attributes")
              .setDetails("Empty attribute name");
        }
        boolean notExist = nameSet.add(attribute.getName());
        if (!notExist) {
          throw new GalaxyEmqServiceException()
              .setErrMsg("Invalid user-defined attributes")
              .setDetails("Duplicate attribute name:" + attribute.getName());
        }
      }
    }
  }

  public static void check(ReceiveMessageRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    if (request.isSetMaxReceiveMessageNumber()) {
      checkParameterRange("receiveMessageMaximumNumber",
          request.getMaxReceiveMessageNumber(),
          RangeConstants.GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MINIMAL,
          RangeConstants.GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MAXIMAL);
    }
    if (request.isSetMaxReceiveMessageWaitSeconds()) {
      checkParameterRange("receiveMessageMaximumWaitSeconds",
          request.getMaxReceiveMessageWaitSeconds(),
          RangeConstants.GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MINIMAL,
          RangeConstants.GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MAXIMAL);
    }
  }

  public static void check(ChangeMessageVisibilityRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    checkNotEmpty(request.getReceiptHandle(), "receipt handle");
    checkParameterRange("invisibilitySeconds", request.getInvisibilitySeconds(),
        0, RangeConstants.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL);
  }

  public static void check(ChangeMessageVisibilityBatchRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    List<ChangeMessageVisibilityBatchRequestEntry> entryList =
        request.getChangeMessageVisibilityRequestEntryList();
    checkNotEmpty(entryList);
    Set<String> receiptHandleSet = new HashSet<String>(entryList.size());
    for (ChangeMessageVisibilityBatchRequestEntry entry : entryList) {
      String receiptHandle = entry.getReceiptHandle();
      checkNotEmpty(receiptHandle, "receipt handle");
      boolean notExist = receiptHandleSet.add(receiptHandle);
      if (!notExist) {
        throw new GalaxyEmqServiceException().setErrMsg("Not Unique ReceiptHandle").
            setDetails("Duplicate receiptHandle:" + receiptHandle);
      }
      checkParameterRange("invisibilitySeconds", entry.getInvisibilitySeconds(),
          0, RangeConstants.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL);
    }
  }

  public static void check(DeleteMessageRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    checkNotEmpty(request.getReceiptHandle(), "receipt handle");
  }

  public static void check(DeleteMessageBatchRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    List<DeleteMessageBatchRequestEntry> entryList =
        request.getDeleteMessageBatchRequestEntryList();
    checkNotEmpty(entryList);
    Set<String> receiptHandleSet = new HashSet<String>(entryList.size());
    for (DeleteMessageBatchRequestEntry entry : entryList) {
      String receiptHandle = entry.getReceiptHandle();
      checkNotEmpty(receiptHandle, "receipt handle");
      boolean notExist = receiptHandleSet.add(receiptHandle);
      if (!notExist) {
        throw new GalaxyEmqServiceException().setErrMsg("Not Unique ReceiptHandle").
            setDetails("Duplicate receiptHandle:" + receiptHandle);
      }
    }
  }

  public static void validateQueueAttribute(QueueAttribute attribute)
      throws GalaxyEmqServiceException {
    if (attribute.isSetDelaySeconds()) {
      checkParameterRange("delaySeconds", attribute.delaySeconds,
          RangeConstants.GALAXY_EMQ_QUEUE_DELAY_SECONDS_MINIMAL,
          RangeConstants.GALAXY_EMQ_QUEUE_DELAY_SECONDS_MAXIMAL);
    }
    if (attribute.isSetInvisibilitySeconds()) {
      checkParameterRange("invisibilitySeconds", attribute.invisibilitySeconds,
          RangeConstants.GALAXY_EMQ_QUEUE_INVISIBILITY_SECONDS_MINIMAL,
          RangeConstants.GALAXY_EMQ_QUEUE_INVISIBILITY_SECONDS_MAXIMAL);
    }
    if (attribute.isSetReceiveMessageWaitSeconds()) {
      checkParameterRange("receiveMessageWaitSeconds",
          attribute.receiveMessageWaitSeconds,
          RangeConstants.GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MINIMAL,
          RangeConstants.GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MAXIMAL);
    }
    if (attribute.isSetReceiveMessageMaximumNumber()) {
      checkParameterRange("receiveMessageMaximumNumber",
          attribute.receiveMessageMaximumNumber,
          RangeConstants.GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MINIMAL,
          RangeConstants.GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MAXIMAL);
    }
    if (attribute.isSetMessageRetentionSeconds()) {
      checkParameterRange("messageRetentionSeconds",
          attribute.messageRetentionSeconds,
          RangeConstants.GALAXY_EMQ_QUEUE_RETENTION_SECONDS_MINIMAL,
          RangeConstants.GALAXY_EMQ_QUEUE_RETENTION_SECONDS_MAXIMAL);
    }
    if (attribute.isSetMessageMaximumBytes()) {
      checkParameterRange("messageMaximumBytes",
          attribute.messageMaximumBytes,
          RangeConstants.GALAXY_EMQ_QUEUE_MAX_MESSAGE_BYTES_MINIMAL,
          RangeConstants.GALAXY_EMQ_QUEUE_MAX_MESSAGE_BYTES_MAXIMAL);
    }
    if (attribute.isSetPartitionNumber()) {
      checkParameterRange("partitionNumber",
          attribute.partitionNumber,
          RangeConstants.GALAXY_EMQ_QUEUE_PARTITION_NUMBER_MINIMAL,
          RangeConstants.GALAXY_EMQ_QUEUE_PARTITION_NUMBER_MAXIMAL);
    }
  }

  public static void validateQueueName(String queueName)
      throws GalaxyEmqServiceException {
    checkNotEmpty(queueName, "queue name");
    for (char c : queueName.toCharArray()) {
      if (!Character.isJavaIdentifierPart(c) && c != '/') {
        throw new GalaxyEmqServiceException().setErrMsg("Invalid Queue Name").
            setDetails("invalid characters in queue name");
      }
    }
    if (queueName.split("/").length != 2) {
      throw new GalaxyEmqServiceException().setErrMsg("Invalid Queue Name").
          setDetails("allowed exactly one '/' in queue name " + queueName);
    }
  }

  public static void validateQueueNamePrefix(String queueNamePrefix)
      throws GalaxyEmqServiceException {
    if (queueNamePrefix == null) {
      throw new GalaxyEmqServiceException().setErrMsg("null prefix");
    }
    for (char c : queueNamePrefix.toCharArray()) {
      if (!Character.isJavaIdentifierPart(c) && c != '/') {
        throw new GalaxyEmqServiceException().setErrMsg("Invalid queue name prefix")
            .setDetails("invalid characters in queueNamePrefix" + queueNamePrefix);
      }
    }
    int slashNum = queueNamePrefix.split("/").length;
    if (slashNum != 1 && slashNum != 2) {
      throw new GalaxyEmqServiceException().setErrMsg("Invalid queue name prefix")
          .setDetails("allowed at most one '/' in queueNamePrefix " + queueNamePrefix);
    }
  }

  public static void checkParameterRange(String parameter, int value,
      int minValue, int maxValue) throws GalaxyEmqServiceException {
    if (value < minValue || value > maxValue) {
      throw new GalaxyEmqServiceException().setErrMsg("Parameter Out of Range").
          setDetails(parameter + ":" + value + " should in range [" + minValue
              + ", " + maxValue + "]");
    }
  }

  public static void checkNotEmpty(List obj)
      throws GalaxyEmqServiceException {
    if (obj == null || obj.isEmpty()) {
      throw new GalaxyEmqServiceException().setErrMsg("empty list of entry");
    }
  }

  public static void checkNotEmpty(String obj, String name)
      throws GalaxyEmqServiceException {
    if (obj == null || obj.isEmpty()) {
      throw new GalaxyEmqServiceException().setErrMsg("empty " + name);
    }
  }
}
