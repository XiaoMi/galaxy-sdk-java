package com.xiaomi.infra.galaxy.emq.client;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.xiaomi.infra.galaxy.emq.thrift.AddQueueAlertPolicyRequest;
import com.xiaomi.infra.galaxy.emq.thrift.AddTagAlertPolicyRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ChangeMessageVisibilityBatchRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ChangeMessageVisibilityBatchRequestEntry;
import com.xiaomi.infra.galaxy.emq.thrift.ChangeMessageVisibilityRequest;
import com.xiaomi.infra.galaxy.emq.thrift.CopyQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.CreateQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.CreateTagRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeadMessageBatchRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeadMessageBatchRequestEntry;
import com.xiaomi.infra.galaxy.emq.thrift.DeadMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteMessageBatchRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteMessageBatchRequestEntry;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeletePeekMessageBatchRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeletePeekMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteQueueAlertPolicyRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteTagAlertPolicyRequest;
import com.xiaomi.infra.galaxy.emq.thrift.DeleteTagRequest;
import com.xiaomi.infra.galaxy.emq.thrift.GalaxyEmqServiceException;
import com.xiaomi.infra.galaxy.emq.thrift.GetQueueDailyStatisticsStateRequest;
import com.xiaomi.infra.galaxy.emq.thrift.GetQueueInfoRequest;
import com.xiaomi.infra.galaxy.emq.thrift.GetTagInfoRequest;
import com.xiaomi.infra.galaxy.emq.thrift.GetUserInfoRequest;
import com.xiaomi.infra.galaxy.emq.thrift.GetUserQuotaRequest;
import com.xiaomi.infra.galaxy.emq.thrift.GetUserUsedQuotaRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ListDeadLetterSourceQueuesRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ListPermissionsRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ListQueueAlertPoliciesRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ListQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ListTagAlertPoliciesRequest;
import com.xiaomi.infra.galaxy.emq.thrift.ListTagRequest;
import com.xiaomi.infra.galaxy.emq.thrift.MessageAttribute;
import com.xiaomi.infra.galaxy.emq.thrift.PeekMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.PurgeQueueRequest;
import com.xiaomi.infra.galaxy.emq.thrift.QueryPermissionForIdRequest;
import com.xiaomi.infra.galaxy.emq.thrift.QueryPermissionRequest;
import com.xiaomi.infra.galaxy.emq.thrift.QueueAttribute;
import com.xiaomi.infra.galaxy.emq.thrift.QueueQuota;
import com.xiaomi.infra.galaxy.emq.thrift.ReceiveMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.RedrivePolicy;
import com.xiaomi.infra.galaxy.emq.thrift.RemoveQueueRedrivePolicyRequest;
import com.xiaomi.infra.galaxy.emq.thrift.RevokePermissionRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SendMessageBatchRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SendMessageBatchRequestEntry;
import com.xiaomi.infra.galaxy.emq.thrift.SendMessageRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SetPermissionRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SetQueueAttributesRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SetQueueDailyStatisticsStateRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SetQueueQuotaRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SetQueueRedrivePolicyRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SetUserInfoRequest;
import com.xiaomi.infra.galaxy.emq.thrift.SetUserQuotaRequest;
import com.xiaomi.infra.galaxy.emq.thrift.VerifyEMQAdminRoleRequest;
import com.xiaomi.infra.galaxy.emq.thrift.Version;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

public class EMQRequestCheckUtils {
  public static Map<String, Method> checkMethodMap;
  private static final char DASH = '-';
  private static final char SLASH = '/';

  static {
    checkMethodMap = new HashMap<String, Method>();
    for (Method method : EMQRequestCheckUtils.class.getMethods()) {
      if (method.getName().equals("check")) {
        checkMethodMap.put(method.getParameterTypes()[0].getName(), method);
      }
    }
  }

  public static void checkRequest(Object[] objects)
      throws Throwable {
    String requestName = objects[0].getClass().getName();
    if (objects.length == 1) {
      Method method = checkMethodMap.get(requestName);
      if (method != null) {
        try {
          method.invoke(null, objects[0]);
        } catch (InvocationTargetException e) {
          throw e.getTargetException();
        }
      } else {
        throw new GalaxyEmqServiceException().setErrMsg("Unknown request class:"
            + objects[0].getClass().getName());
      }
    } else if (objects.length > 1) {
      throw new GalaxyEmqServiceException().setErrMsg("Number of request" +
          " parameters is more than one:" + objects.length);
    }
  }

  public static void check(String queueName) throws GalaxyEmqServiceException {
    validateQueueName(queueName);
  }

  public static void check(CopyQueueRequest request) throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueMeta().getQueueName());
    if (request.getQueueMeta().getQueueAttribute() != null) {
      validateQueueAttribute(request.getQueueMeta().getQueueAttribute());
    }
    if (request.getQueueMeta().getQueueQuota() != null) {
      validateQueueQuota(request.getQueueMeta().getQueueQuota());
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
    if (request.getQueueQuota() != null) {
      validateQueueQuota(request.getQueueQuota());
    }
    if(request.getRedrivePolicy() != null) {
      validateRedrivePolicy(request.getRedrivePolicy());
    }
  }

  public static void check(PeekMessageRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(DeletePeekMessageRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(DeletePeekMessageBatchRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
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

  public static void check(SetQueueQuotaRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    validateQueueQuota(request.getQueueQuota());
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
          EMQClientConfigKeys.GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MAXIMAL);
    }
    if (request.isSetInvisibilitySeconds()) {
      checkParameterRange("invisibilitySeconds", request.getInvisibilitySeconds(),
          EMQClientConfigKeys.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL);
    }
    if (request.isSetMessageAttributes()) {
      for (MessageAttribute messageAttribute :
          request.getMessageAttributes().values()) {
        check(messageAttribute, false);
      }
    }
  }

  public static void check(SendMessageRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    checkNotEmpty(request.getMessageBody(), "message body");
    if (request.isSetDelaySeconds()) {
      checkParameterRange("delaySeconds", request.getDelaySeconds(),
          EMQClientConfigKeys.GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MAXIMAL);
    }
    if (request.isSetInvisibilitySeconds()) {
      checkParameterRange("invisibilitySeconds", request.getInvisibilitySeconds(),
          EMQClientConfigKeys.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL);
    }
    if (request.isSetMessageAttributes()) {
      for (MessageAttribute messageAttribute :
          request.getMessageAttributes().values()) {
        check(messageAttribute, false);
      }
    }
  }

  public static void check(MessageAttribute attribute, boolean allowEmpty)
      throws GalaxyEmqServiceException {
    if (attribute == null) {
      throw new GalaxyEmqServiceException()
          .setErrMsg("Message attribute is null");
    }
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
    } else if (allowEmpty && attribute.getType().equalsIgnoreCase("empty")) {
      return;
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
  }

  public static void check(ReceiveMessageRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    if (request.isSetMaxReceiveMessageNumber()) {
      checkParameterRange("receiveMessageMaximumNumber",
          request.getMaxReceiveMessageNumber(),
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MAXIMAL);
    }
    if (request.isSetMaxReceiveMessageWaitSeconds()) {
      checkParameterRange("receiveMessageMaximumWaitSeconds",
          request.getMaxReceiveMessageWaitSeconds(),
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MAXIMAL);
    }
    if (request.isSetAttributeName()) {
      checkNotEmpty(request.getAttributeName(), "attribute name");
      check(request.getAttributeValue(), true);
    }

    if (request.isSetTagName()) {
      validateTagName(request.getTagName());
    }
  }

  public static void check(ChangeMessageVisibilityRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    checkNotEmpty(request.getReceiptHandle(), "receipt handle");
    checkParameterRange("invisibilitySeconds", request.getInvisibilitySeconds(),
        0, EMQClientConfigKeys.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL);
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
          0, EMQClientConfigKeys.GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL);
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

  public static void check(DeadMessageRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    checkNotEmpty(request.getReceiptHandle(), "receipt handle");
  }

  public static void check(DeadMessageBatchRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    List<DeadMessageBatchRequestEntry> entryList =
        request.getDeadMessageBatchRequestEntryList();
    checkNotEmpty(entryList);
    Set<String> receiptHandleSet = new HashSet<String>(entryList.size());
    for (DeadMessageBatchRequestEntry entry : entryList) {
      String receiptHandle = entry.getReceiptHandle();
      checkNotEmpty(receiptHandle, "receipt handle");
      boolean notExist = receiptHandleSet.add(receiptHandle);
      if (!notExist) {
        throw new GalaxyEmqServiceException().setErrMsg("Not Unique ReceiptHandle").
            setDetails("Duplicate receiptHandle:" + receiptHandle);
      }
    }
  }

  public static void check(ListTagRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(GetTagInfoRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    if (request.isSetTagName()) {
      validateTagName(request.getTagName());
    }
  }

  public static void check(DeleteTagRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    validateTagName(request.getTagName());
  }

  public static void check(CreateTagRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    validateTagName(request.getTagName());

    if (request.isSetAttributeName()) {
      checkNotEmpty(request.getAttributeName(), "attribute name");
      check(request.getAttributeValue(), true);
    }

    if (request.isSetUserAttributes()) {
      validateUserAttributes(request.getUserAttributes());
    }

    checkParameterRange("tagReadQPS", request.getReadQPSQuota(),
        EMQClientConfigKeys.GALAXY_EMQ_QUEUE_READ_QPS_MINIMAL,
        EMQClientConfigKeys.GALAXY_EMQ_QUEUE_READ_QPS_MAXIMAL);
  }

  public static void check(AddQueueAlertPolicyRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(AddTagAlertPolicyRequest request)
          throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    if (request.isSetTagName()) {
      validateTagName(request.getTagName());
    }
  }

  public static void check(DeleteQueueAlertPolicyRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(DeleteTagAlertPolicyRequest request)
          throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    if (request.isSetTagName()) {
      validateTagName(request.getTagName());
    }
  }

  public static void check(ListQueueAlertPoliciesRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(ListTagAlertPoliciesRequest request)
          throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    if (request.isSetTagName()) {
      validateTagName(request.getTagName());
    }
  }

  public static void check(SetQueueDailyStatisticsStateRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(GetQueueDailyStatisticsStateRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(SetQueueRedrivePolicyRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
    validateRedrivePolicy(request.getRedrivePolicy());
  }

  public static void check(RemoveQueueRedrivePolicyRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getQueueName());
  }

  public static void check(ListDeadLetterSourceQueuesRequest request)
      throws GalaxyEmqServiceException {
    validateQueueName(request.getDlqName());
  }

  public static void check(VerifyEMQAdminRoleRequest request)
      throws GalaxyEmqServiceException {
    validateGranteeId(request.getGranteeId());
  }

  public static void check(Version request) {
  }

  public static void check(SetUserQuotaRequest request) {
  }

  public static void check(GetUserQuotaRequest request) {
  }

  public static void check(GetUserUsedQuotaRequest request) {
  }

  public static void check(SetUserInfoRequest request) {
  }

  public static void check(GetUserInfoRequest request) {
  }

  public static void validateQueueAttribute(QueueAttribute attribute)
      throws GalaxyEmqServiceException {
    if (attribute.isSetDelaySeconds()) {
      checkParameterRange("delaySeconds", attribute.delaySeconds,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_DELAY_SECONDS_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_DELAY_SECONDS_MAXIMAL);
    }
    if (attribute.isSetInvisibilitySeconds()) {
      checkParameterRange("invisibilitySeconds", attribute.invisibilitySeconds,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_INVISIBILITY_SECONDS_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_INVISIBILITY_SECONDS_MAXIMAL);
    }
    if (attribute.isSetReceiveMessageWaitSeconds()) {
      checkParameterRange("receiveMessageWaitSeconds",
          attribute.receiveMessageWaitSeconds,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MAXIMAL);
    }
    if (attribute.isSetReceiveMessageMaximumNumber()) {
      checkParameterRange("receiveMessageMaximumNumber",
          attribute.receiveMessageMaximumNumber,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MAXIMAL);
    }
    if (attribute.isSetMessageRetentionSeconds()) {
      checkParameterRange("messageRetentionSeconds",
          attribute.messageRetentionSeconds,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_RETENTION_SECONDS_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_RETENTION_SECONDS_MAXIMAL);
    }
    if (attribute.isSetMessageMaximumBytes()) {
      checkParameterRange("messageMaximumBytes",
          attribute.messageMaximumBytes,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_MAX_MESSAGE_BYTES_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_MAX_MESSAGE_BYTES_MAXIMAL);
    }
    if (attribute.isSetPartitionNumber()) {
      checkParameterRange("partitionNumber",
          attribute.partitionNumber,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_PARTITION_NUMBER_MINIMAL,
          EMQClientConfigKeys.GALAXY_EMQ_QUEUE_PARTITION_NUMBER_MAXIMAL);
    }

    if (attribute.isSetUserAttributes()) {
      validateUserAttributes(attribute.getUserAttributes());
    }
  }

  public static void validateRedrivePolicy(RedrivePolicy redrivePolicy)
      throws GalaxyEmqServiceException {
    validateQueueName(redrivePolicy.getDlqName());
    checkParameterRange("redrivePolicy maxReceiveTime",
        redrivePolicy.getMaxReceiveTime(),
        EMQClientConfigKeys.GALAXY_EMQ_QUEUE_REDRIVE_POLICY_MAX_RECEIVE_TIME_MINIMAL,
        EMQClientConfigKeys.GALAXY_EMQ_QUEUE_REDRIVE_POLICY_MAX_RECEIVE_TIME_MAXIMAL);
  }

  public static void validateQueueQuota(QueueQuota queueQuota)
      throws GalaxyEmqServiceException {
    if (queueQuota.isSetThroughput()) {
      if (queueQuota.getThroughput().isSetReadQps()) {
        checkParameterRange("queueReadQps", queueQuota.getThroughput().getReadQps(),
            EMQClientConfigKeys.GALAXY_EMQ_QUEUE_READ_QPS_MINIMAL,
            EMQClientConfigKeys.GALAXY_EMQ_QUEUE_READ_QPS_MAXIMAL);
      }
      if (queueQuota.getThroughput().isSetWriteQps()) {
        checkParameterRange("queueWriteQps", queueQuota.getThroughput().getWriteQps(),
            EMQClientConfigKeys.GALAXY_EMQ_QUEUE_WRITE_QPS_MINIMAL,
            EMQClientConfigKeys.GALAXY_EMQ_QUEUE_WRITE_QPS_MAXIMAL);
      }
    }

  }

  public static void validateQueueName(String queueName)
      throws GalaxyEmqServiceException {
    checkNotEmpty(queueName, "queue name");
    for (char c : queueName.toCharArray()) {
      if (!Character.isJavaIdentifierPart(c) && c != SLASH && c != DASH) {
        throw new GalaxyEmqServiceException().setErrMsg("Invalid Queue Name").
            setDetails("invalid characters in queue name");
      }
    }
    if (queueName.split("/").length != 2) {
      throw new GalaxyEmqServiceException().setErrMsg("Invalid Queue Name").
          setDetails("allowed exactly one '/' in queue name " + queueName);
    }
  }

  public static void validateTagName(String tagName)
      throws GalaxyEmqServiceException {
    checkNotEmpty(tagName, "tag name");
    for (char c : tagName.toCharArray()) {
      if (!Character.isJavaIdentifierPart(c)) {
        throw new GalaxyEmqServiceException().setErrMsg("Invalid Tag Name").
            setDetails("invalid characters in tag name");
      }
    }
  }

  public static void validateUserAttributes(Map<String, String> attribute)
      throws GalaxyEmqServiceException {
    for (Map.Entry<String, String> entry : attribute.entrySet()) {
      checkNotEmpty(entry.getKey(), "user attribute name");
      checkNotEmpty(entry.getValue(), "user attribute value for " + entry.getKey());
    }
  }

  public static void validateQueueNamePrefix(String queueNamePrefix)
      throws GalaxyEmqServiceException {
    if (queueNamePrefix == null) {
      throw new GalaxyEmqServiceException().setErrMsg("null prefix");
    }
    for (char c : queueNamePrefix.toCharArray()) {
      if (!Character.isJavaIdentifierPart(c) && c != SLASH && c != DASH) {
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

  public static void validateGranteeId(String granteeId)
      throws GalaxyEmqServiceException {
    if(granteeId != null) {
      if(!granteeId.startsWith("CI") && !granteeId.startsWith("U:")) {
        throw new GalaxyEmqServiceException().setErrMsg("Invalid granteeId prefix")
            .setDetails("invalid granteeId prefix: " + granteeId.substring(0, 2));
      }
      if(!StringUtils.isNumeric(granteeId.substring(2))) {
        throw new GalaxyEmqServiceException().setErrMsg("Invalid granteeId number")
            .setDetails("invalid granteeId number: " + granteeId.substring(2));
      }
    }
  }

  public static void checkParameterRange(String parameter, long value,
      long minValue, long maxValue) throws GalaxyEmqServiceException {
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
