/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.admin;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.ChangeTopicAttributeRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicResponse;
import com.xiaomi.infra.galaxy.talos.thrift.DeleteUserQuotaRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DeleteTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicResponse;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.GetPartitionOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetPartitionOffsetResponse;
import com.xiaomi.infra.galaxy.talos.thrift.GetTopicOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetTopicOffsetResponse;
import com.xiaomi.infra.galaxy.talos.thrift.ListPermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.ListPermissionResponse;
import com.xiaomi.infra.galaxy.talos.thrift.ListUserQuotaResponse;
import com.xiaomi.infra.galaxy.talos.thrift.ListTopicsRequest;
import com.xiaomi.infra.galaxy.talos.thrift.ListTopicsResponse;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.OffsetInfo;
import com.xiaomi.infra.galaxy.talos.thrift.GetPermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetPermissionResponse;
import com.xiaomi.infra.galaxy.talos.thrift.QueryUserQuotaResponse;
import com.xiaomi.infra.galaxy.talos.thrift.QuotaService;
import com.xiaomi.infra.galaxy.talos.thrift.RevokePermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.SetPermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.SetUserQuotaRequest;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAttribute;
import com.xiaomi.infra.galaxy.talos.thrift.TopicInfo;
import com.xiaomi.infra.galaxy.talos.thrift.TopicService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;
import com.xiaomi.infra.galaxy.talos.thrift.UserQuota;

public class TalosAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(TalosAdmin.class);
  private TopicService.Iface topicClient;
  private MessageService.Iface messageClient;
  private QuotaService.Iface quotaClient;

  // used by guest
  public TalosAdmin(TalosClientConfig talosClientConfig) {
    this(talosClientConfig, new Credential());
  }

  // used by user/admin
  public TalosAdmin(TalosClientConfig talosClientConfig, Credential credential) {
    this(new TalosClientFactory(talosClientConfig, credential));
  }

  // used by producer/consumer
  public TalosAdmin(TalosClientFactory talosClientFactory) {
    topicClient = talosClientFactory.newTopicClient();
    messageClient = talosClientFactory.newMessageClient();
    quotaClient = talosClientFactory.newQuotaClient();
  }

  // topicAttribute for partitionNumber required
  public CreateTopicResponse createTopic(CreateTopicRequest request)
      throws GalaxyTalosException, TException {
    return topicClient.createTopic(request);
  }

  public Topic describeTopic(DescribeTopicRequest request)
      throws GalaxyTalosException, TException {
    DescribeTopicResponse describeTopicResponse =
        topicClient.describeTopic(request);
    return new Topic(describeTopicResponse.getTopicInfo(),
        describeTopicResponse.getTopicAttribute(),
        describeTopicResponse.getTopicState());
  }

  public void deleteTopic(DeleteTopicRequest request)
      throws GalaxyTalosException, TException {
    topicClient.deleteTopic(request);
  }

  public void changeTopicAttribute(ChangeTopicAttributeRequest request)
      throws GalaxyTalosException, TException {
    topicClient.changeTopicAttribute(request);
  }

  // add partitionNumber, an example of using changeTopicAttribute
  public void addTopicPartitionNumber(
      TopicTalosResourceName topicTalosResourceName, int partitionNumber)
      throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(topicTalosResourceName);
    Topic topic = describeTopic(new DescribeTopicRequest(
        Utils.getTopicNameByResourceName(
            topicTalosResourceName.getTopicTalosResourceName())));
    if (partitionNumber <= topic.getTopicAttribute().getPartitionNumber()) {
      // TODO: using specific GalaxyTalosException
      throw new IllegalArgumentException(
          "The partitionNumber to change must be more than the original number, got: " +
              partitionNumber + " the original: " +
              topic.getTopicAttribute().getPartitionNumber());
    }
    TopicAttribute topicAttribute = topic.getTopicAttribute();
    topicAttribute.setPartitionNumber(partitionNumber);
    changeTopicAttribute(new ChangeTopicAttributeRequest(
        topicTalosResourceName, topicAttribute));
  }

  public List<TopicInfo> listTopic(ListTopicsRequest request)
      throws GalaxyTalosException, TException {
    ListTopicsResponse listTopicsResponse = topicClient.listTopics(request);
    return listTopicsResponse.getTopicInfos();
  }

  public List<OffsetInfo> getTopicOffset(GetTopicOffsetRequest request)
      throws GalaxyTalosException, TException {
    GetTopicOffsetResponse response = messageClient.getTopicOffset(request);
    return response.getOffsetInfoList();
  }

  public OffsetInfo getPartitionOffset(GetPartitionOffsetRequest request)
      throws TException {
    GetPartitionOffsetResponse response = messageClient.getPartitionOffset(request);
    return response.getOffsetInfo();
  }

  public void setPermission(TopicTalosResourceName resourceName, String developerId,
      int permission) throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(resourceName);
    Preconditions.checkNotNull(developerId);
    Preconditions.checkArgument(permission > 0);
    SetPermissionRequest setPermissionRequest = new SetPermissionRequest(
        resourceName, developerId, permission);
    topicClient.setPermission(setPermissionRequest);
  }

  public void revokePermission(TopicTalosResourceName resourceName,
      String developerId) throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(resourceName);
    Preconditions.checkNotNull(developerId);
    RevokePermissionRequest revokePermissionRequest =
        new RevokePermissionRequest(resourceName, developerId);
    topicClient.revokePermission(revokePermissionRequest);
    LOG.info("Revoke permission success for: " + developerId);
  }

  public Map<String, Long> listPermission(TopicTalosResourceName resourceName)
      throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(resourceName);
    ListPermissionRequest listPermissionRequest =
        new ListPermissionRequest(resourceName);
    ListPermissionResponse listPermissionResponse =
        topicClient.listPermission(listPermissionRequest);
    if (listPermissionResponse == null) {
      LOG.warn("List permission got null response for: " +
          resourceName.getTopicTalosResourceName());
      return null;
    }
    return listPermissionResponse.getPermissions();
  }

  public long queryPermission(TopicTalosResourceName resourceName, String developerId)
      throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(resourceName);
    Preconditions.checkNotNull(developerId);
    GetPermissionRequest queryPermissionRequest = new GetPermissionRequest(
        resourceName, developerId);
    GetPermissionResponse queryPermissionResponse =
        topicClient.getPermission(queryPermissionRequest);
    if (queryPermissionResponse == null) {
      LOG.warn("Query permission got null response for: " + developerId);
      return -1;
    }
    return queryPermissionResponse.getPermission();
  }

  public void setUserQuota(String developerId, UserQuota userQuota)
      throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(developerId);
    Preconditions.checkNotNull(userQuota);
    SetUserQuotaRequest setQuotaRequest = new SetUserQuotaRequest(
        developerId, userQuota);
    quotaClient.setUserQuota(setQuotaRequest);
    LOG.info("Set quota success for: " + developerId);
  }

  public Map<String, UserQuota> listAllUserQuota()
      throws GalaxyTalosException, TException {
    ListUserQuotaResponse listQuotaResponse = quotaClient.listUserQuota();
    if (listQuotaResponse == null) {
      LOG.warn("List quota got null response");
      return null;
    }
    return listQuotaResponse.getUserQuotaList();
  }

  public void deleteUserQuota(String developerId)
      throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(developerId);
    DeleteUserQuotaRequest deleteQuotaRequest = new DeleteUserQuotaRequest(developerId);
    quotaClient.deleteUserQuota(deleteQuotaRequest);
    LOG.info("Delete quota success for: " + developerId);
  }

  public UserQuota queryUserQuota() throws GalaxyTalosException, TException {
    QueryUserQuotaResponse queryQuotaResponse = quotaClient.queryUserQuota();
    if (queryQuotaResponse == null) {
      LOG.warn("Query quota got null response");
      return null;
    }
    return queryQuotaResponse.getUserQuota();
  }

}
