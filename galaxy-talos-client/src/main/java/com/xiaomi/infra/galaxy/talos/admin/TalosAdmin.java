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
import com.xiaomi.infra.galaxy.talos.thrift.DeleteTopicQuotaRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DeleteTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DeleteUserQuotaRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicResponse;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.GetPartitionOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetPartitionOffsetResponse;
import com.xiaomi.infra.galaxy.talos.thrift.GetPermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetPermissionResponse;
import com.xiaomi.infra.galaxy.talos.thrift.GetTopicOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetTopicOffsetResponse;
import com.xiaomi.infra.galaxy.talos.thrift.ListPermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.ListPermissionResponse;
import com.xiaomi.infra.galaxy.talos.thrift.ListTopicsRequest;
import com.xiaomi.infra.galaxy.talos.thrift.ListTopicsResponse;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.OffsetInfo;
import com.xiaomi.infra.galaxy.talos.thrift.QueryTopicQuotaRequest;
import com.xiaomi.infra.galaxy.talos.thrift.QuotaService;
import com.xiaomi.infra.galaxy.talos.thrift.RevokePermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.SetPermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.SetTopicQuotaRequest;
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
        describeTopicResponse.getTopicState())
        .setTopicQuota(describeTopicResponse.getTopicQuota())
        .setTopicAcl(describeTopicResponse.getAclMap());
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

  public void setPermission(SetPermissionRequest request)
      throws GalaxyTalosException, TException {
    Preconditions.checkArgument(request.getPermission().getValue() > 0);
    topicClient.setPermission(request);
  }

  public void revokePermission(RevokePermissionRequest request)
      throws GalaxyTalosException, TException {
    topicClient.revokePermission(request);
  }

  public Map<String, Integer> listPermission(ListPermissionRequest request)
      throws GalaxyTalosException, TException {
    ListPermissionResponse listPermissionResponse =
        topicClient.listPermission(request);
    return listPermissionResponse.getPermissions();
  }

  public int queryPermission(GetPermissionRequest request)
      throws GalaxyTalosException, TException {
    GetPermissionResponse queryPermissionResponse =
        topicClient.getPermission(request);
    return queryPermissionResponse.getPermission();
  }

  public void setUserQuota(SetUserQuotaRequest request)
      throws GalaxyTalosException, TException {
    quotaClient.setUserQuota(request);
  }

  public Map<String, UserQuota> listAllUserQuota()
      throws GalaxyTalosException, TException {
    return quotaClient.listUserQuota().getUserQuotaList();
  }

  public void deleteUserQuota(DeleteUserQuotaRequest request)
      throws GalaxyTalosException, TException {
    quotaClient.deleteUserQuota(request);
  }

  public UserQuota queryUserQuota() throws GalaxyTalosException, TException {
    return quotaClient.queryUserQuota().getUserQuota();
  }

  public void setTopicQuota(SetTopicQuotaRequest request)
      throws GalaxyTalosException, TException {
    topicClient.setTopicQuota(request);
  }

  public void queryTopicQuota(QueryTopicQuotaRequest request)
      throws GalaxyTalosException, TException {
    topicClient.queryTopicQuota(request);
  }

  public void deleteTopicQuota(DeleteTopicQuotaRequest request)
      throws GalaxyTalosException, TException {
    topicClient.deleteTopicQuota(request);
  }

  public String getDeveloperId() throws GalaxyTalosException, TException {
    return topicClient.getDeveloperId().getDeveloperId();
  }
}
