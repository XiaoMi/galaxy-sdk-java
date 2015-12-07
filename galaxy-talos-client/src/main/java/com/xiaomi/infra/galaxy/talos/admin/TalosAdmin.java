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
import com.xiaomi.infra.galaxy.talos.thrift.AddPermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.AddPermissionResponse;
import com.xiaomi.infra.galaxy.talos.thrift.ChangeTopicAttributeRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicResponse;
import com.xiaomi.infra.galaxy.talos.thrift.DeletePermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DeletePermissionResponse;
import com.xiaomi.infra.galaxy.talos.thrift.DeleteQuotaRequest;
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
import com.xiaomi.infra.galaxy.talos.thrift.ListQuotaResponse;
import com.xiaomi.infra.galaxy.talos.thrift.ListTopicsRequest;
import com.xiaomi.infra.galaxy.talos.thrift.ListTopicsResponse;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.OffsetInfo;
import com.xiaomi.infra.galaxy.talos.thrift.QueryPermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.QueryPermissionResponse;
import com.xiaomi.infra.galaxy.talos.thrift.QueryQuotaResponse;
import com.xiaomi.infra.galaxy.talos.thrift.QuotaService;
import com.xiaomi.infra.galaxy.talos.thrift.RevokePermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.SetPermissionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.SetQuotaRequest;
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

  public void setPermission(TopicInfo topicInfo, String developerId,
      long permission) throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(topicInfo);
    Preconditions.checkNotNull(developerId);
    Preconditions.checkArgument(permission > 0);
    SetPermissionRequest setPermissionRequest = new SetPermissionRequest(
        topicInfo, developerId, permission);
    topicClient.setPermission(setPermissionRequest);
  }

  public void revokePermission(TopicInfo topicInfo, String developerId)
      throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(topicInfo);
    Preconditions.checkNotNull(developerId);
    RevokePermissionRequest revokePermissionRequest =
        new RevokePermissionRequest(topicInfo, developerId);
    topicClient.revokePermission(revokePermissionRequest);
    LOG.info("Revoke permission success for: " + developerId);
  }

  public long addPermission(TopicInfo topicInfo, String developerId,
      long permission) throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(topicInfo);
    Preconditions.checkNotNull(developerId);
    Preconditions.checkArgument(permission > 0);
    AddPermissionRequest addPermissionRequest = new AddPermissionRequest(
        topicInfo, developerId, permission);
    AddPermissionResponse addPermissionResponse =
        topicClient.addPermission(addPermissionRequest);
    if (addPermissionResponse == null) {
      LOG.warn("Add permission got null response for: " + developerId);
      return -1;
    }
    LOG.info("Add permission success for: " + developerId);
    return addPermissionResponse.getPermission();
  }

  public long deletePermission(TopicInfo topicInfo, String developerId,
      long permission) throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(topicInfo);
    Preconditions.checkNotNull(developerId);
    Preconditions.checkArgument(permission > 0);
    DeletePermissionRequest deletePermissionRequest =
        new DeletePermissionRequest(topicInfo, developerId, permission);
    DeletePermissionResponse deletePermissionResponse =
        topicClient.deletePermission(deletePermissionRequest);
    if (deletePermissionResponse == null) {
      LOG.warn("Delete permission got null response for: " + developerId);
      return -1;
    }
    LOG.info("Delete permission success for: " + developerId);
    return deletePermissionResponse.getPermission();
  }

  public Map<String, Long> listPermission(TopicInfo topicInfo)
      throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(topicInfo);
    ListPermissionRequest listPermissionRequest = new ListPermissionRequest(topicInfo);
    ListPermissionResponse listPermissionResponse =
        topicClient.listPermission(listPermissionRequest);
    if (listPermissionResponse == null) {
      LOG.warn("List permission got null response for: " +
          topicInfo.getTopicTalosResourceName().getTopicTalosResourceName());
      return null;
    }
    return listPermissionResponse.getPermissions();
  }

  public long queryPermission(TopicInfo topicInfo, String developerId)
      throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(topicInfo);
    Preconditions.checkNotNull(developerId);
    QueryPermissionRequest queryPermissionRequest = new QueryPermissionRequest(
        topicInfo, developerId);
    QueryPermissionResponse queryPermissionResponse =
        topicClient.queryPermission(queryPermissionRequest);
    if (queryPermissionResponse == null) {
      LOG.warn("Query permission got null response for: " + developerId);
      return -1;
    }
    return queryPermissionResponse.getPermission();
  }

  public void setQuota(String developerId, UserQuota userQuota)
      throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(developerId);
    Preconditions.checkNotNull(userQuota);
    SetQuotaRequest setQuotaRequest = new SetQuotaRequest(developerId, userQuota);
    quotaClient.setQuota(setQuotaRequest);
    LOG.info("Set quota success for: " + developerId);
  }

  public Map<String, UserQuota> listQuota() throws GalaxyTalosException, TException {
    ListQuotaResponse listQuotaResponse = quotaClient.listQuota();
    if (listQuotaResponse == null) {
      LOG.warn("List quota got null response");
      return null;
    }
    return listQuotaResponse.getQuotas();
  }

  public void deleteQuota(String developerId) throws GalaxyTalosException, TException {
    Preconditions.checkNotNull(developerId);
    DeleteQuotaRequest deleteQuotaRequest = new DeleteQuotaRequest(developerId);
    quotaClient.deleteQuota(deleteQuotaRequest);
    LOG.info("Delete quota success for: " + developerId);
  }

  public UserQuota queryQuota() throws GalaxyTalosException, TException {
    QueryQuotaResponse queryQuotaResponse = quotaClient.queryQuota();
    if (queryQuotaResponse == null) {
      LOG.warn("Query quota got null response");
      return null;
    }
    return queryQuotaResponse.getUserQuota();
  }

  // TODO: add function listQuota need argument developerId
  // TODO: clearly specify all exceptions thrown
  // TODO: change all function to just wrap them by remaining prototype of functions:
  //       for example, public void deleteQuota(DeleteQuotaRequest deleteQuotaRequest) {
  //                      quotaClient.deleteQuota(deleteQuotaRequest);
  // }

}
