/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.codelab.talos;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.Grantee;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.thrift.ChangeTopicAttributeRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicResponse;
import com.xiaomi.infra.galaxy.talos.thrift.DeleteTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoResponse;
import com.xiaomi.infra.galaxy.talos.thrift.GetWorkerIdRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetWorkerIdResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Permission;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAttribute;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosAdminDemo {

  private static final Logger LOG = LoggerFactory.getLogger(TalosAdminDemo.class);
  // authenticate for team
  private static final String accessKey = "$your_team_accessKey";
  private static final String accessSecret = "$your_team_accessSecret";

  // another teamId used to be grant permission
  private static final String anotherTeamId = "$anotherTeamId";

  private static final String topicName = "testTopic";
  private static final String orgId = "$your_org_id";
  // attention that the topic name to be created is 'orgId/topicName'
  private static final String cloudTopicName = orgId + "/" + topicName;
  private static final int partitionNumber = 8;
  private static final String consumerGroup = "groupName";

  private TopicTalosResourceName resourceName;
  private Credential credential;

  private TalosAdmin talosAdmin;

  public TalosAdminDemo() throws TException {
    Properties properties = new Properties();
    properties.setProperty("galaxy.talos.service.endpoint", "$serviceURI");
    TalosClientConfig clientConfig = new TalosClientConfig(properties);

    // credential
    credential = new Credential();
    credential.setSecretKeyId(accessKey)
        .setSecretKey(accessSecret)
        .setType(UserType.DEV_XIAOMI);

    // init admin
    talosAdmin = new TalosAdmin(clientConfig, credential);
  }

  // create topic specified partitionNumber and topicName
  public CreateTopicResponse createTopic() throws TException {
    TopicAttribute topicAttribute = new TopicAttribute()
        .setPartitionNumber(partitionNumber);

    // Note: authorization must use 'teamId' and only identifier setting is required
    Grantee grant = new Grantee().setIdentifier(anotherTeamId);
    Map<Grantee, Permission> aclMap = new HashMap<Grantee, Permission>();
    aclMap.put(grant, Permission.TOPIC_READ_AND_MESSAGE_FULL_CONTROL);
    // Note: using cloudTopicName instead of original topic name
    CreateTopicRequest request = new CreateTopicRequest()
        .setTopicName(cloudTopicName)
        .setTopicAttribute(topicAttribute)
        .setAclMap(aclMap);
    return talosAdmin.createTopic(request);
  }

  // get topicTalosResourceName by topicName
  public TopicTalosResourceName getTopicTalosResourceName() throws TException {
    GetDescribeInfoResponse response = talosAdmin.getDescribeInfo(
        new GetDescribeInfoRequest(topicName));
    resourceName = response.getTopicTalosResourceName();
    LOG.info("Topic resourceName is: " + resourceName.getTopicTalosResourceName());
    return resourceName;
  }

  // delete topic by topicTalosResourceName
  public void deleteTopic() throws TException {
    DeleteTopicRequest request = new DeleteTopicRequest(resourceName);
    talosAdmin.deleteTopic(request);
    LOG.info("Topic success to delete: " + resourceName);
  }

  public void getWorkerId() throws TException {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, resourceName, 1);
    GetWorkerIdRequest request = new GetWorkerIdRequest(topicAndPartition, consumerGroup);
    String workerId = talosAdmin.getWorkerId(request);
    LOG.info("workerId:" + workerId);
  }

  public static void main(String[] args) throws Exception {
    TalosAdminDemo cloudAdminDemo = new TalosAdminDemo();
    cloudAdminDemo.createTopic();
    cloudAdminDemo.getTopicTalosResourceName();
    //cloudAdminDemo.getWorkerId();
  }
}
