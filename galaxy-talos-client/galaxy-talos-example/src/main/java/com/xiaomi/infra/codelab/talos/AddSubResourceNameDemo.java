/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.codelab.talos;

import java.util.Properties;

import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.thrift.AddSubResourceNameRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class AddSubResourceNameDemo {

  private static final Logger LOG = LoggerFactory.getLogger(AddSubResourceNameDemo.class);
  // authenticate for developer
  private static final String accessKey = "$your_accountKey";
  private static final String accessSecret = "$your_accountSecret";

  // your OrgId and TeamId to grant
  private static final String topicName = "$your_topicName";
  private static final String orgId = "$your_org_id";
  private static final String teamId = "$your_team_id";

  private TopicTalosResourceName resourceName;
  private Credential credential;
  private TalosAdmin talosAdmin;

  public AddSubResourceNameDemo() throws TException {
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
    getTopicTalosResourceName();
  }

  public void migrate() throws TException {
    AddSubResourceNameRequest subRequest = new AddSubResourceNameRequest(
        resourceName, orgId, teamId);
    talosAdmin.addSubResourceName(subRequest);
  }

  // get topicTalosResourceName by topicName
  private void getTopicTalosResourceName() throws TException {
    Topic topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
    resourceName = topic.getTopicInfo().getTopicTalosResourceName();
    LOG.info("Topic resourceName is: " + resourceName);
  }

  public static void main(String[] args) {
    try {
      AddSubResourceNameDemo addSubDemo = new AddSubResourceNameDemo();
      addSubDemo.migrate();
    } catch (TException e) {
      LOG.error("add sub name error: ", e);
    }
  }
}