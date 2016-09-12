/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.codelab.talos;

import java.util.HashMap;
import java.util.Map;

import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.GrantType;
import com.xiaomi.infra.galaxy.rpc.thrift.Grantee;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.thrift.ChangeTopicAttributeRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicResponse;
import com.xiaomi.infra.galaxy.talos.thrift.DeleteTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.Permission;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAttribute;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosAdminDemo {
  private static final Logger LOG = LoggerFactory.getLogger(TalosAdminDemo.class);
  // authenticate for Developer, using to createTopic/setPermission etc.
  private static final String accountKeyId = "$your_accountKey";
  private static final String accountSecret = "$your_accountKeySecret";

  // authenticate for AppRoot, which can be grant permission by Developer
  /**
   * Note:
   * TalosAdmin demo authenticate Developer by using 'accountKeyId/accountSecret',
   * then grant read permission to AppRoot principal by using 'appKeyId/appKeySecret'
   * Both TalosConsumer demo and TalosProducer demo use 'appKeyId/appKeySecret'
   */
  private static final String appId = "$appId";
  private static final String appKeyId = "$your_appKey";
  private static final String appKeySecret = "$your_appSecret";
  private static final String propertyFileName = "$your_propertyFile";

  private static final String topicName = "testTopic";
  private static final int partitionNumber = 8;

  private TopicTalosResourceName resourceName;
  private Credential credential;
  private TalosAdmin talosAdmin;

  public TalosAdminDemo() throws TException {
    // init client config by put $your_propertyFile in your classpath
    // with the content of:
    /*
      galaxy.talos.service.endpoint=$talosServiceURI
    */
    TalosClientConfig clientConfig = new TalosClientConfig(propertyFileName);
    /*
      You can also using the other method to init client config as follows:

      Properties properties = new Properties();
      properties.setProperty("galaxy.talos.service.endpoint", "serviceURI");
      TalosClientConfig clientConfig = new TalosClientConfig(properties);
    */

    // credential
    credential = new Credential();
    credential.setSecretKeyId(accountKeyId) // using the 'AccountKey'
        .setSecretKey(accountSecret)        // using the 'AccountSecret'
        .setType(UserType.DEV_XIAOMI);

    // init admin
    talosAdmin = new TalosAdmin(clientConfig, credential);
  }

  // create topic specified partitionNumber and topicName
  public CreateTopicResponse createTopic() throws TException {
    TopicAttribute topicAttribute = new TopicAttribute()
        .setPartitionNumber(partitionNumber);

    // make an authorization operation
    // Note: authorization must use 'appId' or 'accountId'
    Grantee grant = new Grantee().setType(GrantType.APP_ROOT).setIdentifier(appId);
    Map<Grantee, Permission> aclMap = new HashMap<Grantee, Permission>();
    aclMap.put(grant, Permission.TOPIC_READ_AND_MESSAGE_FULL_CONTROL);
    CreateTopicRequest request = new CreateTopicRequest()
        .setTopicName(topicName)
        .setTopicAttribute(topicAttribute)
        .setAclMap(aclMap);
    return talosAdmin.createTopic(request);
  }

  // get topicTalosResourceName by topicName
  public TopicTalosResourceName getTopicTalosResourceName() throws TException {
    Topic topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
    resourceName = topic.getTopicInfo().getTopicTalosResourceName();
    LOG.info("Topic resourceName is: " + resourceName);
    return resourceName;
  }

  // delete topic by topicTalosResourceName
  public void deleteTopic() throws TException {
    DeleteTopicRequest request = new DeleteTopicRequest(resourceName);
    talosAdmin.deleteTopic(request);
  }

  // change topic attribute by topicTalosResourceName
  public void changeTopicAttribute() throws TException {
    TopicAttribute topicAttribute = new TopicAttribute()
        .setPartitionNumber(partitionNumber * 2);
    ChangeTopicAttributeRequest request = new ChangeTopicAttributeRequest(
        resourceName, topicAttribute);
    talosAdmin.changeTopicAttribute(request);
  }

  public static void main(String[] args) throws Exception {
    TalosAdminDemo adminDemo = new TalosAdminDemo();
    adminDemo.createTopic();
    adminDemo.getTopicTalosResourceName();
    // adminDemo.changeTopicAttribute();
    // adminDemo.deleteTopic();
  }
}
