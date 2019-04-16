/**
 * Copyright 2019, Xiaomi. All rights reserved. Author: zhangqian8@xiaomi.com
 */

package com.xiaomi.infra.codelab.talos;

import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.Map;

import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.producer.TalosProducerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosPutStressor {

    private static final Logger LOG = LoggerFactory.getLogger(TalosPutStressor.class);

    private static final String propertyFileName = "talos.properties";
    private static String accessKey;
    private static String accessSecret;
    private static String topicName;
    private static int partitionId;
    private static String messageStr;
    private static int threadNumPerPartition;
    private static int qpsPerThread;

    private TalosClientConfig clientConfig;
    private TalosProducerConfig producerConfig;
    private Credential credential;
    private TalosAdmin talosAdmin;

    private TopicTalosResourceName topicTalosResourceName;
    private int partitionNum;
    private Map<TopicAndPartition, PartitionSendStressor> partitionSendStressorMap =
        new HashMap<TopicAndPartition, PartitionSendStressor>();

    public TalosPutStressor() throws Exception {
        clientConfig = new TalosClientConfig(propertyFileName);
        producerConfig = new TalosProducerConfig(propertyFileName);
        accessKey = clientConfig.getAccessKey();
        accessSecret = clientConfig.getAccessSecret();
        topicName = clientConfig.getTopicName();
        partitionId = clientConfig.getPartitionId();
        threadNumPerPartition = clientConfig.getThreadNumPerPartition();
        qpsPerThread = clientConfig.getQpsPerThread();
        messageStr = clientConfig.getMessageStr();

        // credential
        credential = new Credential();
        credential.setSecretKeyId(accessKey).setSecretKey(accessSecret)
            .setType(UserType.DEV_XIAOMI);

        // init admin and try to get or create topic info
        talosAdmin = new TalosAdmin(clientConfig, credential);
        getTopicInfo();
    }

    private void getTopicInfo() throws Exception {
        Topic topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
        topicTalosResourceName = topic.getTopicInfo().getTopicTalosResourceName();
        partitionNum = topic.getTopicAttribute().getPartitionNumber();
    }

    public void startStress() throws TException {
        TopicAndPartition topicAndPartition;
        PartitionSendStressor partitionSendStressor;
        Message message = new Message(ByteBuffer.wrap(messageStr.getBytes()));

        if (partitionId == -1) {
            for (int partitionId = 0; partitionId < partitionNum ; partitionId++) {
                topicAndPartition = new TopicAndPartition(topicName, topicTalosResourceName, partitionId);
                partitionSendStressor = new PartitionSendStressor(partitionId,
                    threadNumPerPartition, qpsPerThread, producerConfig, topicAndPartition, message, credential);
                partitionSendStressorMap.put(topicAndPartition, partitionSendStressor);
            }
        } else {
            topicAndPartition = new TopicAndPartition(topicName, topicTalosResourceName, partitionId);
            partitionSendStressor = new PartitionSendStressor(partitionId,
                threadNumPerPartition, qpsPerThread, producerConfig, topicAndPartition, message, credential);
            partitionSendStressorMap.put(topicAndPartition, partitionSendStressor);
        }
    }

    public static void main(String[] args) throws Exception {
        TalosPutStressor talosPutStressor = new TalosPutStressor(); //init parms
        talosPutStressor.startStress();
    }
}
