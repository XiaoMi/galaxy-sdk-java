package com.xiaomi.infra.galaxy.sds.client.examples.stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.xiaomi.infra.galaxy.rpc.thrift.GrantType;
import com.xiaomi.infra.galaxy.rpc.thrift.Grantee;
import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.client.TableScanner;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.IncrementRequest;
import com.xiaomi.infra.galaxy.sds.thrift.MutationLogEntry;
import com.xiaomi.infra.galaxy.sds.thrift.PutRequest;
import com.xiaomi.infra.galaxy.sds.thrift.PutResult;
import com.xiaomi.infra.galaxy.sds.thrift.RemoveRequest;
import com.xiaomi.infra.galaxy.sds.thrift.RemoveResult;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.StreamSpec;
import com.xiaomi.infra.galaxy.sds.thrift.StreamViewType;
import com.xiaomi.infra.galaxy.sds.thrift.TableInfo;
import com.xiaomi.infra.galaxy.sds.thrift.TableSchema;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.TableSpec;
import com.xiaomi.infra.galaxy.sds.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.SimpleTopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.consumer.MessageCheckpointer;
import com.xiaomi.infra.galaxy.talos.consumer.MessageProcessor;
import com.xiaomi.infra.galaxy.talos.consumer.MessageProcessorFactory;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumer;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicResponse;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.Permission;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAttribute;
import com.xiaomi.infra.galaxy.talos.thrift.TopicInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: qiankai@xiaomi.com
 */
public class StreamDemo {
  private static final Logger LOG = LoggerFactory.getLogger(StreamDemo.class);
  static Random rand = new Random(0);
  static final int PARTITAL_DELETE_NUMBER = 2;
  static final int ADMIN_CONN_TIMEOUT = 3000;
  static final int ADMIN_SOCKET_TIMEOUT = 50000;
  static final int TABLE_CONN_TIMEOUT = 3000;
  static final int TABLE_SOCKET_TIMEOUT = 10000;


  private Credential createSdsCredential(String secretKeyId, String secretKey, UserType userType) {
    return new Credential()
        .setSecretKeyId(secretKeyId)
        .setSecretKey(secretKey)
        .setType(userType);
  }

  private com.xiaomi.infra.galaxy.rpc.thrift.Credential createTalosCredential(String secretKeyId,
      String secretKey, com.xiaomi.infra.galaxy.rpc.thrift.UserType userType) {
   return new com.xiaomi.infra.galaxy.rpc.thrift.Credential()
       .setSecretKeyId(secretKeyId)
       .setSecretKey(secretKey)
       .setType(userType);
  }

  private AdminService.Iface createAdminClient(String endpoint, Credential credential) {
    ClientFactory clientFactory = new ClientFactory().setCredential(credential);
    return clientFactory.newAdminClient(endpoint + CommonConstants.ADMIN_SERVICE_PATH,
        ADMIN_SOCKET_TIMEOUT, ADMIN_CONN_TIMEOUT);
  }

  private TableService.Iface createTableClient(String endpoint, Credential credential) {
    ClientFactory clientFactory = new ClientFactory().setCredential(credential);
    return clientFactory.newTableClient(endpoint + CommonConstants.TABLE_SERVICE_PATH,
        TABLE_SOCKET_TIMEOUT, TABLE_CONN_TIMEOUT);
  }

  private void initSdsClient() {
    Credential credential = createSdsCredential(accountKey, accountSecret, UserType.DEV_XIAOMI);
    adminClient = createAdminClient(sdsServiceEndpoint, credential);

    credential = createSdsCredential(appKey, appSecret, UserType.APP_SECRET);
    tableClient = createTableClient(sdsServiceEndpoint, credential);
  }

  private void initTalosClient() {
    Properties pro = new Properties();
    pro.setProperty(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT, talosServiceEndpoint);
    TalosClientConfig talosClientConfig = new TalosClientConfig(pro);
    com.xiaomi.infra.galaxy.rpc.thrift.Credential credential = createTalosCredential(
        accountKey, accountSecret, com.xiaomi.infra.galaxy.rpc.thrift.UserType.DEV_XIAOMI);
    talosAdmin = new TalosAdmin(talosClientConfig, credential);
  }

  public StreamDemo() {
    initSdsClient();
    initTalosClient();
  }

  private PutRequest getReplayPutRequest(String tableName, Map<String, Datum> record) {
    return new PutRequest().setTableName(tableName)
        .setRecord(record);
  }

  private IncrementRequest getReplayIncrementRequest(String tableName, Map<String, Datum> record) {
    Map<String, Datum> keys = new HashMap<String, Datum>();
    Map<String, Datum> amounts = new HashMap<String, Datum>();
    for (Map.Entry<String, Datum> entry : record.entrySet()) {
      if (DataProvider.rowKeyDef(enableEntityGroup).keySet()
            .contains(entry.getKey())) {
        keys.put(entry.getKey(), entry.getValue());
      } else {
        amounts.put(entry.getKey(), entry.getValue());
      }
    }
    return new IncrementRequest().setTableName(tableName)
        .setKeys(keys)
        .setAmounts(amounts);
  }

  private RemoveRequest getReplayRemoveRequest(String tableName, Map<String, Datum> record,
      boolean rowDeleted) {
    if (rowDeleted) {
      Preconditions.checkArgument(DataProvider.rowKeyDef(enableEntityGroup).keySet()
          .equals(record.keySet()));
      return new RemoveRequest().setTableName(tableName)
          .setKeys(record);
    } else {
      List<String> attributes = new ArrayList<String>();
      Map<String, Datum> keys = new HashMap<String, Datum>();
      for (Map.Entry<String, Datum> entry : record.entrySet()) {
        if (DataProvider.rowKeyDef(enableEntityGroup).keySet()
            .contains(entry.getKey())) {
          keys.put(entry.getKey(), entry.getValue());
        } else {
          attributes.add(entry.getKey());
        }
      }
      return new RemoveRequest().setTableName(tableName)
          .setKeys(keys)
          .setAttributes(attributes);
    }
  }

  private void replayMutationLogEntry(List<MutationLogEntry> messages, String destTableName)
      throws Exception {
    for (MutationLogEntry entry : messages) {
      LOG.info("Consuming mutation log entry : " + entry);
      switch (entry.getType()) {
      case PUT:
        PutRequest put = getReplayPutRequest(destTableName, entry.getRecord());
        PutResult putResult = tableClient.put(put);
        Preconditions.checkArgument(putResult.isSuccess());
        LOG.info("record " + entry.getRecord() + " is put to " + destTableName);
        break;
      case INCREMENT:
        IncrementRequest increment = getReplayIncrementRequest(destTableName, entry.getRecord());
        tableClient.increment(increment);
        LOG.info("record " + entry.getRecord() + " is increment to " + destTableName);
        break;
      case DELETE:
        RemoveRequest remove = getReplayRemoveRequest(destTableName, entry.getRecord(), entry.isRowDeleted());
        RemoveResult removeResult = tableClient.remove(remove);
        Preconditions.checkArgument(removeResult.isSuccess());
        LOG.info("record " + entry.getRecord() + " is removed from " + destTableName);
        break;
      default:
        Preconditions.checkArgument(false);
      }
    }
  }

  // callback for consumer to process messages, that is, consuming logic
  private class MutateLogProcessor implements MessageProcessor {
    @Override
    public void init(TopicAndPartition topicAndPartition, long messageOffset) {
    }

    @Override
    public void process(List<MessageAndOffset> messages, MessageCheckpointer messageCheckpointer) {
      try {
        if (messages.size() > 0) {
          List<MutationLogEntry> logEntries = new ArrayList<MutationLogEntry>(messages.size());
          for (MessageAndOffset entry : messages) {
            try {
              MutationLogEntry logEntry = DatumUtil.deserialize(entry.getMessage().getMessage(),
                  MutationLogEntry.class);
              logEntries.add(logEntry);
            } catch (Exception e) {
              LOG.error("Deserialize message " + entry + " got exception ", e);
              throw e;
            }
          }

          if (!logEntries.isEmpty()) {
            try {
              replayMutationLogEntry(logEntries, destTableName);
            } catch (Exception e) {
              LOG.error("Replay mutation log entries " + logEntries + " got exception ", e);
              throw e;
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Process mutation log entries for topic " + topicName + " get exception ", e);
        throw new RuntimeException(e);
      }
    }

    @Override
    public void shutdown(MessageCheckpointer messageCheckpointer) {
    }
  }

  // using for thread-safe when processing different partition data
  private class MutateLogProcessorFactory implements MessageProcessorFactory {

    public MutateLogProcessorFactory() {
    }

    @Override
    public MutateLogProcessor createProcessor() {
      return new MutateLogProcessor();
    }
  }

  private TalosConsumer createTalosConsumer()
      throws Exception {
    if (!streamViewType.equals(StreamViewType.MUTATE_LOG)) {
      throw new RuntimeException("Unexpected stream view type : " + streamViewType);
    }

    Properties pro = new Properties();
    pro.setProperty(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT, talosServiceEndpoint);
    TalosConsumerConfig talosConsumerConfig = new TalosConsumerConfig(pro);
    com.xiaomi.infra.galaxy.rpc.thrift.Credential credential = createTalosCredential(
        appKey, appSecret, com.xiaomi.infra.galaxy.rpc.thrift.UserType.APP_SECRET);

    return new TalosConsumer(consumerGroup, talosConsumerConfig, credential,
        topicInfo.getTopicTalosResourceName(), new MutateLogProcessorFactory(), clientPrefix,
        new SimpleTopicAbnormalCallback());
  }

  private boolean shouldDo() {
    return rand.nextBoolean();
  }

  public void produceData()
      throws Exception {
    LOG.info("Begin to produce data for table " + tableName);
    for (int i = 0; i < 100; i++) {
      Map<String, Datum> record = DataProvider.randomRecord(
          DataProvider.attributesDef(enableEntityGroup));
      Map<String, Datum> keys = DataProvider.getRecordKeys(tableSchema, record);
      Map<String, Datum> dataRecord = Maps.difference(record, keys).entriesOnlyOnLeft();

      // put
      PutRequest put = new PutRequest();
      put.setTableName(tableName)
          .setRecord(record);
      PutResult putResult = tableClient.put(put);
      Preconditions.checkArgument(putResult.isSuccess());

      // increment
      if (shouldDo()) {
        Map<String, Datum> amounts = DataProvider.randomIncrementAmounts(tableSchema,
            dataRecord.keySet());
        IncrementRequest increment = new IncrementRequest();
        increment.setTableName(tableName)
            .setKeys(keys)
            .setAmounts(amounts);
        tableClient.increment(increment);
      }

      // partial delete
      if (shouldDo()) {
        List<String> attributes = DataProvider.randomSelect(dataRecord.keySet(),
            PARTITAL_DELETE_NUMBER);
        RemoveRequest remove = new RemoveRequest();
        remove.setTableName(tableName)
            .setAttributes(attributes)
            .setKeys(keys);
        RemoveResult removeResult = tableClient.remove(remove);
        Preconditions.checkArgument(removeResult.isSuccess());
      }
    }
    LOG.info("Produce data finished for table " + tableName);
  }

  public TableSchema createSrcTable(String tableName, String topicName, StreamSpec streamSpec)
      throws Exception {
    Map<String, StreamSpec> streamSpecs = new HashMap<String, StreamSpec>();
    streamSpecs.put(topicName, streamSpec);
    TableSpec tableSpec = DataProvider.createTableSpec(appId, enableEntityGroup,
        enableEntityGroupHash, streamSpecs);

    TableInfo tableInfo = adminClient.createTable(tableName, tableSpec);
    LOG.info("Src table " + tableName + " is created");
    return tableInfo.getSpec().getSchema();
  }

  public void createDestTable() throws Exception {
    TableSpec tableSpec = DataProvider.createTableSpec(appId, enableEntityGroup,
        enableEntityGroupHash);

    adminClient.createTable(destTableName, tableSpec);
    LOG.info("Dest table " + destTableName + " is created");
  }

  private TopicInfo createTopic() throws Exception {
    CreateTopicRequest createTopicRequest = new CreateTopicRequest();
    createTopicRequest.setTopicName(topicName);

    Map<Grantee,Permission> aclMap = new HashMap<Grantee, Permission>();
    Grantee grantee = new Grantee();
    grantee.setIdentifier(appId)
        .setType(GrantType.APP_ROOT);
    aclMap.put(grantee, Permission.TOPIC_READ_AND_MESSAGE_FULL_CONTROL);
    createTopicRequest.setAclMap(aclMap);

    TopicAttribute topicAttribute = new TopicAttribute();
    topicAttribute.setPartitionNumber(topicPartitionNumber);
    createTopicRequest.setTopicAttribute(topicAttribute);
    CreateTopicResponse createTopicResponse = talosAdmin.createTopic(createTopicRequest);
    LOG.info("Topic " + topicName + " is created");
    return createTopicResponse.getTopicInfo();
  }

  private StreamSpec createStreamSpec() throws Exception {
    StreamSpec streamSpec = new StreamSpec();
    streamSpec.setViewType(streamViewType)
        .setAttributes(Lists.newArrayList(DataProvider.columnsDef().keySet()))
        .setEnableStream(true);
    LOG.info("Stream " + streamSpec + " is created");
    return streamSpec;
  }

  public void createTable() throws Exception {
    topicInfo = createTopic();
    streamSpec = createStreamSpec();
    tableSchema = createSrcTable(tableName, topicName, streamSpec);
  }

  // sds config
  private static final String sdsServiceEndpoint = "$sdsServiceEndpoint";
  private static final String tableName = "streamDemoTable";
  private static final String destTableName = "destStreamDemoTable";
  private static final boolean enableEntityGroup = true;
  private static final boolean enableEntityGroupHash = true;
  private static final StreamViewType streamViewType = StreamViewType.MUTATE_LOG;
  private static final String accountKey = "$your_accountKey";
  private static final String accountSecret = "$your_accountSecret";
  private static final String appId = "$your_appId";
  private static final String appKey = "$your_appKey";
  private static final String appSecret = "$your_appSecret";
  private static AdminService.Iface adminClient;
  private static TableService.Iface tableClient;

  private static TableSchema tableSchema;
  private static StreamSpec streamSpec;

  // talos config
  private static final String talosServiceEndpoint = "$talosServiceEndpoint";
  private static final String topicName = "streamDemoTopic";
  private static final int topicPartitionNumber = 8;
  private static final String clientPrefix = "departmentName-";
  private static final String consumerGroup = "groupName";

  private static TalosAdmin talosAdmin;
  private static TopicInfo topicInfo;

  public static void main(String[] args) throws Exception {
    StreamDemo streamDemo = new StreamDemo();

    streamDemo.createTable();
    streamDemo.createDestTable();

    streamDemo.createTalosConsumer();
    streamDemo.produceData();
  }
}
