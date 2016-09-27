package com.xiaomi.infra.galaxy.sds.examples.stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.xiaomi.infra.galaxy.rpc.thrift.GrantType;
import com.xiaomi.infra.galaxy.rpc.thrift.Grantee;
import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.IncrementRequest;
import com.xiaomi.infra.galaxy.sds.thrift.PointInTimeRecovery;
import com.xiaomi.infra.galaxy.sds.thrift.PutRequest;
import com.xiaomi.infra.galaxy.sds.thrift.PutResult;
import com.xiaomi.infra.galaxy.sds.thrift.RemoveRequest;
import com.xiaomi.infra.galaxy.sds.thrift.RemoveResult;
import com.xiaomi.infra.galaxy.sds.thrift.StreamCheckpoint;
import com.xiaomi.infra.galaxy.sds.thrift.StreamSpec;
import com.xiaomi.infra.galaxy.sds.thrift.StreamViewType;
import com.xiaomi.infra.galaxy.sds.thrift.TableInfo;
import com.xiaomi.infra.galaxy.sds.thrift.TableSchema;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.TableSnapshots;
import com.xiaomi.infra.galaxy.sds.thrift.TableSpec;
import com.xiaomi.infra.galaxy.sds.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Permission;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAttribute;
import com.xiaomi.infra.galaxy.talos.thrift.TopicInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: qiankai@xiaomi.com
 */
public class PitrDemo {
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

  public PitrDemo() {
    initSdsClient();
    initTalosClient();
  }

  private boolean shouldDo() {
    return rand.nextBoolean();
  }

  public void listSnapshots() throws Exception {
    TableSnapshots tableSnapshots = adminClient.listSnapshots(tableName);
    LOG.info("table " + tableName + " snapshots are " + tableSnapshots);
  }

  public StreamCheckpoint getLatestCheckpoint() throws Exception {
    return adminClient.getLatestStreamCheckpoint(tableName, topicName);
  }

  public void produceData()
      throws Exception {
    LOG.info("Begin to produce data for table " + tableName);
    for (int i = 0; i < 1000; i++) {
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

  public TableSchema createSrcTable()
      throws Exception {
    Map<String, StreamSpec> streamSpecs = new HashMap<String, StreamSpec>();
    streamSpecs.put(topicName, streamSpec);
    TableSpec tableSpec = DataProvider.createTableSpec(appId, enableEntityGroup,
        enableEntityGroupHash, streamSpecs, pitr);

    TableInfo tableInfo = adminClient.createTable(tableName, tableSpec);
    LOG.info("Src table " + tableName + " is created");
    return tableInfo.getSpec().getSchema();
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

  private PointInTimeRecovery createPitr() {
    PointInTimeRecovery pitr = new PointInTimeRecovery();
    pitr.setEnablePointInTimeRecovery(true);
    pitr.setTopicName(topicName);
    pitr.setSnapshotPeriod(86400);   // 1 day
    pitr.setTtl(604800);    // 1 week
    return pitr;
  }

  public void createTable() throws Exception {
    topicInfo = createTopic();
    streamSpec = createStreamSpec();
    pitr = createPitr();
    tableSchema = createSrcTable();
  }

  public void recoverTable(long timestamp) throws Exception {
    adminClient.recoverTable(tableName, destTableName, topicName, timestamp);
  }

  // sds config
  private static final String sdsServiceEndpoint = "$sdsServiceEndpoint";
  private static final String tableName = "pitrDemoTable";
  private static final String destTableName = "destPitrDemoTable";
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
  private static PointInTimeRecovery pitr;

  // talos config
  private static final String talosServiceEndpoint = "$talosServiceEndpoint";
  private static final String topicName = "pitrDemoTopic";
  private static final int topicPartitionNumber = 8;

  private static TalosAdmin talosAdmin;
  private static TopicInfo topicInfo;

  public static void main(String[] args) throws Exception {
    PitrDemo pitrDemo = new PitrDemo();

    pitrDemo.createTable();
    pitrDemo.produceData();

    Thread.sleep(120000);

    pitrDemo.produceData();

    pitrDemo.listSnapshots();
    StreamCheckpoint latestCheckpoint = pitrDemo.getLatestCheckpoint();

    pitrDemo.recoverTable(latestCheckpoint.getTimestamp() - 10);
  }
}
